import dgl
from torch import nn
import dgl.function as fn

from learning.message_aggregators.simple_aggregator import SimpleAggregator
from learning.utils.fc_out_model import FcOutModel
from learning.message_aggregators import message_aggregators
from learning.utils.node_type_encoder import NodeTypeEncoder


class MyPassDirection:
    def __init__(self, model_name, e_name=None, n_src=None, n_dest=None, allow_empty=False):
        self.etypes = set()
        self.in_types = set()
        self.out_types = set()
        self.model_name = model_name

        self.etypes = [(n_src, e_name, n_dest)]
        self.in_types = [n_src]
        self.out_types = [n_dest]
        if not allow_empty:
            assert len(self.etypes) > 0, f"No nodes in the graph qualify for e_name={e_name}, n_dest={n_dest}"


class PassDirection:
    def __init__(self, model_name, g, e_name=None, n_dest=None, allow_empty=False):
        self.etypes = set()
        self.in_types = set()
        self.out_types = set()
        self.model_name = model_name

        for curr_n_src, curr_e_name, curr_n_dest in g.canonical_etypes:
            if e_name is not None and curr_e_name != e_name:
                continue

            if n_dest is not None and curr_n_dest != n_dest:
                continue

            self.etypes.add((curr_n_src, curr_e_name, curr_n_dest))
            self.in_types.add(curr_n_src)
            self.out_types.add(curr_n_dest)

        self.etypes = list(self.etypes)
        self.in_types = list(self.in_types)
        self.out_types = list(self.out_types)
        if not allow_empty:
            assert len(self.etypes) > 0, f"No nodes in the graph qualify for e_name={e_name}, n_dest={n_dest}"


class ZeroShotMessagePassingModel(FcOutModel):
    def __init__(self, hidden_dim=None, final_mlp_kwargs=None, output_dim=1, tree_layer_name=None,
                 tree_layer_kwargs=None, test=False, skip_message_passing=False, readout_mode=None,
                 device="cpu", label_norm=None, mp_scheme=None):
        super().__init__(output_dim=output_dim, input_dim=hidden_dim, final_out_layer=True, **final_mlp_kwargs)

        self.device = device
        self.label_norm = label_norm
        self.test = test
        self.skip_message_passing = skip_message_passing
        self.hidden_dim = hidden_dim
        self.readout_mode = readout_mode
        self.mp_scheme = mp_scheme

        if self.mp_scheme == "bottom-up":
            # use different models per edge type
            self.tree_models = nn.ModuleDict(
                {"edge": message_aggregators.__dict__[tree_layer_name](hidden_dim=self.hidden_dim, **tree_layer_kwargs),
                 "has_operator": message_aggregators.__dict__[tree_layer_name](hidden_dim=self.hidden_dim,
                                                                               **tree_layer_kwargs)})

        elif self.mp_scheme == "full":
            self.tree_models = nn.ModuleDict(
                {"edge": message_aggregators.__dict__[tree_layer_name](hidden_dim=self.hidden_dim, **tree_layer_kwargs),
                 "has_operator": message_aggregators.__dict__[tree_layer_name](hidden_dim=self.hidden_dim,
                                                                               **tree_layer_kwargs),
                 "is_placed_on": message_aggregators.__dict__[tree_layer_name](hidden_dim=self.hidden_dim,
                                                                               **tree_layer_kwargs)})
        elif self.mp_scheme == "simple":
            self.tree_models = nn.ModuleDict({"edge": SimpleAggregator()})

        else:
            raise RuntimeError(f'{mp_scheme} is not supported')

    def encode_node_types(self, g, features):
        """
        Initializes the hidden states based on the node type specific models.
        """
        raise NotImplementedError

    def forward(self, input):
        """
        Returns logits for output classes
        """
        graph, features = input
        graph.features = self.encode_node_types(graph, features)
        out = self.message_passing(graph)
        return out

    def message_passing(self, g):
        """
        Runs the GNN component of the model and returns logits for output classes.
        """
        pass_directions = g.pd
        feat_dict = g.features
        graph_data = g.data

        # Message passing
        if self.mp_scheme == "simple":
            self.tree_models["edge"](g, feat_dict=feat_dict)

        else:
            for pd in pass_directions:
                assert len(pd.etypes) > 0
                out_dict = self.tree_models[pd.model_name](g, etypes=pd.etypes, in_node_types=pd.in_types,
                                                           out_node_types=pd.out_types, feat_dict=feat_dict)
                for out_type, hidden_out in out_dict.items():
                    feat_dict[out_type] = hidden_out

        # Readout
        if self.readout_mode == "sink_readout":
            # Performing a sinkd readout
            # outs = []
            g.ndata["feat"] = feat_dict
            outs = feat_dict['sink']

        elif self.readout_mode == "graph_readout":
            # perform graph readout by performing sum/min/max/avg operation over all nodes
            g.ndata["feat"] = feat_dict
            outs = self.readout_graph(g)

        else:
            raise NotImplementedError(self.readout_mode + " is not supported")

        if not self.test:
            outs = self.fcout(outs)
        # outs = th.reshape(outs, (1, int(outs.shape[0])))[0]
        return outs

    def readout_graph(self, graph):
        readout_result = None
        for ntype in graph.ntypes:
            tmp = dgl.readout_nodes(graph, feat='feat', op="sum", ntype=ntype)
            if readout_result is None:
                readout_result = tmp
            else:
                readout_result += tmp
        return readout_result


class ZeroShotModel(ZeroShotMessagePassingModel):
    def __init__(self, hidden_dim=None, node_type_kwargs=None, feature_statistics=None, featurization=None, **kwargs):
        super().__init__(hidden_dim=hidden_dim, **kwargs)

        self.plan_featurization = featurization
        # different models to encode operators
        node_type_kwargs.update(output_dim=hidden_dim)
        self.node_type_encoders = nn.ModuleDict({
            'host': NodeTypeEncoder(self.plan_featurization.HOST_FEATURES, feature_statistics, **node_type_kwargs),
            'spout': NodeTypeEncoder(self.plan_featurization.SPOUT_FEATURES, feature_statistics,
                                     **node_type_kwargs),
            'filter': NodeTypeEncoder(self.plan_featurization.FILTER_FEATURES, feature_statistics,
                                      **node_type_kwargs),
            'join': NodeTypeEncoder(self.plan_featurization.WINDOWED_JOIN_FEATURES, feature_statistics,
                                    **node_type_kwargs),
            'aggregation': NodeTypeEncoder(self.plan_featurization.AGGREGATION_FEATURES, feature_statistics,
                                           **node_type_kwargs),
            'windowedAggregation': NodeTypeEncoder(self.plan_featurization.WINDOWED_AGGREGATION,
                                                   feature_statistics, **node_type_kwargs),
            'sink': NodeTypeEncoder(self.plan_featurization.SINK, feature_statistics, **node_type_kwargs)
        })

    def encode_node_types(self, g, features):
        """
        Initializes the hidden states based on the node type specific models.
        """
        # initialize hidden state per node type
        hidden_dict = dict()
        for node_type, input_features in features.items():
            # encode all plans with same model
            node_type_m = self.node_type_encoders[node_type]
            hidden_dict[node_type] = node_type_m(input_features)
        return hidden_dict
