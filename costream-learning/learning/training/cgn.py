import os

import torch

from learning.constants import FULL_FEATURIZATION
from learning.utils import losses
from learning.utils.fc_out_model import FcOutModel
from learning.utils.node_type_encoder import NodeTypeEncoder

os.environ['DGLBACKEND'] = 'pytorch'
import torch.nn as nn
import torch.nn.functional as F
import dgl.nn as dglnn


class SAGE(nn.Module):
    def __init__(self, hidden_dim, final_mlp_kwargs, node_type_kwargs, output_dim, feature_statistics, device, label_norm, num_layers):
        super().__init__()
        self.device = device
        self.label_norm = label_norm

        rel_names = []
        rel_names.append("placement")
        rel_names.append("edge")

        self.conv_layers = []
        for i in range(num_layers):
            self.conv_layers.append(dglnn.HeteroGraphConv({rel: dglnn.GraphConv(hidden_dim, hidden_dim) for rel in rel_names}, aggregate='sum'))
        self.final_conv = dglnn.HeteroGraphConv({rel: dglnn.GraphConv(hidden_dim, output_dim) for rel in rel_names}, aggregate='sum')

        self.plan_featurization = FULL_FEATURIZATION
        self.q_loss_fxn = losses.QLoss(self)
        self.mse_loss_fxn = losses.MSELoss(self)
        self.to(torch.float)

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

    def forward(self, input):
        graph, features = input
        h = self.encode_node_types(graph, features)
        for conv_layer in self.conv_layers:
            h = conv_layer(graph, h)
            h = {k: F.leaky_relu(v) for k, v in h.items()}
        h = self.final_conv(graph, h)
        return h["sink"]