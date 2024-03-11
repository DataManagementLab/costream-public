from torch import nn
import dgl.function as fn
import dgl as dgl


class SimpleAggregator(nn.Module):
    def forward(self, graph: dgl.DGLHeteroGraph = None, feat_dict=None):
        with graph.local_scope():
            graph.ndata['h'] = feat_dict
            graph.update_all(fn.copy_u('h', 'm'), fn.sum('m', 'feat'))
