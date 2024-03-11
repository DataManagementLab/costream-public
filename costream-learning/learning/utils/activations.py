from torch import nn
from torch.nn import LeakyReLU, CELU, SELU, ELU, Sigmoid
from torch.nn import functional as F

LeakyReLU
CELU
SELU
ELU
Sigmoid

class GELU(nn.Module):
    def forward(self, input):
        return F.gelu(input)
