import torch
from torch import nn
import copy
from utils import to_variable, make_dir

class TestQNetwork(nn.Module):

    def __init__(self, I):
        super(TestQNetwork, self).__init__()
	H_scale = 2
        self.linear1 = nn.Sequential(
            nn.Linear(I, I*H_scale, bias=True),
            nn.ReLU()
        )
        self.out = nn.Sequential(
            nn.Linear(I*H_scale, 1, bias=True),
        )

        # Init with cuda if available
	if torch.cuda.is_available():
	    self.cuda()

    def forward(self, x):
        x = self.linear1(x)
        x = self.out(x)
        return x
