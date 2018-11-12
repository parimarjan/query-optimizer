import os
import errno
import torch
from torch.autograd import Variable
import copy
import numpy as np

def clear_terminal_output():
    os.system('clear')


def to_variable(arr):
    if isinstance(arr, list) or isinstance(arr, tuple):
        arr = np.array(arr)
    if isinstance(arr, np.ndarray):
        arr = Variable(torch.from_numpy(arr))
    else:
        arr = Variable(arr)

    if torch.cuda.is_available():
        arr = arr.cuda()
    return arr


def make_dir(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def copy_network(Q):
    q2 = copy.deepcopy(Q)
    if torch.cuda.is_available():
        return q2.cuda()
    return q2

