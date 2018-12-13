import os
import errno
import torch
from torch.autograd import Variable
import copy
import numpy as np
import glob

def clear_terminal_output():
    os.system('clear')


def to_variable(arr):
    if isinstance(arr, list) or isinstance(arr, tuple):
        arr = np.array(arr)
    if isinstance(arr, np.ndarray):
        arr = Variable(torch.from_numpy(arr), requires_grad=True)
    else:
        arr = Variable(arr, requires_grad=True)

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

def save_network(model, name, step, out_dir):
    '''
    saves the model for the given step, and deletes models for older
    steps.
    '''
    out_dir = '{}/models/'.format(out_dir)
    # Make Dir
    make_dir(out_dir)
    # find files in the directory that match same format:
    fnames = glob.glob(out_dir + name + "*")
    # for f in fnames:
        # # delete old ones
        # os.remove(f)

    # Save model
    torch.save(model.state_dict(), '{}/{}_step_{}'.format(out_dir, name, step))

def model_name_to_step(name):
    return int(name.split("_")[-1])

def get_model_names(name, out_dir):
    '''
    returns sorted list of the saved model_step files.
    '''
    out_dir = '{}/models/'.format(out_dir)
    # Make Dir
    # find files in the directory that match same format:
    fnames = sorted(glob.glob(out_dir + name + "*"), key=model_name_to_step)
    return fnames

def get_model_name(args):
    if args.suffix == "":
        return str(hash(str(args)))
    else:
        return args.suffix

def adjust_learning_rate(args, optimizer, epoch):
    """
    FIXME: think about what makes sense for us?
    Sets the learning rate to the initial LR decayed by half every 30 epochs
    """
    # lr = args.lr * (0.1 ** (epoch // 30))
    lr = args.lr * (0.5 ** (epoch // 30))
    lr = max(lr, args.min_lr)
    if (epoch % 30 == 0):
        print("new lr is: ", lr)
    for param_group in optimizer.param_groups:
        param_group['lr'] = lr

