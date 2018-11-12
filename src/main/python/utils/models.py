import numpy as np
import json
import utils
import sys
import torch
from torch.autograd import Variable
import random



class ReplayMemory():

    def __init__(self, N, load_existing=False, data_dir="./data"):
        self.max = N
        self.memory = []

    def _init_arrays(self, N):
        '''
        Inits memory arrays as empty numpy arrays with expected shapes
        '''
        pass

    def _save_arrays(self, path, size):
        '''
        Saves slice of current memory arrays with given size into input path
        '''
        pass

    def _load_arrays(self, path, size):
        '''
        Loads memory arrays from path with input size
        '''
        pass


    def to_dict(self, saved_size):
        '''
        Converts current replay memory into dict representation
        '''
	pass

    def add(self, experience):
        '''
        This operation adds a new experience e, replacing the earliest experience if arrays are full.
        '''
        if len(self.memory) == self.max:
            # then will need to replace something
            self.memory = self.memory[1:-1]
        assert len(self.memory) != self.max, 'len must be less'
        self.memory.append(experience)

    def sample(self, size):
        '''
        Samples slice of arrays with input size.
        '''
        return random.sample(self.memory, size)

    def can_sample(self, sample_size):
        '''
        Returns true if item count is at least as big as sample size.
        '''
        pass
        return len(self.memory) >= sample_size

    def save(self, size):
        '''
        Saves replay memory (attributes and arrays).
        '''
        pass

    def load(self):
        '''
        Loads replay memory (attributes and arrays) into self, if possible.
        '''
        pass

    def arrays_size(self):
        '''
        Returns the size in bytes of the memory's array
        '''
        pass
