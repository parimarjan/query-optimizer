from torch import optim
import torch
from query_opt_env import QueryOptEnv
import argparse
from utils.net import TestQNetwork, CostModelNetwork
from utils.logger import Logger
# from utils.utils import copy_network, save_network, get_model_names, to_variable
from utils.utils import *
from utils.learn import egreedy_action, Qvalues, Qtargets, gradient_descent
from utils.models import ReplayMemory
from utils.viz import ScalarVisualizer, TextVisualizer, convert_to_html

import numpy as np
import time
import random
import math

# to execute the java process
import subprocess as sp
import os
import signal
import subprocess

def test(args, env):
    print("in test!")
    num_input_features = env.get_num_input_features()
    model_names = get_model_names(get_model_name(args), args.dir)
    model_name = model_names[-1]
    print("number of saved models: ", len(model_names))
    Q = TestQNetwork(num_input_features)
    Q.load_state_dict(torch.load(model_name))
    model_step = model_name_to_step(model_name)
    print("loaded Q network, model step number is: ", model_step)

    if True:
        env_name = "test queries: " + str(env.query_set)
        viz_ep_costs = ScalarVisualizer("costs", env=env_name,
                opts={"xlabel":"query number", "ylabel":"costs",
                    "title": "per query costs"})

    # just iterate over all samples in env
    num_good = 0
    num_bad = 0
    for ep in range(args.num_episodes):
        done = False
        env.reset()
        ep_rewards = []
        ep_max_qvals = []
        print("running ep: ", ep)
        while not done:
            state = env._get_state()
            actions = env.action_space()
            # FIXME: have a separate greedy function.
            action_index, all_qvals, epsilon = egreedy_action(Q, state, actions,
                    1000, decay_steps=args.decay_steps, greedy=True)
            new_state, reward, done = env.step(action_index)

            ep_rewards.append(reward)
            ep_max_qvals.append(all_qvals.max())

        total_remaining_rewards = []
        for i in range(len(ep_max_qvals)):
            total_remaining_reward = 0
            for j in range(i, len(ep_rewards)):
                total_remaining_reward += ep_rewards[j]
            total_remaining_rewards.append(total_remaining_reward)

        real_loss = np.sum(np.array(total_remaining_rewards)-np.array(ep_max_qvals))
        # rl_plan = env.get_optimized_plans("RL")
        assert args.lopt, "testing should use it"

        if args.lopt:
            # lopt_plan = env.get_optimized_plans("LOpt")
            lopt_cost = env.get_optimized_costs("LOpt")
            rl_cost = env.get_optimized_costs("RL")
            if lopt_cost < rl_cost:
                num_bad += 1
            else:
                num_good += 1
        if args.lopt:
            viz_ep_costs.update(ep, lopt_cost,
                    name="LOpt")
        viz_ep_costs.update(ep, rl_cost,
                name="RL")

        # viz_ep_costs.update(ep, math.log(lopt_cost),
                # name="LOpt")
        # viz_ep_costs.update(ep, math.log(rl_cost),
                # name="RL")

        if args.lopt:
            print("ep {}, real loss: {}, cost_diff: {}, lopt_cost:{}, \
                    rl_cost:{}".format(ep, real_loss, lopt_cost-rl_cost, lopt_cost,
                        rl_cost))
        print("num good: {}, num bad: {}".format(num_good, num_bad))

