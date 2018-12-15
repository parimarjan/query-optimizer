from torch import optim
import torch
from query_opt_env import QueryOptEnv
import argparse
from utils.net import TestQNetwork, CostModelNetwork
from utils.logger import Logger
from utils.utils import *
from utils.learn import egreedy_action, Qvalues, Qtargets, gradient_descent
from utils.models import ReplayMemory
from utils.viz import ScalarVisualizer, TextVisualizer, convert_to_html

import numpy as np
import time
import random
import math
import pandas as pd

# to execute the java process
import subprocess as sp
import os
import signal
import subprocess

def test(args, env):
    num_input_features = env.get_num_input_features()
    model_names = get_model_names(get_model_name(args), args.dir)
    model_name = model_names[-1]
    print("number of saved models: ", len(model_names))
    Q = TestQNetwork(num_input_features)
    Q.load_state_dict(torch.load(model_name))
    model_step = model_name_to_step(model_name)
    print("loaded Q network, model step number is: ", model_step)

    env_name = "test queries: " + str(env.query_set)
    viz_ep_costs = ScalarVisualizer("costs", env=env_name,
            opts={"xlabel":"query number", "ylabel":"costs",
                "title": "per query costs"})

    # just iterate over all samples in env
    num_good = 0
    num_bad = 0
    lopt_opts = []
    rl_opts = []
    ld_opts = []
    queries_seen = []
    stored_exh_cost = 0
    no_exh = 0

    for ep in range(args.num_episodes):
        done = False
        query = env.reset()
        if query in queries_seen:
            print("repeating query")
            continue
        queries_seen.append(query)

        ep_rewards = []
        ep_max_qvals = []
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
            lopt_cost = env.get_optimized_costs("LOpt")
            rl_cost = env.get_optimized_costs("RL")
            exh_cost = env.get_optimized_costs("EXHAUSTIVE")
            ld_cost = env.get_optimized_costs("LEFT_DEEP")
            if (exh_cost == 0):
                print("exh cost was 0")
                exh_cost = min(lopt_cost, rl_cost, ld_cost)
                no_exh += 1
            else:
                stored_exh_cost += 1
            if (ld_cost != 0):
                if (len(ld_opts) > 0):
                    if not (ld_cost == ld_opts[-1]):
                        ld_opts.append(ld_cost / exh_cost)
                else:
                    ld_opts.append(ld_cost / exh_cost)

            lopt_opts.append(lopt_cost / exh_cost)
            rl_opts.append(rl_cost / exh_cost)

            if lopt_cost < rl_cost:
                num_bad += 1
            else:
                num_good += 1
        if args.lopt:
            viz_ep_costs.update(ep, lopt_cost,
                    name="LOpt")
        if args.exh:
            viz_ep_costs.update(ep, exh_cost,
                    name="LOpt")

        viz_ep_costs.update(ep, rl_cost,
                name="RL")

        # if args.lopt:
            # print("ep {}, real loss: {}, cost_diff: {}, lopt_cost:{}, \
                    # rl_cost:{}".format(ep, real_loss, lopt_cost-rl_cost, lopt_cost,
                        # rl_cost))

    def print_results(arr, name):
        print("results for ", name)
        print("avg: ", sum(arr) / len(arr))
        print("max: ", max(arr))
        print("std: ", np.std(arr))
        print("var: ", np.var(arr))

    print_results(lopt_opts, "lopt")
    print_results(rl_opts, "rl")
    print_results(ld_opts, "ld")
    print("stored exh cost: " , stored_exh_cost)
    print("no exh cost: ", no_exh)

    # print(len(lopt_opts), lopt_opts)
    # print(len(rl_opts), rl_opts)
    # print(len(ld_opts), ld_opts)

    # print("avg lopt opts: ", sum(lopt_opts) / len(lopt_opts))
    # print("lopt var: ",

    # print("avg rl opts: ", sum(rl_opts) / len(rl_opts))
    # print("avg ld opts: ", sum(ld_opts) / len(ld_opts))


    print("num good: {}, num bad: {}".format(num_good, num_bad))


