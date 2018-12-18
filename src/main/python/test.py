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
pd.set_option("display.precision", 2)
from collections import defaultdict
import re
import glob

# to execute the java process
import subprocess as sp
import os
import signal
import subprocess

ALGS = ["LOpt", "LEFT_DEEP", "EXHAUSTIVE", "RL"]
QUERY_DIR = "./join-order-benchmark/"

def get_num_from_string(string):
    '''
    '''
    # matches scientific notation and stuff.
    numbers = re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?",
            string)
    # all the things we are printing so far has just 1 num.
    if len(numbers) >= 1:
        return int(numbers[0])
    else:
        return None


def get_query_map():
    fns = glob.glob(QUERY_DIR + "*.sql")
    print("number of queries: ", len(fns))
    qmap = {}
    for f in fns:
        query = open(f, "r").read()
        # FIXME: hack
        # qmap[query[0:100]] = get_num_from_string(f)
        # do calcite rewrites here
        query = query.replace(";","")
        query = query.replace("!=", "<>")
        qmap[query] = get_num_from_string(f)
        # qmap[hash(query.strip())] = get_num_from_string(f)
    return qmap

def test(args, env):
    query_map = get_query_map()

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
    queries_seen = []
    all_data = defaultdict(list)

    for ep in range(args.num_episodes):
        done = False
        query = env.reset()
        if query in queries_seen:
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
        assert args.lopt, "testing should use it"

        # query_key = query[0:100]
        # query_key = hash(query.strip())
        query_key = query
        if query_key in query_map:
            all_data["query_template"].append(query_map[query_key])
        join_order_seq = env.send("joinOrderSeq")
        all_data["RL Join Order"].append(join_order_seq.strip())

        for alg in ALGS:
            cost = env.get_optimized_costs(alg)
            all_data[alg].append(cost)

    all_df = pd.DataFrame(all_data)

    file_name = args.suffix + str(args.query) + args.costModel + ".csv"
    all_df.to_csv(file_name)

    print(all_df.sort("query_template"))
    print("number of entries without exhaustive stats: ", all_df[all_df.EXHAUSTIVE==0].count().EXHAUSTIVE)
    # all_df[all_df.EXHAUSTIVE == 0] = all_df.LEFT_DEEP[all_df.EXHAUSTIVE == 0]

    # remove 0s
    df = ((all_df.loc[all_df.EXHAUSTIVE != 0])).drop("query_template",axis=1)
    df = df.drop("RL Join Order", axis=1)
    df = df.div(df.EXHAUSTIVE, axis=0)

    stats=pd.DataFrame()
    stats["Mean"] = df.mean()
    stats["Var"] = df.var()
    stats["Max"]= df.max()
    stats["Min"]= df.min()
    print(stats)

    import pdb
    pdb.set_trace()


