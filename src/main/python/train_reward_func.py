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

def train_reward_func(args, env):
    assert args.only_final_reward, "train reward func only for this scenario"

    ################## Visdom Stuff ######################
    if args.visdom:
        env_name = "queries: " + str(env.query_set) + "-plots-" + args.suffix
        # FIXME: just loop over all args.
        viz_params = TextVisualizer("Parameters",env=env_name)
        params_text = str(args)
        viz_params.update(params_text)

        viz_ep_loss = ScalarVisualizer("per episode loss", env=env_name,
                opts={"xlabel":"episode", "ylabel":"Reward - Sum(EstimatedRewards)",
                    "title":"Difference of Reward and Estimated Reward"})
        viz_reward_estimates = ScalarVisualizer("reward estimates", env=env_name,
                opts={"xlabel":"mdp step", "ylabel":"reward estimates", "markersize":30,
                    "show_legend":True})

    ################## End Visdom Stuff ######################

    num_input_features = env.get_num_input_features()

    R = CostModelNetwork(num_input_features)
    # Init network optimizer
    optimizer = optim.RMSprop(
        R.parameters(), lr=args.lr, alpha=0.95, eps=.01  # ,momentum=0.95,
    )

    for ep in range(args.num_episodes):
        # FIXME: should we be doing this in general or not?
        if args.adjust_learning_rate:
            adjust_learning_rate(args, optimizer, ep)

        # don't know precise episode lengths, changes based on query
        done = False
        env.reset()
        cur_ep_it = 0
        # per episode comparison between rewards / and qvals
        true_rewards = []
        # ep_estimated_rewards = []
        phi_batch = []
        final_reward = 0
        while not done:
            cur_ep_it += 1
            state = env._get_state()
            actions = env.action_space()
            # just take the action randomly since we are just trying to learn
            # the cost model, and not an optimal policy
            action_index = random.choice(range(len(actions)))
            # action_index = 0
            new_state, reward, done = env.step(action_index)
            true_reward = env.get_true_reward()
            true_rewards.append(true_reward)
            final_reward += reward
            assert new_state == state, "should be same in berkeley featurization"
            # ep_rewards.append(reward)
            phi_batch.append(state + actions[action_index])

        phi_batch = to_variable(phi_batch).float()
        est_rewards = R(phi_batch)
        est_rewards_sum = est_rewards.sum()
        est_loss = float((final_reward - est_rewards_sum).data.cpu().numpy()[0])
        print("est loss: ", est_loss)

        # TODO: training + minibatching
        # episode is done!
        learning_loss = gradient_descent(est_rewards_sum, final_reward, optimizer)
        print("learning loss: ", learning_loss.data.cpu().numpy())

        ########### updating visdom  #############
        est_rewards = est_rewards.data.cpu().numpy()
        if args.visdom:
            viz_ep_loss.update(ep, est_loss)

            assert len(est_rewards) == len(true_rewards), 'test'
            viz_reward_estimates.update(range(len(est_rewards)), est_rewards,
                        update="replace", name="estimated values")
            viz_reward_estimates.update(range(len(est_rewards)), true_rewards,
                        update="replace", name="true reward values")

