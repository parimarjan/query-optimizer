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

# temp imports
import pdb

# TODO: execute multiple runs together
JAVA_PROCESS = None

def to_bitset(num_attrs, arr):
    ret = [i for i, val in enumerate(arr) if val == 1.0]
    for i, r in enumerate(ret):
        ret[i] = r % num_attrs
    return ret

def check_actions_in_state(num_attrs, state, actions):
    sb = set(to_bitset(num_attrs, state))
    for a in actions:
        ab = set(to_bitset(num_attrs, a))
        if not ab.issubset(sb):
            return False
    return True

def get_model_name(args):
    if args.suffix == "":
        return str(hash(str(args)))
    else:
        return args.suffix

def read_flags():
    parser = argparse.ArgumentParser()
    # FIXME: specify params
    parser.add_argument("-num_episodes", "-e", type=int, required=False,
                                default=2000, help="number of training episodes")
    parser.add_argument("-minibatch_size", "-mbs", type=int, required=False,
                                default=16, help="")
    parser.add_argument("-replay_memory_size", type=int, required=False,
                                default=2000, help="")
    parser.add_argument("-train_freq", type=int, required=False,
                                default=1, help="")
    parser.add_argument("-decay_steps", type=float, required=False,
                                default=400.00, help="")
    parser.add_argument("-target_update_freq", type=int, required=False,
                                default=100, help="")
    parser.add_argument("-model_save_freq", type=int, required=False,
                                default=100, help="")
    parser.add_argument("-port", type=int, required=False,
                                default=5602, help="")
    parser.add_argument("-query", type=int, required=False, default=40,
            help="Query to execute. -1 means all queries")
    parser.add_argument("-lr", type=float, required=False,
                                default=0.001, help="")
    parser.add_argument("-min_lr", type=float, required=False,
                                default=0.001, help="")
    parser.add_argument("-min_eps", type=float, required=False,
                                default=0.05, help="")
    # boolean
    parser.add_argument("-debug", type=int, required=False,
                                default=0, help="")
    parser.add_argument("-train", type=int, required=False,
                                default=1, help="")
    parser.add_argument("-test", type=int, required=False,
                                default=0, help="")
    parser.add_argument("-train_reward_func", type=int, required=False,
                                default=0, help="")
    parser.add_argument("-verbose", type=int, required=False,
                                default=0, help="")
    parser.add_argument("-only_final_reward", type=int, required=False,
                                default=0, help="")
    parser.add_argument("-lopt", type=int, required=False,
                                default=1, help="use the LOpt planner")
    parser.add_argument("-adjust_learning_rate", type=int, required=False,
                                default=0, help="adjust pytorch learning rate while training")
    parser.add_argument("-reward_damping", type=int, required=False,
                                default=0, help="use reward damping")
    parser.add_argument("-clip_min_max", type=int, required=False,
                                default=0, help="clip min max normalization")
    parser.add_argument("-visdom", type=int, required=False, default=1,
            help="do visdom vizualizations for this run or not")


    # strings
    parser.add_argument("-dir", type=str, required=False,
                                default="./data", help="default dir")
    parser.add_argument("-suffix", type=str, required=False, default="",
            help="Suffix for the visdom visualizvisualization or file names \
            etc.")
    parser.add_argument("-reward_normalization", type=str, required=False,
                                default="min_max", help="type of reward normalization")

    # parser.add_argument("-mode", type=str, required=False, default="train",
            # help="test or train.")

    return parser.parse_args()

def find_cost(planOutput):
    '''
    parses planOutput to find the associated cost after the last join.
    '''
    all_lines = planOutput.split("\n")
    for s in all_lines:
        if "Join" in s:
            # the first Join we see would be the top most join.
            # JdbcJoin(condition=[=($40, $3)], joinType=[inner]): rowcount
            # = 480541.9921875, cumulative cost = {516195.625 rows, 1107.0
            words = s.split(" ")
            for i, w in enumerate(words):
                if w == "rows,":
                    cost = float(words[i-1].replace("{",""))
                    return cost

def start_java_server(args):
    global JAVA_PROCESS
    JAVA_EXEC_FORMAT = 'mvn -e exec:java -Dexec.mainClass=Main \
    -Dexec.args="-query {query} -port {port} -mode {mode} -onlyFinalReward \
    {final_reward} -lopt {lopt} -python 1"'
    # FIXME: setting the java directory relative to the directory we are
    # executing it from?
    mode = ""
    if args.train:
        mode = "train"
    else:
        mode = "test"

    cmd = JAVA_EXEC_FORMAT.format(query = args.query, port = str(args.port),
            mode=mode, final_reward=args.only_final_reward, lopt=args.lopt)
    print("cmd is: ", cmd)
    JAVA_PROCESS = sp.Popen(cmd, shell=True)
    print("started java server!")

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
        if args.visdom:
            viz_ep_loss.update(ep, est_loss)

            assert len(est_rewards) == len(true_rewards), 'test'
            viz_reward_estimates.update(range(len(est_rewards)), est_rewards,
                        update="replace", name="estimated values")
            viz_reward_estimates.update(range(len(est_rewards)), true_rewards,
                        update="replace", name="true reward values")

def train(args, env):

    ################## Visdom Setup ######################
    if args.visdom:
        env_name = "queries: " + str(env.query_set) + "-plots-" + args.suffix
        # env_name_plans = "queries: " + str(env.query_set) + "-plans" + args.suffix

        viz_ep_rewards = ScalarVisualizer("rewards", env=env_name,
                opts={"xlabel":"episode", "ylabel":"rewards"})
        ep_costs_title = "Cost for query {}".format(args.query)
        viz_ep_costs = ScalarVisualizer("costs", env=env_name,
                opts={"xlabel":"episode", "ylabel":"costs",
                    "title":ep_costs_title})
        viz_qval_stats = ScalarVisualizer("QVals", env=env_name,
                opts={"xlabel":"Query Action", "ylabel":"QValue",
                    "title":"QValue Stats"})
        viz_real_loss = ScalarVisualizer("real-loss", env=env_name,
                opts={"xlabel":"episode", "ylabel":"Reward - QValue",
                    "title":"Difference of Rewards and Qvalues"})
        viz_qvals_rewards = ScalarVisualizer("qvals-rewards", env=env_name,
                opts={"xlabel":"Query Action", "ylabel":"qvals + rewards", "markersize":30,
                    "show_legend":True})
        viz_epsilon = ScalarVisualizer("epsilon", env=env_name,
                opts={"xlabel":"episode", "ylabel":"epsilon"})

        # FIXME: just loop over all args.
        viz_params = TextVisualizer("Parameters",env=env_name)
        params_text = str(args)
        viz_params.update(params_text)

        # viz_rl_plan = TextVisualizer("RL Query Plan", env=env_name_plans)
        # viz_lopt_plan = TextVisualizer("LOpt Query Plan", env=env_name_plans)

    ################## End Visdom Setup ######################

    # Initialize replay memory D to capacity N
    D = ReplayMemory(N=args.replay_memory_size)
    num_input_features = env.get_num_input_features()

    Q = TestQNetwork(num_input_features)
    Q_ = copy_network(Q)

    # FIXME: experiment with this.
    # Init network optimizer
    optimizer = optim.RMSprop(
        Q.parameters(), lr=args.lr, alpha=0.95, eps=.01  # ,momentum=0.95,
    )

    step = 0

    # DEBUG stuff
    model_names = None
    if args.debug:
        # then there better be a bunch of models saved with name suffix
        model_names = get_model_names(get_model_name(args), args.dir)
        print("number of saved models: ", len(model_names))

    for ep in range(args.num_episodes):
        if args.adjust_learning_rate:
            adjust_learning_rate(args, optimizer, ep)

        if args.debug:
            # let's change Q based on saved models
            if ep > len(model_names):
                print("finished going through all models")
                break
            model_name = model_names[ep]
            Q.load_state_dict(torch.load(model_name))
            model_step = model_name_to_step(model_name)
            print("updated Q, model step number is: ", model_step)
            # for this episode, we will use the given Q

        # don't know precise episode lengths, changes based on query
        done = False
        # FIXME: need to change reset semantics to return new state as in openai.
        env.reset()
        cur_ep_it = 0
        # per episode comparison between rewards / and qvals
        ep_rewards = []
        ep_max_qvals = []
        # The size would be = number of actions in the given episode. each
        # element will be a vector of all the qvals for that action in the
        # episode.
        ep_all_qvals = []

        while not done:
            if (step % args.model_save_freq == 0):
                save_network(Q, get_model_name(args), step, args.dir)

            # starting from 1
            step += 1
            cur_ep_it += 1
            state = env._get_state()
            actions = env.action_space()
            assert check_actions_in_state(env.attr_count, state, actions), "actions must be in state"
            action_index, all_qvals, epsilon = egreedy_action(Q, state, actions, step,
                    decay_steps=args.decay_steps, min_eps=args.min_eps)
            ep_all_qvals.append(all_qvals)

            new_state, reward, done = env.step(action_index)
            assert new_state == state, "should be same in berkeley featurization"
            ep_rewards.append(reward)
            if args.verbose:
                print("cur_ep_it: {}, reward: {}".format(cur_ep_it, reward))
            ep_max_qvals.append(all_qvals.max())

            new_actions = env.action_space()
            new_state_actions = [new_state+a for a in new_actions]
            assert len(new_state_actions[0]) == env.get_num_input_features()
            D.add((state, actions[action_index], reward, new_state_actions, done))
            should_train_model = D.can_sample(args.minibatch_size) and \
                                    step % args.train_freq == 0
            if should_train_model:
                exps = D.sample(args.minibatch_size)
                # state_mb, actions_mb, r_mb, new_state_mb, new_actions_mb, done_mb= zip(*exps)
                state_mb, actions_mb, r_mb, new_state_actions_mb, done_mb= zip(*exps)
                qvals = Qvalues(state_mb, actions_mb, Q)
                qtargets = Qtargets(r_mb, new_state_actions_mb, done_mb, Q_)
                qdiff = (qtargets-qvals).sum().data[0]
                # FIXME: debug stuff
                num_action_choices = len(new_state_actions_mb[0]) - done_mb[0]

                loss = gradient_descent(qtargets, qvals, optimizer)
                if args.verbose:
                    print("step: {}, loss: {}, qtargets-qvals: {}, epsilon: {}".format(step,
                        loss.data[0], qdiff, epsilon))

                if step % args.target_update_freq == 0:
                    del Q_
                    Q_ = copy_network(Q)

        # episode is done!

        episode_reward = sum(ep_rewards)
        if args.verbose:
            print("episode {}, reward: {}".format(ep, episode_reward))

        ########### updating visdom  #############
        if args.visdom:
            viz_ep_rewards.update(ep, episode_reward, name="RL")
            viz_epsilon.update(ep, epsilon)

            total_remaining_rewards = []
            for i in range(len(ep_max_qvals)):
                total_remaining_reward = 0
                for j in range(i, len(ep_rewards)):
                    total_remaining_reward += ep_rewards[j]
                total_remaining_rewards.append(total_remaining_reward)

            real_loss = np.sum(np.array(total_remaining_rewards)-np.array(ep_max_qvals))
            viz_real_loss.update(ep, real_loss)
            viz_qvals_rewards.update(range(len(ep_max_qvals)), ep_max_qvals, update="replace", name="qvals")
            viz_qvals_rewards.update(range(len(ep_max_qvals)), total_remaining_rewards,
                    update="replace",
                    name="rewards")

            # Qval stats
            stats1 = []
            stats2 = []
            stats3 = []
            for i, action_all_qvals in enumerate(ep_all_qvals):
                sorted_action_all_qvals = sorted(action_all_qvals)
                maxQVal = sorted_action_all_qvals[-1]
                if len(action_all_qvals) > 1:
                    max2QVal = sorted_action_all_qvals[-2]
                else:
                    max2QVal = sorted_action_all_qvals[-1]

                minQVal = sorted_action_all_qvals[0]
                assert maxQVal >= max2QVal, "check"
                assert maxQVal >= minQVal , "check"
                stats1.append(abs(maxQVal-max2QVal))
                stats2.append(abs(maxQVal - (sum(action_all_qvals) /
                    float(len(action_all_qvals)))))
                stats3.append(abs(maxQVal - minQVal))

            viz_qval_stats.update(range(len(ep_all_qvals)), stats1,
                update="replace", name="Qmax - Qmax2")
            viz_qval_stats.update(range(len(ep_all_qvals)), stats2,
                update="replace", name="Qmax - Qmean")
            viz_qval_stats.update(range(len(ep_all_qvals)), stats3,
                update="replace", name="Qmax - Qmin")


            # FIXME: test robustness if LOpt not being used etc.
            rl_plan = env.get_optimized_plans("RL")
            if args.lopt:
                lopt_plan = env.get_optimized_plans("LOpt")
                lopt_cost = env.get_optimized_costs("LOpt")
            rl_cost = env.get_optimized_costs("RL")

            if args.lopt:
                # FIXME: temporary special casing
                if args.query < 0:
                    viz_ep_costs.update(ep, math.log(lopt_cost),
                        name="LOpt")
                else:
                    viz_ep_costs.update(ep, lopt_cost,
                            name="LOpt")

            if args.query < 0:
                viz_ep_costs.update(ep, math.log(rl_cost),
                        name="RL")
            else:
                viz_ep_costs.update(ep, rl_cost,
                        name="RL")

            # viz_rl_plan.update(convert_to_html(rl_plan))
            # viz_lopt_plan.update(convert_to_html(lopt_plan))

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
            action_index, qvalue, epsilon = egreedy_action(Q, state, actions,
                    1000, decay_steps=args.decay_steps, greedy=True)
            new_state, reward, done = env.step(action_index)

            ep_rewards.append(reward)
            ep_max_qvals.append(qvalue)

        total_remaining_rewards = []
        for i in range(len(ep_max_qvals)):
            total_remaining_reward = 0
            for j in range(i, len(ep_rewards)):
                total_remaining_reward += ep_rewards[j]
            total_remaining_rewards.append(total_remaining_reward)

        real_loss = np.sum(np.array(total_remaining_rewards)-np.array(ep_max_qvals))
        rl_plan = env.get_optimized_plans("RL")
        assert args.lopt, "testing should use it"

        if args.lopt:
            lopt_plan = env.get_optimized_plans("LOpt")
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

def cleanup():
    # Send the signal to
    # os.killpg(os.getpgid(JAVA_PROCESS.pid), signal.SIGTERM)
    JAVA_PROCESS.kill()
    print("killed the java server")

def main():
    args = read_flags()
    start_java_server(args)
    time.sleep(5)

    env = QueryOptEnv(port=args.port, only_final_reward=args.only_final_reward,
            reward_normalization=args.reward_normalization,
            reward_damping=args.reward_damping, clip_min_max=args.clip_min_max)
    try:
        if args.train_reward_func:
            train_reward_func(args, env)
        elif args.train:
            train(args, env)
        elif args.test:
            test(args, env)
    except Exception:
        cleanup()
        raise
    print("after train!")
    cleanup()

main()
