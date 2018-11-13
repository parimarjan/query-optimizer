from torch import optim
from query_opt_env import QueryOptEnv
import argparse
from utils.net import TestQNetwork
from utils.logger import Logger
from utils.utils import copy_network
from utils.learn import egreedy_action, Qvalues, Qtargets, gradient_descent
from utils.models import ReplayMemory
from utils.viz import ScalarVisualizer, TextVisualizer, convert_to_html
import numpy as np

# temp imports
import random

FILL_UP_GOOD_RUNS = False

def read_flags():
    parser = argparse.ArgumentParser()
    # FIXME: specify params
    parser.add_argument("-num_episodes", "-e", type=int, required=False,
                                default=2000, help="number of training episodes")
    parser.add_argument("-minibatch_size", "-mbs", type=int, required=False,
                                default=32, help="")
    parser.add_argument("-replay_memory_size", type=int, required=False,
                                default=2000, help="")
    parser.add_argument("-train_freq", type=int, required=False,
                                default=2, help="")
    parser.add_argument("-decay_steps", type=float, required=False,
                                default=2000.00, help="")
    parser.add_argument("-target_update_freq", type=int, required=False,
                                default=100, help="")
    parser.add_argument("-port", type=int, required=False,
                                default=5602, help="")

    parser.add_argument("-lr", type=float, required=False,
                                default=0.001, help="")
    parser.add_argument("-log_dir", "-ld", type=str, required=False,
                                default="./logs", help="")
    parser.add_argument("-dir", type=str, required=False,
                                default="./data", help="default dir")
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

args = read_flags()
env = QueryOptEnv(args.port)
step = 0

################## Visdom Stuff ######################
env_suffix = "-debug-no-and"
env_name = "queries: " + str(env.query_set) + "-plots" + env_suffix
env_name_plans = "queries: " + str(env.query_set) + "-plans" + env_suffix

viz_ep_rewards = ScalarVisualizer("rewards", env=env_name,
        opts={"xlabel":"episode", "ylabel":"rewards"})
viz_ep_costs = ScalarVisualizer("costs", env=env_name,
        opts={"xlabel":"episode", "ylabel":"costs"})

viz_real_loss = ScalarVisualizer("real-loss", env=env_name,
        opts={"xlabel":"episode", "ylabel":"loss", "title":"Difference of rewards and qvalues"})
viz_qvals_rewards = ScalarVisualizer("qvals-rewards", env=env_name,
        opts={"xlabel":"mdp step", "ylabel":"qvals + rewards", "markersize":30,
            "show_legend":True})
viz_epsilon = ScalarVisualizer("epsilon", env=env_name,
        opts={"xlabel":"episode", "ylabel":"epsilon"})

# FIXME: just loop over all args.
viz_params = TextVisualizer("Parameters",env=env_name)
params_text = ("minibatch size: {}\r\n, learning rate: {}\r\n".format(args.minibatch_size, args.lr))
params_text = "minibatch size: {}\r\n".format(args.minibatch_size)
params_text += "learning rate: {}\r\n".format(args.lr)
params_text += "num episodes: {}\r\n".format(args.num_episodes)
params_text += "replay memory size: {}\r\n".format(args.replay_memory_size)
params_text += "decay steps: {}\r\n".format(args.decay_steps)
viz_params.update(params_text)

viz_rl_plan = TextVisualizer("RL Query Plan", env=env_name_plans)
viz_lopt_plan = TextVisualizer("LOpt Query Plan", env=env_name_plans)

################## End Visdom Stuff ######################

# Initialize Logger
# log = Logger(log_dir=args.log_dir)

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

orig_episode_reward = None
best_episode_reward = None

for ep in range(args.num_episodes):
    # TODO: save / restore model once things start working

    # don't know precise episode lengths
    done = False
    # FIXME: need to change reset semantics to return new state as in openai.
    env.reset()
    cur_ep_it = 0
    # per episode comparison between rewards / and qvals
    ep_rewards = []
    ep_qvals = []

    while not done:
        # starting from 1
        step += 1
        cur_ep_it += 1
	# TODO: can print memory usage diagnostics etc.

        state = env._get_state()
        actions = env.action_space()
        action_index, qval, epsilon = egreedy_action(Q, state, actions, step,
                decay_steps=args.decay_steps)
        new_state, reward, done = env.step(action_index)
        ep_rewards.append(reward)
        ep_qvals.append(qval)

        new_actions = env.action_space()
        D.add((state, actions[action_index], reward, new_state, new_actions, done))
        should_train_model = D.can_sample(args.minibatch_size) and \
                                step % args.train_freq == 0
        if should_train_model:
            print("going to train model")
            exps = D.sample(args.minibatch_size)
            state_mb, actions_mb, r_mb, new_state_mb, new_actions_mb, done_mb= zip(*exps)
            qvals = Qvalues(state_mb, actions_mb, Q)
            qtargets = Qtargets(r_mb, new_state_mb, new_actions_mb,
                    done_mb, Q_)
            loss = gradient_descent(qtargets, qvals, optimizer)
            print("step: {}, loss: {}, epsilon: {}".format(step,
                loss.data[0], epsilon))

            if step % args.target_update_freq == 0:
                del Q_
                Q_ = copy_network(Q)

    episode_reward = sum(ep_rewards)
    # Note: this stat ONLY makes sense for single query runs.
    if orig_episode_reward is None:
        orig_episode_reward = episode_reward
        best_episode_reward = episode_reward

    print("episode {}, reward: {}".format(ep, episode_reward))
    print("episode reward improvement: {}".format(episode_reward-orig_episode_reward))

    if (episode_reward > best_episode_reward) and FILL_UP_GOOD_RUNS:
        D.fill_up_with_episode()
        best_episode_reward = episode_reward

    ########### updating visdom  #############
    # TODO: do we need name here?

    viz_ep_rewards.update(ep, episode_reward, name="RL")
    viz_epsilon.update(ep, epsilon)

    print("ep qvals: ", ep_qvals)
    print(type(ep_qvals[0]))
    total_remaining_rewards = []
    for i in range(len(ep_qvals)):
        total_remaining_reward = 0
        for j in range(i, len(ep_rewards)):
            total_remaining_reward += ep_rewards[j]
        total_remaining_rewards.append(total_remaining_reward)

    real_loss = np.sum(np.array(total_remaining_rewards)-np.array(ep_qvals))
    print("real loss: ", real_loss)
    viz_real_loss.update(ep, real_loss)
    viz_qvals_rewards.update(range(len(ep_qvals)), ep_qvals, update="replace", name="qvals")
    viz_qvals_rewards.update(range(len(ep_qvals)), total_remaining_rewards,
            update="replace",
            name="rewards")

    # FIXME: test robustness if LOpt not being used etc.
    rl_plan = env.get_optimized_plans("RL")
    lopt_plan = env.get_optimized_plans("LOpt")
    lopt_cost = env.get_optimized_costs("LOpt")
    rl_cost = env.get_optimized_costs("RL")

    # lopt_cost = find_cost(lopt_plan)
    print("lopt cost: ", lopt_cost)
    print("rl cost: ", rl_cost)

    viz_ep_costs.update(ep, lopt_cost,
            name="LOpt")
    viz_ep_costs.update(ep, rl_cost,
            name="RL")


    # viz_ep_rewards.update(ep, env.normalize_reward(-lopt_cost),
            # name="LOpt")

    viz_rl_plan.update(convert_to_html(rl_plan))
    viz_lopt_plan.update(convert_to_html(lopt_plan))



