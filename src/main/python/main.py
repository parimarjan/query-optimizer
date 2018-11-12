from torch import optim
from query_opt_env import QueryOptEnv
import argparse
from utils.net import TestQNetwork
from utils.logger import Logger
from utils.utils import copy_network
from utils.learn import egreedy_action, Qvalues, Qtargets, gradient_descent
from utils.models import ReplayMemory
from utils.viz import ScalarVisualizer, TextVisualizer, convert_to_html

# temp imports
import random

def read_flags():
    parser = argparse.ArgumentParser()
    # FIXME: specify params
    parser.add_argument("-num_episodes", "-e", type=int, required=False,
                                default=2000, help="number of training episodes")
    parser.add_argument("-minibatch_size", "-mbs", type=int, required=False,
                                default=32, help="")
    parser.add_argument("-train_freq", type=int, required=False,
                                default=1, help="")
    parser.add_argument("-target_update_freq", type=int, required=False,
                                default=100, help="")
    parser.add_argument("-lr", type=float, required=False,
                                default=0.001, help="")
    parser.add_argument("-log_dir", "-ld", type=str, required=False,
                                default="./logs", help="")
    parser.add_argument("-dir", type=str, required=False,
                                default="./data", help="default dir")
    return parser.parse_args()

args = read_flags()
env = QueryOptEnv()
step = 0

################## Visdom Stuff ######################
env_name = "queries: " + str(env.query_set) + "new"
viz_ep_rewards = ScalarVisualizer("rewards", env=env_name,
        opts={"xlabel":"episode", "ylabel":"rewards"})
viz_qvals_rewards = ScalarVisualizer("qvals-rewards", env=env_name,
        opts={"xlabel":"mdp step", "ylabel":"qvals-rewards"})

viz_epsilon = ScalarVisualizer("epsilon", env=env_name,
        opts={"xlabel":"episode", "ylabel":"epsilon"})

# FIXME: how to make this nicer, like add newlines etc?
viz_params = TextVisualizer("Parameters",env=env_name)
params_text = ("minibatch size: {}\r\n, learning rate: {}\r\n".format(args.minibatch_size, args.lr))
params_text = "minibatch size: {}\r\n".format(args.minibatch_size)
params_text += "learning rate: {}\r\n".format(args.lr)
params_text += "num episodes: {}\r\n".format(args.num_episodes)
viz_params.update(params_text)
viz_rl_plan = TextVisualizer("RL Query Plan", env=env_name)
viz_lopt_plan = TextVisualizer("LOpt Query Plan", env=env_name)

################## End Visdom Stuff ######################

# Initialize Logger
# log = Logger(log_dir=args.log_dir)

# Initialize replay memory D to capacity N
D = ReplayMemory(N=args.minibatch_size*1000)
num_input_features = env.get_num_input_features()
Q = TestQNetwork(num_input_features)
# log.network(Q)
Q_ = copy_network(Q)

# FIXME: experiment with this.
# Init network optimizer
optimizer = optim.RMSprop(
    Q.parameters(), lr=args.lr, alpha=0.95, eps=.01  # ,momentum=0.95,
)
orig_episode_reward = None

for ep in range(args.num_episodes):
    # print("ep : {} ".format(ep))
    episode_reward = 0.00
    # TODO: save / restore model once things start working

    # don't know precise episode lengths
    done = False

    # FIXME: need to change reset semantics to return new state.
    env.reset()
    cur_ep_it = 0
    while not done:
        # starting from 1
        step += 1
        cur_ep_it += 1
	# TODO: can print memory usage diagnostics etc.

        state = env._get_state()
        actions = env.action_space()
        action_index, qval, epsilon = egreedy_action(Q, state, actions, step)
        new_state, reward, done = env.step(action_index)
        episode_reward += reward
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

    # TODO: this stat ONLY makes sense for single query runs.
    if orig_episode_reward is None:
        orig_episode_reward = episode_reward

    print("episode {}, reward: {}".format(ep, episode_reward))
    print("episode reward improvement: {}".format(episode_reward-orig_episode_reward))

    # updating visdom
    # TODO: should ONLY do this if we know other optimizers being used.
    rl_plan = env.get_optimized_plans("RL")
    lopt_plan = env.get_optimized_plans("LOpt")

    viz_rl_plan.update(convert_to_html(rl_plan))
    viz_lopt_plan.update(convert_to_html(lopt_plan))

    viz_ep_rewards.update(ep, episode_reward, name="RL")
    viz_epsilon.update(ep, epsilon, name="RL")

