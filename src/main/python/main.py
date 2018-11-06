from torch import optim
from query_opt_env import QueryOptEnv
import argparse
from utils.net import TestQNetwork
from utils.logger import Logger
from utils.utils import copy_network
from utils.learn import egreedy_action, Qvalues, Qtargets, gradient_descent
from utils.models import ReplayMemory

# temp imports
import random

def read_flags():
    parser = argparse.ArgumentParser()
    # FIXME: specify params
    parser.add_argument("-num_episodes", "-e", type=int, required=False,
                                default=10000, help="number of training episodes")
    parser.add_argument("-minibatch_size", "-mbs", type=int, required=False,
                                default=32, help="")
    parser.add_argument("-train_freq", type=int, required=False,
                                default=32, help="")

    parser.add_argument("-lr", type=float, required=False,
                                default=0.001, help="")

    parser.add_argument("-log_dir", "-ld", type=str, required=False,
                                default="./logs", help="")
    parser.add_argument("-dir", type=str, required=False,
                                default="./data", help="default dir")
    return parser.parse_args()

print("starting")
args = read_flags()
env = QueryOptEnv()
step = 0
# Initialize Logger
log = Logger(log_dir=args.log_dir)

# TODO: add replay memory
# Initialize replay memory D to capacity N
D = ReplayMemory(N=512)
# FIXME:
num_input_features = env.get_num_input_features()
print("get num input features: " , num_input_features)
Q = TestQNetwork(num_input_features)
log.network(Q)
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
    # TODO: save model if you want.

    done = False
    # don't know precise episode lengths

    # FIXME: need to change reset semantics to return new state.
    env.reset()
    while not done:
        step += 1
	# TODO: can print memory usage etc.
        state = env._get_state()

        actions = env.action_space()
        # print("egreedy action going to be called")
        action_index, epsilon = egreedy_action(Q, state, actions, step)
        # print("egreedy done!")
        new_state, reward, done = env.step(action_index)
        episode_reward += reward
        new_actions = env.action_space()
        # FIXME: we just might not need done in general for calculating
        # qtargets since we have new_actions
        D.add((state, actions[action_index], reward, new_state, new_actions, done))
        should_train_model = D.can_sample(args.minibatch_size) and \
                                step % args.train_freq == 0
        if should_train_model:
            # FIXME: should get this back from replay memory
            print("going to train model")
            exps = D.sample(args.minibatch_size)
            state_mb, actions_mb, r_mb, new_state_mb, new_actions_mb, done_mb= zip(*exps)
            qvals = Qvalues(state_mb, actions_mb, Q)
            qtargets = Qtargets(r_mb, new_state_mb, new_actions_mb,
                    done_mb, Q_)
            # print("going to do gradient descent")
            loss = gradient_descent(qtargets, qvals, optimizer)
            print("step: {}, loss: {}, epsilon: {}".format(step,
                loss.data[0], epsilon))

            # FIXME: what is the point of not updating target every time we train?
            # if step % params['target_update_freq'] == 0:
            del Q_
            Q_ = copy_network(Q)

        # store stuff etc.

    if orig_episode_reward is None:
        orig_episode_reward = episode_reward
    print("*****************************")
    print("episode {}, reward: {}".format(ep, episode_reward))
    print("episode reward improvement: {}".format(episode_reward-orig_episode_reward))
    print("*****************************")
