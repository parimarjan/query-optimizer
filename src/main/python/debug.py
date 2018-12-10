from query_opt_env import QueryOptEnv
import time
import random
import pdb

def simple_test():
    env = QueryOptEnv()
    state = env.reset()
    # state = env._get_state()
    actions = env.action_space()
    env.step(0)
    env.step(1)
    env.step(3)
    # env.reset()
    env.close()

def run_episodes(n):
    env = QueryOptEnv()
    for i in range(n):
        env.reset()
        done = False
        episode_reward = 0.00
        while not done:
            actions = env.action_space()
            ob, reward, done = env.step(random.choice(range(len(actions))))
            episode_reward += float(reward)
        print("episode {}, reward: {}".format(i, episode_reward))
    # env.reset()
    env.close()

if __name__ == "__main__":
    # simple_test()
    run_episodes(5)

