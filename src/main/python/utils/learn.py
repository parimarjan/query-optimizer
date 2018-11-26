import numpy as np
from utils import to_variable
import random
import torch

MIN_EPS = 0.05
def egreedy_action(Q, state, actions, step, decay_steps=1000.00, greedy=False):
    '''
    step is only useful for annealed epsilon.
    '''
     # Initial values
    initial_epsilon, final_epsilon = 1.0, MIN_EPS
    # Calculate step size to move from final to initial epsilon with #decay_steps
    step_size = (initial_epsilon - final_epsilon) / decay_steps
    # Calculate annealed epsilon
    ann_eps = initial_epsilon - step * step_size
    # Define allowsd min. epsilon
    min_eps = MIN_EPS
    # Set epsilon as max(min_eps, annealed_epsilon)
    epsilon = max(min_eps, ann_eps)

    # Obtain a random value in range [0,1)
    rand = np.random.uniform()

    # FIXME: for debugging, we always calculate the optimal Qvalue, and return
    # it, even if we are using epsilon.
    # With probability e select random action a_t
    # if rand < epsilon:
        # return random.choice(range(len(actions))), 0.0, epsilon
    # else:

    # will need to update features for each possible action
    # best_action = -2
    # best_val = -100000000
    phi_batch = []
    for action, action_features in enumerate(actions):
        # since these are python lists, + is just concatenation
        phi = state + action_features
        assert len(phi) == len(state) + len(action_features), "phi len test"
        phi_batch.append(phi)

    phi_batch = to_variable(np.array(phi_batch)).float()
    all_rewards = Q(phi_batch)
    best_val, best_action = all_rewards.max(0)
    best_action = int(best_action.data.cpu().numpy()[0][0])

    # Note: can also find it like this, but converting best_action to cpu would
    # be slow for choosing every action.
    # best_reward, best_action = all_vals.data.max(0)
    # print("best action = ", best_action[0])
    if rand < epsilon and not greedy:
        best_action = random.choice(range(len(actions)))

    return best_action, best_val.data.cpu().numpy()[0][0], epsilon

def Qvalues(state_mb, actions_mb, Q):
    '''
    Note: for each state_i \in state_mb, there is only one action in
    actions_mb.
    returns an array with the Qvalues achieved after passing Q(state_i concat
    actions_i).
    '''
    phi_batch = []
    for i, action_features in enumerate(actions_mb):
        phi = state_mb[i] + action_features
        phi_batch.append(phi)

    phi_batch = to_variable(np.array(phi_batch)).float()
    all_vals = Q(phi_batch)
    return all_vals

def Qtargets(r_mb, new_state_action_mb, done_mb, Q_, gamma=1.0):
    '''
    '''
    maxQ = []
    for i, phi_batch in enumerate(new_state_action_mb):
        done = done_mb[i]
        phi_batch = to_variable(np.array(phi_batch)).float()
        all_vals = Q_(phi_batch)
        best_reward = all_vals.max().data[0]
        assert done == 0 or done == 1, "sanity check"
        maxQ.append(best_reward*(1 - float(done)))

    assert len(maxQ) == len(r_mb)
    # now we can find the target qvals using standard reward +
    # q(new_state)*discount_factor formula.
    # Note: we don't care about 'done' because we already take care of
    # situations where episode was done based on the length of the actions
    # array.
    maxQ = to_variable(maxQ).float()
    r_mb = to_variable(r_mb).float()
    target = r_mb + gamma*maxQ
    return target

# FIXME: should gamma be just 1.00?
def Qtargets_old(r_mb, new_state_mb, new_actions_mb, done_mb, Q_, gamma=1.0):
    '''
    '''
    # find the max reward we can get for each of the new states using Q_, and
    # the given actions.
    maxQ = []
    for i, state in enumerate(new_state_mb):
        # array of actions
        actions = new_actions_mb[i]
        done = done_mb[i]
        phi_batch = []
        if done:
            # FIXME: new array creations should go through a common point, like
            # to_variable.
            maxQ.append(torch.cuda.FloatTensor([[0]]))
            # maxQ.append(torch.FloatTensor([[0]]))
        else:
            for _, action_features in enumerate(actions):
                # since these are python lists, + is just concatenation
                phi = state + action_features
                assert len(phi) == len(state) + len(action_features), "phi len test"
                phi_batch.append(phi)
            phi_batch = to_variable(np.array(phi_batch)).float()
            all_vals = Q_(phi_batch)
            best_reward, _ = all_vals.data.max(0)
            maxQ.append(best_reward)

    assert len(maxQ) == len(r_mb)
    maxQ = to_variable(torch.cat(maxQ))
    # now we can find the target qvals using standard reward +
    # q(new_state)*discount_factor formula.
    # Note: we don't care about 'done' because we already take care of
    # situations where episode was done based on the length of the actions
    # array.
    r_mb = to_variable(r_mb).float()
    target = r_mb + gamma*maxQ
    return target

def gradient_descent(y, q, optimizer):
    # Clear previous gradients before backward pass
    optimizer.zero_grad()

    # Run backward pass
    error = (y - q)

    # PN: is this important? Should probably depend on the range we choose for rewards?
    # Clip error to range [-1, 1]
    # error = error.clamp(min=-1, max=1)

    # Square error
    error = error**2
    error = error.sum()

    # q.backward(error.data)
    error.backward()

    # Perfom the update
    optimizer.step()

    return error



