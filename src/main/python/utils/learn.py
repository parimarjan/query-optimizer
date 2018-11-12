import numpy as np
from utils import to_variable
import random
import torch

MIN_EPS = 0.0
def egreedy_action(Q, state, actions, step):
    '''
    step is only useful for annealed epsilon.
    '''
     # Initial values
    initial_epsilon, final_epsilon = 1.0, MIN_EPS
    # FIXME: might want to modify this because our episode lengths are small?
    decay_steps = float(10000)
    # decay_steps = float(100)
    # Calculate step size to move from final to initial epsilon with #decay_steps
    step_size = (initial_epsilon - final_epsilon) / decay_steps
    # Calculate annealed epsilon
    ann_eps = initial_epsilon - step * step_size
    # Define allowsd min. epsilon
    min_eps = MIN_EPS
    # Set epsilon as max(min_eps, annealed_epsilon)
    epsilon = max(min_eps, ann_eps)
    # print("*************************")
    # print("epsilon: " , epsilon)
    # print("*************************")

    # Obtain a random value in range [0,1)
    rand = np.random.uniform()

    # With probability e select random action a_t
    if rand < epsilon:
        return random.choice(range(len(actions))), epsilon
    else:
        # will need to update features for each possible action
        best_action = 0
        best_val = -100000000
        phi_batch = []
        for action, action_features in enumerate(actions):
            # since these are python lists, + is just concatenation
            phi = state + action_features
            assert len(phi) == len(state) + len(action_features), "phi len test"
            # phi = [phi]
            phi_batch.append(phi)

            # alternative way to do it: call Q function for each phi.
            # phi = to_variable(np.array(phi)).float()
            # val = Q(phi)
            # if (val > best_val):
                # best_action = action
                # best_val = val

        phi_batch = to_variable(np.array(phi_batch)).float()
        all_rewards = Q(phi_batch)

        # TODO: check that this doesn't force a gpu->cpu conversion
        for i, val in enumerate(all_rewards):
            if (val > best_val):
                best_action = action
                best_val = val

        # Note: can also find it like this, but converting best_action to cpu would
        # be slow for choosing every action.
        # best_reward, best_action = all_vals.data.max(0)
        # print("best action = ", best_action[0])

        return best_action, best_val, epsilon

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
    # print(type(phi_batch))
    all_vals = Q(phi_batch)
    # print(len(all_vals))
    return all_vals

# FIXME: should gamma be just 1.00?
def Qtargets(r_mb, new_state_mb, new_actions_mb, done_mb, Q_, gamma=1.00):
    '''
    '''
    # find the max reward we can get for each of the new states using Q_, and
    # the given actions.
    maxQ = []
    for i, state in enumerate(new_state_mb):
        # array of actions
        actions = new_actions_mb[i]
        phi_batch = []
        if len(actions) == 0:
            # maxQ.append(torch.tensor(0.00))
            maxQ.append(np.array([0.00]))
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



