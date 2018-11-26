import zmq
import time
# import javaobj
import ast
import random
import numpy as np

DEFAULT_PORT = 5600
def get_port():
    context = zmq.Context()
    #  Socket to talk to server
    print("Going to connect to port server")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:2000")
    # TODO: make it only wait for limited time
    time.sleep(0.1)
    socket.send(b"python")
    ret = socket.recv()
    print("received: ", ret)
    return ret

    # return DEFAULT_PORT


class QueryOptEnv():
    """
    Follow all the conventions of openai gym but haven't figured out all the
    details to make it an openai environment yet.
    """


    def __init__(self, port=5605):
        """
        TODO: init the zeromq server and establish connection etc.
        """
        # Want to find a port number to talk on
        # port = get_port()
        context = zmq.Context()
        #  Socket to talk to server
        print("Going to connect to calcite server")
        self.socket = context.socket(zmq.REQ)
        self.socket.connect("tcp://localhost:" + str(port))
        self.query_set = self.send("getCurQuerySet")
        self.attr_count = int(self.send("getAttrCount"))
        # TODO: figure this out using the protocol too. Or set it on the java
        # side using some protocol.
        self.only_join_condition_attributes = False

        # parameters
        self.reward_damping_factor = 1.00
        # will store min_reward / max_reward for each unique query
        # will map query: (min_reward, max_reward)
        self.reward_mapper = {}
        # these values will get updated in reset.
        self.min_reward = None
        self.max_reward = None

    def run_random_episode(self):
        '''
        have a run of the episode and return the min / max reward from this run.
        '''
        done = False
        min_reward = 10000000
        max_reward = -10000000
        while not done:
            state = self._get_state()
            actions = self.action_space()
            ob, reward, done = self.step(random.choice(range(len(actions))))
            if reward < min_reward:
                min_reward = reward
            if reward > max_reward:
                max_reward = reward

        return min_reward, max_reward

    def get_optimized_plans(self, name):
        # ignore response
        self.send(b"getOptPlan")
        resp = self.send(name)
        return resp

    def get_optimized_costs(self, name):
        self.send(b"getJoinsCost")
        resp = self.send(name)
        return float(resp)

    def get_num_input_features(self):
        if self.only_join_condition_attributes:
            return self.attr_count*2
        else:
            return self.attr_count*3

    def send(self, msg):
        """
        """
        # stupid hack, but otherwise we weren't able to close / start the
        # server in time. And somehow without the close / start after sending a
        # reply from the server, it would just go crazy with polling stuff
        time.sleep(0.05)
        self.socket.send(msg)
        ret = self.socket.recv()
	return ret

    def close(self):
	# can ignore the message sent by it
        self.send(b"end")
        self.socket.close()

    def step(self, action):
        """
        Parameters
        ----------
        action: int, must be one of the indices from the action space vector.

        Returns
        -------
        ob, reward, episode_over, info : tuple
            ob (object) :
                an environment-specific object representing your observation of
                the environment.
            reward (float) :
                amount of reward achieved by the previous action. The scale
                varies between environments, but the goal is always to increase
                whether it's time to reset the environment again. Most (but not
                all) tasks are divided up into well-defined episodes, and done
                being True indicates the episode has terminated. (For example,
                perhaps the pole tipped too far, or you lost your last life.)
            info (dict) :
                 diagnostic information useful for debugging. It can sometimes
                 be useful for learning (for example, it might contain the raw
                 probabilities behind the environment's last state change).
                 However, official evaluations of your agent are not allowed to
                 use this for learning.
        """
        self.send(b"step")
        self._take_action(str(action))
        # ask for results
        ob = self._get_state()
        reward = float(self.send(b"getReward"))
        reward = self.normalize_reward(reward)
        done = int(self.send(b"isDone"))
        return ob, reward, done

    def normalize_reward(self, reward):
        # to keep rewards positive, shouldn't matter I guess?
        # reward = (1.00 / -reward)*self.reward_damping_factor
        # reward /= self.reward_damping_factor
        if self.min_reward is not None:
            reward = np.interp(reward, [self.min_reward, self.max_reward], [0,1])
        return reward


    def reset(self):
        """
        This should start a new episode.
        """
        # send a reset message to the server
        query = self.send(b"reset")
        # print("reset. Next query is: ", query)
        # print("self.reward mapper is: ")
        # for k, v in self.reward_mapper.iteritems():
            # print(v)
        # we want to be able to figure out which query this was so we can set
        # the appropriate max / min ranges to scale the rewards.
        if query in self.reward_mapper:
            self.min_reward = self.reward_mapper[query][0]
            self.max_reward = self.reward_mapper[query][1]
        else:
            # FIXME: dumb hack so self.step does right thing when executing
            # random episode.
            self.min_reward = None
            self.max_reward = None
            self.min_reward, self.max_reward = self.run_random_episode()
            self.reward_mapper[query] = (self.min_reward, self.max_reward)

            # FIXME: dumb hack
            self.reset()

        # return self._get_state()


    def action_space(self):
        """
        TODO: flags for the featurization scheme, e.g., one hot v/s something
        else.

        Returns
        -------
        @action features: an array of arrays. Each element is a feature vector
        reprsenting a possible action.
        """
        rep = self.send(b"getActions")

        if self.only_join_condition_attributes:
            action_bitsets = ast.literal_eval(rep)
            actions = []
            for a in action_bitsets:
                actions.append(self.bitset_to_features(a))
            return actions
        else:
            # this is so shitty.
            rep = rep.replace("<", "(")
            rep = rep.replace(">", ")")
            action_bitsets = ast.literal_eval(rep)
            actions = []
            for a in action_bitsets:
                left_features = self.bitset_to_features(a[0])
                right_features = self.bitset_to_features(a[1])
                actions.append(left_features + right_features)

            assert len(actions[0]) == self.attr_count*2
            return actions

    def _get_state(self):
        rep = self.send(b"getState")
        state = ast.literal_eval(rep)
        return self.bitset_to_features(state)

    def bitset_to_features(self, arr):
        '''
        arr is an array of ints with the values being the turned on bits of a
        bitset. Converts this into an array of size self.attr_count, with the
        appropriate elements 1.
        '''
        features = []
        for i in range(self.attr_count):
            if i in arr:
                features.append(1.00)
            else:
                features.append(0.00)
        return features

    def _take_action(self, action):
        """
        Called from env.step - communicates action to calcite / updates state
        etc.
        """
        self.send(action)

    def _get_reward(self):
        """
        current reward, after every action has been taken.
        """
        pass
