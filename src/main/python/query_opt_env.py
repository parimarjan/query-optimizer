import zmq
import time
import javaobj
import ast

class QueryOptEnv():
    """
    Follow all the conventions of openai gym but haven't figured out all the
    details to make it an openai environment yet.
    """

    def __init__(self):
        """
        TODO: init the zeromq server and establish connection etc.
        """
        context = zmq.Context()
        #  Socket to talk to server
        print("Going to connect to calcite server")
        self.socket = context.socket(zmq.REQ)
        self.socket.connect("tcp://localhost:5555")
        self.attr_count = int(self.send("getAttrCount"))

        # parameters
        self.reward_damping_factor = 100.00

    def get_num_input_features(self):
        return self.attr_count*2

    def send(self, msg):
        """
        """
        # stupid hack, but otherwise we weren't able to close / start the
        # server in time. And somehow without the close / start after sending a
        # reply from the server, it would just go crazy with polling stuff
        time.sleep(0.01)
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
                your total reward.
            episode_over (bool) :
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
        # reward /= self.reward_damping_factor

        # to keep rewards positive, shouldn't matter I guess?
        reward = (1.00 / -reward)*self.reward_damping_factor

        done = int(self.send(b"isDone"))
        return ob, reward, done

    def reset(self):
        """
        This should start a new episode.
        """
        # send a reset message to the server
        self.send(b"reset")
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
        action_bitsets = ast.literal_eval(rep)
        actions = []
        for a in action_bitsets:
            actions.append(self.bitset_to_features(a))
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
