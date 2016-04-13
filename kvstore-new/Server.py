import sys, socket, select, time, json, random, datetime


class Server:
    """
    Defines the class of a Server (replica)
    """

    def __init__(self, id, replica_ids):
        """
        Initializes a new server with the given ID number and the list of replica IDs
        """
        self.id = id
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.sock.connect(id)
        self.replica_ids = replica_ids
        self.election_timeout = random.randint(150, 300)
        self.election_timeout_start = datetime.datetime.now()
        self.heartbeat_timeout = 120  # self.election_timeout / 2
        self.heartbeat_timeout_start = datetime.datetime.now()
        self.currentTerm = 0
        self.voted_for = None
        self.votes_recieved = 0
        self.quorum_size = 3
        self.leader_id = "FFFF"
        self.node_state = "F"
        self.voted_for_me = []
        self.client_queue = []

        self.failed_queue = []

        self.last_applied = 0
        self.match_index = {}
        self.reinitialize_match_index()
        self.log = []

        self.key_value_store = {}

    def all_receive_message(self, msg):
        if msg['type'] in ['request_vote_rpc', 'append_entries_rpc']:
            if msg['term'] > self.currentTerm:
                self.currentTerm = msg['term']
                self.become_follower("FFFF")

    def leader_receive_message(self, msg):
        """
        All Leader Message Receiving
        @:param msg - the JSON message received
        @:return: Void
        """
        if msg['type'] == 'append_entries_rpc_ack':
            self.receive_append_entries_rpc_ack(msg)

        if msg['type'] in ['get', 'put']:
            self.send_fail_message(msg)

    def candidate_receive_message(self, msg):
        """
        All Candidate Message Receiving
        @:param msg - the JSON message received
        @:return: Void
        """
        if msg['type'] == 'vote':
            self.receive_vote(msg)

        if msg['type'] == 'append_entries_rpc':
            self.become_follower(msg['src'])

        if msg['type'] in ['get', 'put']:
            self.send_redirect_to_client(msg)

    def follower_receive_message(self, msg):
        """
        All Follower Message Receiving
        @:param msg - the JSON message received
        @:return: Void
        """

        if msg['type'] == 'request_vote_rpc':
            self.receive_request_vote_rpc(msg)

        if msg['type'] == 'append_entries_rpc':
            self.receive_append_entries_rpc(msg)

        if msg['type'] in ['get', 'put']:
            self.send_redirect_to_client(msg)


    def send_fail_message(self, client_json_message):
        fail_message = {"src": self.id,
                        "dst": client_json_message['src'],
                        "leader": self.id,
                        "type": "fail",
                        "MID": client_json_message['MID']}
        self.send(fail_message)


    def send_redirect_to_client(self, client_json_message):
        redirect_message = {"src": self.id,
                            "dst": client_json_message['src'],
                            "leader": self.leader_id,
                            "type": "redirect",
                            "MID": client_json_message['MID']}
        self.send(redirect_message)


    def reinitialize_match_index(self):
        for replica in self.replica_ids:
            self.match_index[replica] = 0

    def get_lastLogTerm(self):
        lastLogTerm = 0
        if len(self.log):
            lastLogTerm = self.log[len(self.log) - 1][1]
        return lastLogTerm

    def send_request_vote_rpc(self):
        """
        Create a new requst_vote_rpc, returning the json
        :return: JSON
        """
        request_vote_rpc = {"src": self.id,
                            "dst": "FFFF",
                            "leader": "FFFF",
                            "type": "request_vote_rpc",
                            "term": self.currentTerm,
                            "lastLogIndex": len(self.log) - 1,
                            "lastLogTerm": self.get_lastLogTerm()}
        self.send(request_vote_rpc)

    def receive_request_vote_rpc(self, json_message):
        if json_message['term'] >= self.currentTerm:
            if self.voted_for is None or self.voted_for == json_message['src']:
                if self.get_lastLogTerm() <= json_message['lastLogTerm']:
                    if len(self.log) - 1 <= json_message['lastLogIndex']:
                        # self.currentTerm = json_message['term']
                        vote = {"src": self.id,
                                "dst": json_message['src'],
                                "leader": "FFFF",
                                "type": "vote",
                                "term": self.currentTerm}
                        self.voted_for = json_message['src']
                        self.send(vote)
                        self.get_new_election_timeout()
                        print str(self.id) + ": VOTED FOR ======" + str(json_message['src'])

    def receive_vote(self, json_message):
        """
        Process a new vote message, determining if we can add it to our counted votes for this election as a candidate
        @:param message - the vote Message object
        @:return: Void
        """
        if json_message['term'] == self.currentTerm and json_message['src'] not in self.voted_for_me:
            self.voted_for_me.append(json_message['src'])

        if len(self.voted_for_me) >= self.quorum_size:
            self.change_to_leader()
        print str(self.id) + ": Voted For Me..." + str(self.voted_for_me)

    def initiate_election(self):
        """
        Initiate a new election - setting voted_for to None, and voted_for_me to []
        @:return: Void
        """
        self.node_state = "C"
        self.currentTerm += 1
        self.voted_for = self.id
        self.voted_for_me = [self.voted_for]
        self.get_new_election_timeout()
        self.send_request_vote_rpc()

    def send_append_entries(self):
        """
        Loop through each replica and send the necessary append entries to keep the followers up to date with this leader
        :return: Void
        """
        # print str(self.id) + " LEADER LOG- " + str(self.log)
        for replica in self.match_index:
            self.send_append_entries_rpc_individual(replica)

        self.reset_heartbeat_timeout()

    def send_append_entries_rpc_individual(self, replica_id):
        """
        Create a new append_entries_rpc, returning the json
        :return: JSON
        """
        prevLogTerm = 0
        if len(self.log):
            prevLogTerm = self.log[self.match_index[replica_id] - 1][1]

        entries = self.log[self.match_index[replica_id]:]

        append_entries_rpc = {"src": self.id,
                            "dst": replica_id,
                            "leader": self.id,
                            "type": "append_entries_rpc",
                            "term": self.currentTerm,
                            "prevLogIndex": self.match_index[replica_id] - 1,
                            "prevLogTerm": prevLogTerm,
                            "entries": entries,
                            "leaderLastApplied": self.last_applied}
        self.send(append_entries_rpc)

    def receive_append_entries_rpc(self, json_message):
        """
        FOLLOWER
        :param json_message:
        :return:
        """
        if json_message['term'] >= self.currentTerm:
            self.get_new_election_timeout()
            self.leader_id = json_message['src']

            if not len(self.log):
                self.log = json_message['entries']
                self.last_applied = json_message['leaderLastApplied']
                if len(self.log):
                    self.send_append_entries_rpc_ack()

            elif len(self.log) - 1 >= json_message['prevLogIndex']:
                # print str(self.id) + " -- LEN LOG INDEX = " + str(len(self.log) - 1) + ""
                if self.log[json_message['prevLogIndex']][1] == json_message['prevLogTerm']:
                    self.log = self.log[:json_message['prevLogIndex']] + json_message['entries']
                    self.last_applied = json_message['leaderLastApplied']
                    if len(json_message['entries']):
                        self.send_append_entries_rpc_ack()

                if self.log[json_message['prevLogIndex']][1] != json_message['prevLogTerm']:
                    self.log = self.log[:json_message['prevLogIndex']] + json_message['entries']
                    self.send_append_entries_rpc_ack_decrement(json_message['prevLogIndex'])

            else:
                self.send_append_entries_rpc_ack_decrement(json_message['prevLogIndex'])


    def send_append_entries_rpc_ack(self):
        """
        FOLLOWER
        :return:
        """
        append_entries_rpc = {"src": self.id,
                              "dst": self.leader_id,
                              "leader": self.leader_id,
                              "type": "append_entries_rpc_ack",
                              "term": self.currentTerm,
                              "match_index": len(self.log)}

        self.send(append_entries_rpc)

    def send_append_entries_rpc_ack_decrement(self, leader_prev_log_index):
        """
        FOLLOWER
        :return:
        """
        append_entries_rpc = {"src": self.id,
                              "dst": self.leader_id,
                              "leader": self.leader_id,
                              "type": "append_entries_rpc_ack",
                              "term": self.currentTerm,
                              "match_index": leader_prev_log_index - 1}

        self.send(append_entries_rpc)

    def receive_append_entries_rpc_ack(self, json_msg):
        """
        LEADER
        :param json_msg:
        :return:
        """
        if json_msg['term'] == self.currentTerm:
            self.match_index[json_msg['src']] = self.match_index[json_msg['match_index']]


    def send(self, json_message):
        """
        Takes in a json message to send on the socket
        @:param json_message: the json message to send on the socket
        """
        try:
            self.sock.send(json.dumps(json_message) + '\n')
        except:
            raise Exception("Could not successfully send message" + str(json_message))

    def change_to_leader(self):
        """
        Execute the actions needed to change to a leader status, resetting timeouts, leader ID, etc.
        @:return: Void
        """
        self.reinitialize_match_index()
        self.get_new_election_timeout()
        self.node_state = "L"
        self.leader_id = self.id
        self.send_append_entries()

    def become_follower(self, leader_id):
        """
        Execute the actions to become a follower (resetting node_state, election timeout, votedforme, votedfor), and
        set this leader_id to the input leader_id
        @:param leader_id - Int - the ID of the new leader
        @:return Void
        """
        self.node_state = "F"
        self.get_new_election_timeout()
        self.voted_for_me = []
        self.voted_for = None
        self.leader_id = leader_id

    def get_new_election_timeout(self):
        """
        Effect: Resets the election timeout (selecting a new random timeout range)
        :return: Void
        """
        self.election_timeout = random.randint(150, 300)
        self.election_timeout_start = datetime.datetime.now()

    def reset_heartbeat_timeout(self):
        """
        Effect: Resets the heartbeat timeout
        :return: Void
        """
        self.heartbeat_timeout_start = datetime.datetime.now()

    def election_timedout(self):
        """
        Checks if this election cycle of this server has timed out
        @:return: Boolean - True if timedout, False if not
        """
        return (datetime.datetime.now() - self.election_timeout_start).microseconds > (self.election_timeout * 1000)

    def heart_beat_timedout(self):
        """
        Checks if this heart_beat cycle of this server has timed out (to check if it (leader) needs to resend a heartbeat)
        @:return: Boolean - True if timedout, False if not
        """
        return (datetime.datetime.now() - self.heartbeat_timeout_start).microseconds > (self.heartbeat_timeout * 1000)

