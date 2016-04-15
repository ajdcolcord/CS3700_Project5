import sys, socket, select, time, json, random, datetime

DEBUG = False

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
        self.heartbeat_timeout = 120
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

                if msg['type'] == 'append_entries_rpc':
                    self.become_follower(msg['src'])
                else:
                    if not self.node_state == "F":
                        self.become_follower("FFFF")
                    else:
                        self.become_follower(self.leader_id)

    def leader_receive_message(self, msg):
        """
        All Leader Message Receiving
        @:param msg - the JSON message received
        @:return: Void
        """
        if msg['type'] == 'append_entries_rpc_ack':
            self.receive_append_entries_rpc_ack(msg)

        if msg['type'] in ['get', 'put']:
            self.add_to_client_queue(msg)


    def candidate_receive_message(self, msg):
        """
        All Candidate Message Receiving
        @:param msg - the JSON message received
        @:return: Void
        """
        if msg['type'] == 'vote':
            self.receive_vote(msg)

        # if msg['type'] == 'append_entries_rpc':
        #     self.become_follower(msg['src'])

        if msg['type'] in ['get', 'put']:
            self.add_to_client_queue(msg)

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

    def add_to_client_queue(self, json_message):
        """
        Adds a new incoming message (get or put) from a client into the 'buffer' - client_queue
        @:param message - Message object - the message to add to the queue
        @:return Void
        """
        # str(self.id) + ": ADDING TO CLIENT QUEUE"
        self.client_queue.append(json_message)

    def pull_from_queue(self):
        """
        Loads all the entries in the client_queue into our log
        @:return: Void
        """
        for entry in self.client_queue:
            self.add_client_entry_to_log(entry)
        self.client_queue = []

    def add_client_entry_to_log(self, message):
        """
        Effect: Runs the necessary actions when receiving a client message (get or put)
        @:param message - Message object - the message to act upon
        @:return: Void
        """
        if message['type'] == 'get':
            self.add_entry((message['type'],
                            (message['key'])),
                           self.currentTerm,
                           message['src'],
                           message['MID'])
        elif message['type'] == 'put':
            self.add_entry((message['type'],
                            (message['key'], message['value'])),
                           self.currentTerm,
                           message['src'],
                           message['MID'])

    def add_entry(self, command, term, client_address, mid):
        """
        Adds a new entry with the given command and the term into the log of this server. Increments
        the commit index of this server to the length of the log.
        @:param: command - One of:  - Tuple(String, Tuple(key, value))
                                    - Tuple(String, Tuple(key)
        :return: Void
        """
        self.log.append((command, term, client_address, mid))


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
        for replica in self.match_index:
            self.send_append_entries_rpc_individual(replica)

        self.reset_heartbeat_timeout()

    def send_append_entries_rpc_individual(self, replica_id):
        """
        Create a new append_entries_rpc, returning the json
        :return: JSON
        """
        print str(self.id) + ": prevLogTerm... ID: " + str(replica_id) + " match_index= " + str(
            self.match_index[replica_id]) + " len_lead_log= " + str(len(self.log)) + "\n"

        prevLogTerm = 0
        if len(self.log) and self.match_index[replica_id] > 0:


            prevLogTerm = self.log[self.match_index[replica_id] - 1][1]

        entries = self.log[self.match_index[replica_id]: self.match_index[replica_id] + 50]

        append_entries_rpc = {"src": self.id,
                            "dst": replica_id,
                            "leader": self.id,
                            "type": "append_entries_rpc",
                            "term": self.currentTerm,
                            "prevLogIndex": max(0, self.match_index[replica_id] - 1), # - 1,
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

            if DEBUG:
                print str(self.id) + "len log follower - " + str(len(self.log)) + " json_prevIndex=" + str(
                    json_message['prevLogIndex']) + " Len Entries from Leader=" + str(len(json_message['entries']))

            if not len(self.log):
                self.log = json_message['entries']
                #self.last_applied = json_message['leaderLastApplied']
                if len(self.log):
                    self.run_command_follower(json_message['leaderLastApplied'])
                    self.send_append_entries_rpc_ack()

            elif len(self.log) - 1 >= json_message['prevLogIndex']:

                if self.log[json_message['prevLogIndex']][1] == json_message['prevLogTerm']:

                    self.log = self.log[:json_message['prevLogIndex'] + 1] + json_message['entries']

                    #self.last_applied = json_message['leaderLastApplied']
                    if len(json_message['entries']):
                        self.run_command_follower(json_message['leaderLastApplied'])
                        self.send_append_entries_rpc_ack()

                elif self.log[json_message['prevLogIndex']][1] != json_message['prevLogTerm']:
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
                              "match_index": len(self.log)} #(self.log)}

        self.send(append_entries_rpc)

    def send_append_entries_rpc_ack_decrement(self, leader_prev_log_index):
        """
        FOLLOWER
        :return:
        """
        if DEBUG:
            print "DECREMENT"
        append_entries_rpc = {"src": self.id,
                              "dst": self.leader_id,
                              "leader": self.leader_id,
                              "type": "append_entries_rpc_ack",
                              "term": self.currentTerm,
                              "match_index": self.last_applied} #TODO: SHOULD THiS STILL BE THIS?????????????????????
                              #"match_index": max(0, leader_prev_log_index)}

        self.send(append_entries_rpc)

    def receive_append_entries_rpc_ack(self, json_msg):
        """
        LEADER
        :param json_msg:
        :return:
        """
        if json_msg['term'] == self.currentTerm:
            if DEBUG:
                print json_msg
            self.match_index[json_msg['src']] = json_msg['match_index']
            self.check_for_quorum()

    def check_for_quorum(self):

        agreement_size = 1
        for replica in self.match_index:
            if self.match_index[replica] == len(self.log):
                agreement_size += 1

        if agreement_size == self.quorum_size:
            self.run_command_leader()
            self.last_applied = len(self.log)
            if len(self.client_queue):
                self.pull_from_queue()
                self.send_append_entries()






    def run_command_leader(self):
        """
        Runs through the items in the log ready to be applied to the state machine, executing them each one by one
        """
        for index in range(self.last_applied, len(self.log)):
            entry = self.log[index]
            client_addr = entry[2]
            mess_id = entry[3]
            command = entry[0][0]
            content = entry[0][1]

            if command == 'get':
                key = content
                value = self.key_value_store.get(key)
                if value:
                    response = {'src': self.id, 'dst': client_addr, 'leader': self.id,
                                'type': 'ok', 'MID': mess_id, 'value': value}
                    self.send(response)
                    if DEBUG: print str(self.id) + ": SEND OK GET"
                else:
                    response = {"src": self.id, "dst": client_addr, "leader": self.id,
                                "type": "fail", "MID": mess_id, "value": ""}
                    self.send(response)
                    if DEBUG: print str(self.id) + ": SEND FAIL GET"

            elif command == 'put':
                key = content[0]
                value = content[1]
                self.put_into_store(key, value)
                message = {'src': self.id, 'dst': client_addr, 'leader': self.id,
                           'type': 'ok', 'MID': mess_id}

                if DEBUG: print str(self.id) + ": SEND OK PUT"
                self.send(message)

    def run_command_follower(self, leader_last_applied):
        """
        Runs through the items in the log ready to be applied to the state machine, executing them each one by one
        @:param leader_last_applied - leader's last applied index, to apply each entry up to that in this log
        @:return: Void
        """
        for index in range(self.last_applied, min(len(self.log), leader_last_applied)):
            #if len(self.log) - 1 >= index:
            entry = self.log[index]
            command = entry[0][0]
            content = entry[0][1]
            if command == 'put':
                key = content[0]
                value = content[1]
                self.put_into_store(key, value)

        if DEBUG: print str(self.id) + " Follower Log Size " + str(len(self.log))
        # TODO: POSSIBLE POINT OF FAILURE
        self.last_applied = min(len(self.log), leader_last_applied)

    def put_into_store(self, key, value):
        """
        Store the given key and value into self.key_value_store
        @:param key - String - the key to add
        @:param value - String - the value to add at the location of key
        @:return: Void
        """
        self.key_value_store[key] = value

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
        if DEBUG: print str(self.id) + ": BECAME LEADER"
        self.reinitialize_match_index()
        self.get_new_election_timeout()
        self.node_state = "L"
        self.leader_id = self.id

        self.pull_from_queue()
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
        self.send_redirects_from_client_queue()

    def send_redirects_from_client_queue(self):
        for message in self.client_queue:
            self.send_redirect_to_client(message)
        self.client_queue = []

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

