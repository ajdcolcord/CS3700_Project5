#!/usr/bin/python -u

# Austin Colcord and Nick Scheuring

import sys, socket, select, time, json, random, datetime
from message import Message


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
        self.heartbeat_timeout = 75
        self.heartbeat_timeout_start = datetime.datetime.now()
        self.current_term = 0
        self.voted_for = None
        self.votes_recieved = 0
        self.quorum_size = 3
        self.leader_id = "FFFF"
        self.node_state = "F"
        self.voted_for_me = []
        self.client_queue = []

        self.commit_index = 0
        self.last_applied = -1
        self.match_index = {}
        self.log = []

        self.key_value_store = {}

        for replica in replica_ids:
            self.match_index[replica] = -1

    def add_to_client_queue(self, message):
        """
        Adds a new incoming message (get or put) from a client into the 'buffer' - client_queue
        @:param message - Message object - the message to add to the queue
        @:return Void
        """
        print str(self.id) + ": ADDING TO CLIENT QUEUE"
        self.client_queue.append(message)

    def pull_from_queue(self):
        """
        Loads all the entries in the client_queue into our log
        @:return: Void
        """
        for entry in self.client_queue:
            self.add_client_entry_to_log(entry)
        self.client_queue = []

    def add_entry(self, command, term, client_address, mid):
        """
        Adds a new entry with the given command and the term into the log of this server. Increments
        the commit index of this server to the length of the log.
        @:param: command - One of:  - Tuple(String, Tuple(key, value))
                                    - Tuple(String, Tuple(key)
        :return: Void
        """
        print str(self.id) + ": Adding new entry: " + str(client_address) + " : " + str(mid) + " : " + str(command) +" : " + str(term)
        self.log.append((command, term, client_address, mid))
        self.commit_index = len(self.log) - 1 # 'increment' our last-committed index

    def run_command_leader(self):
        for index in range(self.last_applied + 1, self.commit_index + 1):
            entry = self.log[index]
            client_addr = entry[2]
            mess_id = entry[3]
            command = entry[0][0]
            content = entry[0][1]
            if command == 'get':
                key = content[0]
                if self.key_value_store.get(key):
                    message = {'src': self.id, 'dst': client_addr, 'leader': self.id,
                               'type': 'ok', 'MID': mess_id, 'value': self.key_value_store[key]}
                    self.send(message)
                else:
                    message = {'src': self.id, 'dst': client_addr, 'leader': self.id,
                               'type': 'fail', 'MID': mess_id}
                    self.send(message)
            elif command == 'put':
                key = content[0]
                value = content[1]
                message = {'src': self.id, 'dst': client_addr, 'leader': self.id,
                           'type': 'ok', 'MID': mess_id}
                self.send(message)

        #TODO: SERVER.apply_command/reply_to_clients(SERVER.last_committed)


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

    # def client_action(self, message):
    #     """
    #     Effect: Runs the necessary actions when receiving a client message (get or put)
    #     @:param message - Message object - the message to act upon
    #     @:return: Void
    #     """
    #     if message.type == 'get':
    #         self.add_entry((message.type, (message.key)), self.current_term)
    #         self.send_append_entry()
    #         self.get(message)
    #     elif message.type == 'put':
    #         self.add_entry((message.type, (message.key, message.value)), self.current_term)
    #         self.send_append_entry()
    #         self.put(message)

    def add_client_entry_to_log(self, message):
        """
        Effect: Runs the necessary actions when receiving a client message (get or put)
        @:param message - Message object - the message to act upon
        @:return: Void
        """
        if message.type == 'get':
            self.add_entry((message.type, (message.key)), self.current_term, message.src, message.message_id)
            # self.send_append_entry()
            # self.get(message)
        elif message.type == 'put':
            self.add_entry((message.type, (message.key, message.value)), self.current_term, message.src, message.message_id)
            # self.send_append_entry()
            # self.put(message)

    def get(self, message):
        """
        Effect: sends either a fail message or an OK message, depending on if the client's requested
        key exists in this key value store or not
        @:param message - Message object - the message received to use to send a response
        @:return: Void
        """
        if message.key not in self.key_value_store:
            self.send(message.create_fail_message())
        else:
            self.send(message.create_ok_get_message(self.key_value_store[message.key]))

    def put(self, message):
        """
        Effect: stores the message's value at the message's key located in our key value store
        @:param message - Message object - the message to get the key and value from
        """
        self.put_into_store(message.key, message.value)
        self.send(message.create_ok_put_message())


    def send_append_entries(self):
        for replica in self.match_index:
            self.send_inidivual_append_entry(replica)

        self.reset_heartbeat_timeout()
        self.get_new_election_timeout()


    def send_inidivual_append_entry(self, replica_id):
        src = self.id
        term = self.current_term
        prevLogIndex = self.match_index[replica_id]

        if prevLogIndex >= 0:
            prevLogTerm = self.log[self.match_index[replica_id]][1]
        else:
            prevLogIndex = -1
            prevLogTerm = 0

        entries_to_send = self.log[self.match_index[replica_id] + 1:]
        print str(self.id) + ": Entries to send: " + str(entries_to_send) + " Log=" + str(self.log) + " CommitIndex = " + str(self.commit_index)
        app_entry = Message.create_append_entry_message(src, replica_id, term, prevLogIndex, prevLogTerm, entries_to_send, self.commit_index)

        self.send(app_entry)

    def send_append_entry(self):
        """
        Effect: Send a new append entry message to the other replicas
        @:return: Void
        """
        src = self.id
        term = self.current_term
        prevLogIndex = self.last_applied

        if prevLogIndex >= 0:
           prevLogTerm = self.log[prevLogIndex][1]
        else:
           prevLogIndex = -1
           prevLogTerm = 0

        entries_to_send = self.log[self.last_applied + 1:]
        print str(self.id) + ": Entries to send: " + str(entries_to_send) + " Log=" + str(self.log) + " CommitIndex = " + str(self.commit_index)

        app_entry = Message.create_append_entry_message(src, "FFFF", term, prevLogIndex, prevLogTerm, entries_to_send, self.commit_index)
        self.reset_heartbeat_timeout()
        self.get_new_election_timeout()
        self.send(app_entry)

    def receive_append_entry(self, message):
        """
        Receives a new append entry, and decides wether or not to store the value into our log (follower)
        and send a response.
        @:param message - Json object - the message received from the leader
        @:return: Void
        """
        print str(self.id) + " receiving AppendEntry " + str(message)
        logEntry = message['logEntry']
        leader_prev_log_index = logEntry['prevLogIndex']
        leader_prev_log_term = logEntry['prevLogTerm']

        self.get_new_election_timeout()

        if len(self.log) == 0:
            self.log = logEntry['entries']

        else:
            if len(self.log) - 1 > leader_prev_log_index:
                print str(self.id) + " COMPARING LEADERPREVLOGTERM " + str(leader_prev_log_term) + " TO MY TERM " + str(self.log[leader_prev_log_index][1])
                if self.log[leader_prev_log_index][1] == leader_prev_log_term:
                    self.log = self.log[:leader_prev_log_index] + logEntry['entries']
                    self.commit_index = len(self.log) - 1
                    reply = {'src': self.id,
                             'dst': message['src'],
                             'type': "appendACK",
                             'leader': self.leader_id,
                             'follower_last_applied': self.last_applied,
                             'follower_commit_index': self.commit_index}
                    self.send(reply)


                elif self.log[leader_prev_log_index][1] != leader_prev_log_term:
                    # TODO: send fail, do not add to log
                    reply = {'src': self.id,
                             'dst': message['src'],
                             'type': "appendACK",
                             'leader': self.leader_id,
                             'follower_last_applied': self.last_applied,
                             'follower_commit_index': self.commit_index}
                    self.send(reply)
            else:
                # TODO: send fail, do not add to log
                reply = {'src': self.id,
                         'dst': message['src'],
                         'type': "appendACK",
                         'leader': self.leader_id,
                         'follower_last_applied': self.last_applied,
                         'follower_commit_index': self.commit_index}
                self.send(reply)

    # def receive_append_entry(self, append_entry_message):
    #     if append_entry_message.term < self.term:
    #         print ' '
    #         # TODO: reply false
    #     # if my log doesn't contain an entry contained at prevLogIndex whose term matches prevLogTerm
    #     if self.log[append_entry_message.prev_log_index][1] != append_entry_message.prev_log_term:
    #         print ' '
    #         # TODO: reply false
    #
    #     if self.log[append_entry_message.commit_index][1] != append_entry_message.term:
    #         self.log = self.log[:append_entry_message.commit_index - 1]

    def put_into_store(self, key, value):
        """
        Store the given key and value into self.key_value_store
        @:param key - String - the key to add
        @:param value - String - the value to add at the location of key
        @:return: Void
        """
        self.key_value_store[key] = value
        print str(self.id) + ": Added " + str(key) + " with value " + str(value)

    def send(self, json_message):
        """
        Takes in a json message to send on the socket
        @:param json_message: the json message to send on the socket
        """
        try:
            print str(self.id) + " SENDING MESSAGE of type  " + json_message['type'] + " -~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
            self.sock.send(json.dumps(json_message) + '\n')
        except:
            raise Exception("Could not successfully send message" + str(json_message))

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

    def send_vote(self, vote_request_from_candidate):
        """
        When a Follower, send a vote back to the requesting Candidate
        @:param vote_request_from_candidate: Message object - the vote request form a candidate
        @:return: Void
        """
        print str(self.id) + ": SENDING VOTE~!~!~!~!~!~ to : " + str(vote_request_from_candidate.src) + " requestterm = " + str(vote_request_from_candidate.term) + str(datetime.datetime.now())
        if self.voted_for is None:
            self.current_term = vote_request_from_candidate.term
            self.get_new_election_timeout()
            self.voted_for = vote_request_from_candidate.src
            json_message = vote_request_from_candidate.create_vote_message(self.id, self.leader_id)
            self.send(json_message)

    def send_vote_request(self):
        """
        When a candidate, send out this vote request to all replicas
        @:return: Void
        """
        print str(self.id) + ": SEND_VOTE_REQUEST" + str(datetime.datetime.now())
        # if self.voted_for is None:
        # send these along with RequestRPC self.current_term, self.id, self.lastLogIndex, self.lastLogTerm

        vote = Message(self.id, "FFFF", self.id, "voteRequest", 1234567890)
        json_message = vote.create_vote_request_message(self.id, self.current_term)
        self.voted_for = self.id
        self.send(json_message)

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

    def initiate_election(self):
        """
        Initiate a new election - setting voted_for to None, and voted_for_me to []
        @:return: Void
        """
        self.voted_for = self.id
        self.voted_for_me = [self.voted_for]
        self.current_term += 1
        print str(self.id) + "INITIATE_ELECTION --  INCREMENTED TERM : " + str(self.current_term)
        self.get_new_election_timeout()
        self.node_state = "C"
        self.send_vote_request()

    def receive_vote(self, message):
        """
        Process a new vote message, determining if we can add it to our counted votes for this election as a candidate
        @:param message - the vote Message object
        @:return: Void
        """
        if message.term == self.current_term and message.src not in self.voted_for_me:
            self.voted_for_me.append(message.src)
        if len(self.voted_for_me) >= self.quorum_size:
            self.change_to_leader()

        print str(self.id) + " : received vote--- messageterm=" + str(message.term) + " myterm=" + str(self.current_term) + "votefrom: " + str(message.src) + " voted4me=" + str(self.voted_for_me) + " time=" + str(datetime.datetime.now())

    def send_heartbeat(self):
        """
        Send out a new heartbeat message to all replicas, resetting our heartbeat timeout
        :return: Void
        """
        print str(self.id) + "~~~HEARTBEAT~~~"
        message = Message.create_heart_beat_message(self.id, self.current_term)
        self.reset_heartbeat_timeout()
        self.get_new_election_timeout()
        self.send(message)

    def change_to_leader(self):
        """
        Execute the actions needed to change to a leader status, resetting timeouts, leader ID, etc.
        @:return: Void
        """
        print str(self.id) + "CHANGED TO LEADER!!!!!!"
        self.get_new_election_timeout()
        self.node_state = "L"
        self.leader_id = self.id
        self.send_heartbeat()
