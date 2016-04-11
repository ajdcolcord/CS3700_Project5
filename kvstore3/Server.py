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
        self.heartbeat_timeout = 120  # self.election_timeout / 2
        self.heartbeat_timeout_start = datetime.datetime.now()
        self.current_term = 0
        self.voted_for = None
        self.votes_recieved = 0
        self.quorum_size = 3
        self.leader_id = "FFFF"
        self.node_state = "F"
        self.voted_for_me = []
        self.client_queue = []

        self.failed_queue = []

        self.commit_index = 0
        self.last_applied = -1
        self.match_index = {}
        self.log = []

        self.key_value_store = {}

        for replica in replica_ids:
            self.match_index[replica] = -1

    def leader_receive_message(self, msg):
        """
        All Leader Message Receiving
        @:param msg - the JSON message received
        @:return: Void
        """
        if msg['type'] in ['get', 'put']:
            print ":" + str(self.id) + " :LEADER: RECV: " + msg['type'] + " : mid= " + msg['MID']
            self.add_to_client_queue(msg)


        # elif msg['type'] == 'heartbeatACK':
        #     print ":" + self.id + " :LEADER: RECV: " + msg['type'] + " : mid= " + str(msg['MID'])
        #     self.get_new_election_timeout()
        #     # TODO: commit log entry yet?????

        if msg['type'] == 'heartbeat':
            print ":" + str(self.id) + " :LEADER: RECV: " + msg['type'] + " : mid= " + str(msg['MID'])
            message = Message.create_message_from_json(msg)

            if message.term > self.current_term:
                print ":" + str(self.id) + " :LEADER: RECV: BECOMING FOLLOWER 0"
                self.become_follower(message.leader)

        if msg['type'] == 'voteRequest':
            print ":" + str(self.id) + " :LEADER: RECV: " + msg['type'] + " : mid= " + str(msg['MID'])
            message = Message.create_message_from_json(msg)
            if message.term > self.current_term:
                if len(self.log):
                    if msg['last_entry_term'] >= self.log[-1][1] and msg['log_size'] >= len(self.log):
                        print ":" + str(self.id) + " :LEADER: RECV: BECOMING FOLLOWER 1"
                        self.become_follower(msg['leader'])
                        #self.voted_for = msg['src']
                else:
                    print ":" + str(self.id) + " :LEADER: RECV: BECOMING FOLLOWER 2"
                    self.become_follower(msg['leader'])
                    #self.voted_for = msg['src']

        if msg['type'] == 'appendACK':
            print ":" + str(self.id) + " :LEADER: RECV: " + msg['type'] + " : src= " + str(msg['src'])
            self.receive_append_ack(msg)

    def candidate_receive_message(self, msg):
        """
        All Candidate Message Receiving
        @:param msg - the JSON message received
        @:return: Void
        """
        if msg['type'] == 'vote':
            #print str(self.id) + ": Got Vote Message----------"
            message = Message.create_message_from_json(msg)
            self.receive_vote(message)

        if msg['type'] == 'heartbeat':
            #print str(self.id) + "got ~~~HEARTBEAT~~~"
            heart_beat = Message.create_message_from_json(msg)

            if heart_beat.term >= self.current_term:
                self.current_term = heart_beat.term
                self.become_follower(heart_beat.leader)

        if msg['type'] == 'voteRequest':
            message = Message.create_message_from_json(msg)
            if message.term > self.current_term:
                self.become_follower(msg['leader'])

        if msg['type'] in ['get', 'put']:
            message = Message.create_message_from_json(msg)
            redirect_message = message.create_redirect_message("FFFF")

            self.send(redirect_message)

    def follower_receive_message(self, msg):
        """
        All Follower Message Receiving
        @:param msg - the JSON message received
        @:return: Void
        """
        if msg['type'] == 'heartbeat':
            self.receive_heartbeat(msg)

        if msg['type'] == 'voteRequest':
            self.receive_vote_request_as_follower(msg)

        if msg['type'] in ['get', 'put']:
            message = Message.create_message_from_json(msg)
            redirect_message = message.create_redirect_message(self.leader_id)

            self.send(redirect_message)

        if msg['type'] == 'appendEntry':
            self.receive_append_entry(msg)


    def add_to_client_queue(self, message):
        """
        Adds a new incoming message (get or put) from a client into the 'buffer' - client_queue
        @:param message - Message object - the message to add to the queue
        @:return Void
        """
        # str(self.id) + ": ADDING TO CLIENT QUEUE"
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
        #print str(self.id) + ": Adding new entry: " + str(client_address) + " : " + str(mid) + " : " + str(command) +" : " + str(term)
        self.log.append((command, term, client_address, mid))
        self.commit_index = len(self.log) - 1 # 'increment' our last-committed index

    def run_command_leader(self):
        """
        Runs through the items in the log ready to be applied to the state machine, executing them each one by one
        """
        for index in range(self.last_applied + 1, self.commit_index + 1):
            entry = self.log[index]
            client_addr = entry[2]
            mess_id = entry[3]
            command = entry[0][0]
            content = entry[0][1]

            if command == 'get':
                print str(self.id) + " :LEADER: " + "GET REQ RUN COMMAND : " + mess_id
                key = content
                value = self.key_value_store.get(key)
                if value:
                    response = {'src': self.id, 'dst': client_addr, 'leader': self.id,
                                       'type': 'ok', 'MID': mess_id, 'value': value}
                    print str(self.id) + " :LEADER: GET RETURN VALUE " + str(value)
                    self.send(response)
                else:
                    response = {"src": self.id, "dst": client_addr, "leader": self.id,
                                "type": "fail", "MID": mess_id, "value": ""}
                    print str(self.id) + " :LEADER: RETURN FAIL "
                    print str(self.id) + " :LEADER: GET FAIL VALUE "

                    self.send(response)

            if command == 'put':
                print str(self.id) + " :LEADER: " + "PUT REQ RUN COMMAND : " + mess_id
                key = content[0]
                value = content[1]
                self.put_into_store(key, value)
                message = {'src': self.id, 'dst': client_addr, 'leader': self.id,
                           'type': 'ok', 'MID': mess_id}
                print str(self.id) + " :LEADER: SEND OK VALUE " + str(value)

                # print str(self.id) + ": Sending (run_command_leader) out message for put: " + str(message) + "\n"
                self.send(message)

    def run_command_follower(self, leader_last_applied):
        """
        Runs through the items in the log ready to be applied to the state machine, executing them each one by one
        @:param leader_last_applied - leader's last applied index, to apply each entry up to that in this log
        @:return: Void
        """
        for index in range(self.last_applied + 1, leader_last_applied + 1):
            if len(self.log) - 1 >= index:
                entry = self.log[index]
                command = entry[0][0]
                content = entry[0][1]
                if command == 'put':
                    key = content[0]
                    value = content[1]
                    self.put_into_store(key, value)
                self.last_applied += index

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


    def add_client_entry_to_log(self, message):
        """
        Effect: Runs the necessary actions when receiving a client message (get or put)
        @:param message - Message object - the message to act upon
        @:return: Void
        """
        if message['type'] == 'get':
            self.add_entry((message['type'], (message['key'])), self.current_term, message['src'], message['MID'])
        elif message['type'] == 'put':
            self.add_entry((message['type'], (message['key'], message['value'])), self.current_term, message['src'], message['MID'])

    def send_append_entries(self):
        """
        Loop through each replica and send the necessary append entries to keep the followers up to date with this leader
        :return: Void
        """
        #print str(self.id) + " LEADER LOG- " + str(self.log)
        for replica in self.match_index:
            self.send_inidivual_append_entry(replica)


        self.reset_heartbeat_timeout()
        self.get_new_election_timeout()

    def send_inidivual_append_entry(self, replica_id):
        """
        Send an append entry for the given replica
        :param replica_id: the ID of the follower to send an append entry to
        :return: Void
        """
        src = self.id
        term = self.current_term
        prevLogIndex = self.match_index[replica_id]

        if self.match_index[replica_id] == self.commit_index:
            entries_to_send = []
            app_entry = Message.create_append_entry_message(src, replica_id, term, prevLogIndex, -1,
                                                            entries_to_send, self.last_applied)
            print "APP ENTRY EMPTY~~~~~~~~~~~~~~~~~~~"

            self.send(app_entry)


        elif prevLogIndex > len(self.log) - 1:
            app_entry = Message.create_append_entry_message(src, replica_id, term, prevLogIndex, self.log[self.last_applied][1], self.log[self.last_applied + 1:self.last_applied + 51], self.last_applied)
            # app_entry = Message.create_append_entry_message(src, replica_id, term, prevLogIndex, self.log[self.last_applied][1], self.log[self.last_applied + 1:self.last_applied + 2], self.last_applied)
            print "APP ENTRY~~~~~~~~~~~~~~~~~~~"
            self.send(app_entry)
        else:
            if prevLogIndex >= 0:
                prevLogItem = self.log[prevLogIndex]
                prevLogTerm = prevLogItem[1] #gets the previous term from log

            else:
                prevLogIndex = -1
                prevLogTerm = 0

            entries_to_send = self.log[self.match_index[replica_id] + 1:self.match_index[replica_id] + 51]
            # entries_to_send = self.log[self.match_index[replica_id] + 1:self.match_index[replica_id] + 2]

            app_entry = Message.create_append_entry_message(src, replica_id, term, prevLogIndex, prevLogTerm, entries_to_send, self.last_applied)
            print "APP ENTRY~~~~~~~~~~~~~~~~~~~"

            self.send(app_entry)

    def receive_heartbeat(self, msg):
        """
        Run the functions necessary when receiving a heartbeat, only if the heartbeat is 'valid'
        @:param msg - the message (json) from the leader
        @:return: Void
        """
        if msg['term'] >= self.current_term:
            heart_beat = Message.create_message_from_json(msg)
            self.current_term = heart_beat.term
            self.leader_id = heart_beat.leader
            self.get_new_election_timeout()
            self.run_command_follower(msg['leader_last_applied'])
            # hb_ack = heart_beat.create_heart_beat_ACK_message(self.id, self.commit_index)
            # print "HB ACK~~~~~~~~~~~~~~~~~~~"

            # self.send(hb_ack)

    def receive_vote_request_as_follower(self, msg):
        """
        Run the desired functions when receiving a vote request from a Candidate
        @:param msg - the json message received from the candidate
        @:return: Void
        """
        #print str(self.id) + ": RECEIVED VOTE REQUEST from: " + str(msg['src']) + " time=" + str(datetime.datetime.now())

        vote_req_message = Message.create_message_from_json(msg)

        if vote_req_message.term >= self.current_term:
            if self.voted_for is None or self.voted_for == vote_req_message.src:
                if len(self.log):
                    if msg['last_entry_term'] >= self.log[-1][1] and msg['log_size'] >= len(self.log):
                        self.send_vote(vote_req_message)
                        self.get_new_election_timeout()
                else:
                    self.send_vote(vote_req_message)
                    self.get_new_election_timeout()

    def receive_append_entry(self, message):
        """
        Receives a new append entry, and decides wether or not to store the value into our log (follower)
        and send a response.
        @:param message - Json object - the message received from the leader
        @:return: Void
        """
        #print str(self.id) + " receiving AppendEntry " + str(message)



        if message['term'] >= self.current_term:

            self.run_command_follower(message['leader_last_applied'])

            self.current_term = message['term']
            self.leader_id = message['src']

            logEntry = message['logEntry']
            leader_prev_log_index = logEntry['prevLogIndex']
            leader_prev_log_term = logEntry['prevLogTerm']

            print str(self.id) + "log_entry_length_received = " + str(len(logEntry['entries']))

            self.get_new_election_timeout()
            if len(self.log) == 0:
                self.log = logEntry['entries']
                self.commit_index = len(self.log) - 1


                if len(logEntry['entries']) > 0:

                    reply = {'src': self.id,
                             'dst': message['src'],
                             'type': "appendACK",
                             'leader': self.leader_id,
                             'follower_last_applied': self.last_applied,
                             'follower_commit_index': self.commit_index}

                    print str(self.id) + 'empty log--- Log Entries: ' + str(logEntry['entries']) + 'Log now=' + str(self.log) + " c_i=" + str(self.commit_index) + ' l_a=' + str(self.last_applied)

                    self.send(reply)


            # if leader_prev_log_term = 0:
            #     self.log =



            else:
                print str(self.id) + ": received append_entry - my_prev_term=" + str(
                    self.log[leader_prev_log_index][1]) + " Leader_prev_term=" + str(leader_prev_log_term)

                if self.log[leader_prev_log_index][1] == leader_prev_log_term:
                    #self.log = self.log[:leader_prev_log_index + 1] + logEntry['entries']
                    self.log = self.log[:leader_prev_log_index + 1] + logEntry['entries']

                    self.commit_index = len(self.log) - 1
                    self.run_command_follower(logEntry['leader_last_applied'])
                    if len(logEntry['entries']) > 0:

                        reply = {'src': self.id,
                                 'dst': message['src'],
                                 'type': "appendACK",
                                 'leader': self.leader_id,
                                 'follower_last_applied': self.last_applied,
                                 'follower_commit_index': self.commit_index}
                        self.send(reply)

                elif self.log[leader_prev_log_index][1] != leader_prev_log_term:
                    # TODO: send fail, do not add to log
                    self.commit_index = len(self.log) - 1
                    reply = {'src': self.id,
                             'dst': message['src'],
                             'type': "appendACK",
                             'leader': self.leader_id,
                             'follower_last_applied': self.last_applied,
                             'follower_commit_index': self.commit_index}

                    self.send(reply)


    def receive_append_ack(self, msg):
        """
        Determine what to do when receiving an append_entry acknowledgement from a follower
        @:param msg - the json message received from the follower
        """
        self.get_new_election_timeout()
        follow_source = msg['src']
        follower_commit_index = int(msg['follower_commit_index'])
        self.match_index[follow_source] = follower_commit_index


        agreement_size = 1
        for replica in self.match_index:
            if self.match_index[replica] >= self.commit_index:
                agreement_size += 1

        if agreement_size == self.quorum_size:
            # TODO: self.apply_command/reply_to_clients(self.last_committed)
            print str(self.id) + "QUORUM REACHED :::"
            self.run_command_leader()
            #agreement_size += 1
            self.last_applied = self.commit_index



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
        #print str(self.id) + ": SENDING VOTE~!~!~!~!~!~ to : " + str(vote_request_from_candidate.src) + " requestterm = " + str(vote_request_from_candidate.term) + str(datetime.datetime.now())
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
        #print str(self.id) + ": SEND_VOTE_REQUEST" + str(datetime.datetime.now())
        # if self.voted_for is None:
        # send these along with RequestRPC self.current_term, self.id, self.lastLogIndex, self.lastLogTerm
        if len(self.log):
            vote = Message(self.id, "FFFF", self.id, "voteRequest", 1234567890)
            json_message = vote.create_vote_request_message(self.id, self.current_term, self.log[-1][1], len(self.log))
            self.voted_for = self.id
            self.send(json_message)
        else:
            vote = Message(self.id, "FFFF", self.id, "voteRequest", 1234567890)
            json_message = vote.create_vote_request_message(self.id, self.current_term, 0, len(self.log))
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
        for replica in self.match_index:
            self.match_index[replica] = -1

        self.voted_for = self.id
        self.voted_for_me = [self.voted_for]
        self.current_term += 1
        #print str(self.id) + "INITIATE_ELECTION --  INCREMENTED TERM : " + str(self.current_term)
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

        #print str(self.id) + " : received vote--- messageterm=" + str(message.term) + " myterm=" + str(self.current_term) + "votefrom: " + str(message.src) + " voted4me=" + str(self.voted_for_me) + " time=" + str(datetime.datetime.now())

    def send_heartbeat(self):
        """
        Send out a new heartbeat message to all replicas, resetting our heartbeat timeout
        :return: Void
        """
        print str(self.id) + "~~~HEARTBEAT~~~"
        message = Message.create_heart_beat_message(self.id, self.current_term, self.last_applied)
        self.reset_heartbeat_timeout()
        self.get_new_election_timeout()
        self.send(message)

    def change_to_leader(self):
        """
        Execute the actions needed to change to a leader status, resetting timeouts, leader ID, etc.
        @:return: Void
        """
        # print str(self.id) + "CHANGED TO LEADER!!!!!!"
        self.get_new_election_timeout()
        self.node_state = "L"
        self.leader_id = self.id
        self.send_heartbeat()
