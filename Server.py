import sys, socket, select, time, json, random, datetime
from message import Message

class Server:
    def __init__(self, id, replica_ids):
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

        self.commit_index = 0
        self.last_applied = 0
        self.match_index = {}
        self.log = []

        self.key_value_store = {}

        for replica in replica_ids:
            self.match_index[replica] = 0


    def add_entry(self, command, term):
        print str(self.id) + ": Adding new entry: " + str(command) +" : " + str(term)
        self.log.append((command, term))
        self.commit_index = len(self.log) - 1 # 'increment' our last-committed index
        self.send_append_new_entry()


    def get_new_election_timeout(self):
        self.election_timeout = random.randint(150, 300)
        self.election_timeout_start = datetime.datetime.now()

    def reset_heartbeat_timeout(self):
       self.heartbeat_timeout_start = datetime.datetime.now()

    def client_action(self, message):
        # TODO: 1) add command (type, values) to log with self.term (uncommitted)

        if message.type == 'get':
            self.add_entry((message.type, (message.key)), self.current_term)

            self.get(message)
        elif message.type == 'put':
            self.add_entry((message.type, (message.key, message.value)), self.current_term)
            self.put(message)

    def send_append_new_entry(self):

        # src, term, prevLogIndex, prevLogTerm, entries, leaderCommit
        src = self.id
        term = self.current_term

        prevLogIndex = self.commit_index - 1


        if prevLogIndex >= 0:
           prevLogTerm = self.log[prevLogIndex][1]
        else:
           prevLogIndex = -1
           prevLogTerm = 0

        #entries_to_send = []
        #if prevLogTerm == 0:
        #    entries_to_send = self.log
        #else:
        entries_to_send = self.log[self.commit_index:]
        print str(self.id) + ": Entries to send: " + str(entries_to_send) + " Log=" + str(self.log) + " CommitIndex = " + str(self.commit_index)

        leaderCommit = self.last_applied


        app_entry = Message.create_append_entry_message(src, term, prevLogIndex, prevLogTerm, entries_to_send, leaderCommit)
        self.send(app_entry)

    def receive_append_new_entry(self, message):
        print str(self.id) + " receiving AppendEntry " + str(message)
        logEntry = message['logEntry']
        leader_prev_log_index = logEntry['prevLogIndex']
        leader_prev_log_term = logEntry['prevLogTerm']

        if len(self.log) == 0:
            self.log = logEntry['entries']

        #if leader_prev_log_index == -1:
        else:
            #if self.log[self.commit_index][1] == leader_prev_log_term:
            if self.log[leader_prev_log_index][1] == leader_prev_log_term:
                self.log = self.log[:leader_prev_log_index] + logEntry['entries']
                self.commit_index = len(self.log) - 1
                # TODO: send ack, add to log
                reply = {'src': self.id,
                         'dst': message['src'],
                         'type': "appendACK",
                         'follower_last_added': self.commit_index,
                         'follower_last_committed': self.last_applied}
                self.send(reply)

            elif self.log[leader_prev_log_index][1] != leader_prev_log_term:
                # TODO: send fail, do not add to log
                reply = {'src': self.id,
                         'dst': message['src'],
                         'type': "appendACK",
                         'follower_commit_index': self.commit_index} #,
                         #'follower_last_applied': self.last_applied}
                self.send(reply)


    def receive_append_entry(self, append_entry_message):
        if append_entry_message.term < self.term:
            print ' '
            # TODO: reply false
        # if my log doesn't contain an entry contained at prevLogIndex whose term matches prevLogTerm
        if self.log[append_entry_message.prev_log_index][1] != append_entry_message.prev_log_term:
            print ' '
            # TODO: reply false

        if self.log[append_entry_message.commit_index][1] != append_entry_message.term:
            self.log = self.log[:append_entry_message.commit_index - 1]

    def get(self, message):
        print str(self.id) + ": Get"
        if message.key not in self.key_value_store:
            self.send(message.create_fail_message())
        else:

            self.send(message.create_ok_get_message(self.key_value_store[message.key]))

    def put(self, message):
        print str(self.id) + ": Put"

        self.put_into_store(message.key, message.value)
        print str(self.id) + ": Added " + str(message.key) + " with value " + str(message.value)

        # assuming successful
        self.send(message.create_ok_put_message())

    def put_into_store(self, key, value):
        self.key_value_store[key] = value

    def send(self, json_message):
        #print str(self.id) + ": sending"

        try:
            self.sock.send(json.dumps(json_message) + '\n')
        except:
            raise Exception("Could not successfully send message" + str(json_message))

    def am_i_leader(self):
        return self.leader_id == self.id

    def election_timedout(self):
        return (datetime.datetime.now() - self.election_timeout_start).microseconds > (self.election_timeout * 1000)

    def heart_beat_timedout(self):
        return (datetime.datetime.now() - self.heartbeat_timeout_start).microseconds > (self.heartbeat_timeout * 1000)

    def send_vote(self, vote_request_from_candidate):
        """
        When a Follower, send a vote back to the requesting Candidate
        """
        print str(self.id) + ": SENDING VOTE~!~!~!~!~!~ to : " + str(vote_request_from_candidate.src) + " requestterm = " + str(vote_request_from_candidate.term) + str(datetime.datetime.now())
        if self.voted_for is None:
            self.current_term = vote_request_from_candidate.term
            self.get_new_election_timeout()
            self.voted_for = vote_request_from_candidate.src
            json_message = vote_request_from_candidate.create_vote_message(self.id, self.leader_id)
            self.send(json_message)

    def send_vote_request(self):
        print str(self.id) + ": SEND_VOTE_REQUEST" + str(datetime.datetime.now())
        if self.voted_for is None:
            # send these along with RequestRPC self.current_term, self.id, self.lastLogIndex, self.lastLogTerm

            # Send a vote request message to all other followers
            vote = Message(self.id, "FFFF", self.id, "voteRequest", 1234567890)
            json_message = vote.create_vote_request_message(self.id, self.current_term)
            self.voted_for = self.id
            self.send(json_message)

    def become_follower(self, leader_id):
        self.node_state = "F"
        self.get_new_election_timeout()
        self.voted_for_me = []
        self.voted_for = None
        self.leader_id = leader_id


    def initiate_election(self):
        print str(self.id) + "INITIATE_ELECTION"
        self.voted_for = None
        self.voted_for_me = []
        self.current_term += 1
        print "INCREMENTED TERM : " + str(self.current_term)
        self.get_new_election_timeout()
        self.node_state = "C"
        self.send_vote_request()

    def receive_vote(self, message):
        # if terms are equal, and src has not voted for me yet
        print str(self.id) + " : receiving vote--- messageterm=" + str(message.term) + " myterm=" + str(self.current_term) + "votefrom: " + str(message.src) + " voted4me=" + str(self.voted_for_me) + " time=" + str(datetime.datetime.now())
        if message.term == self.current_term and message.src not in self.voted_for_me:
            self.voted_for_me.append(message.src)
            print "ADDED TO VOTED_FOR_ME: " + str(len(self.voted_for_me))
            #self.get_new_election_timeout()
        if len(self.voted_for_me) >= self.quorum_size:
            self.change_to_leader()

        print "RECEIVED: that_term=" + str(message.term) + " Candidate_term=" + str(self.current_term) + " VOTED_FOR_ME = " + str(self.voted_for_me)


            #self.request_vote_RPC(message.term, message.src, 1, 1) #, msg['lastLogIndex'], msg['lastLogTerm'])

    # def request_vote_RPC(self, term, candidateId, lastLogIndex, lastLogTerm):
    #     if term < self.current_term:
    #         return False  # reply false
    #     else:
    #         self.votes_recieved += 1

    def send_heartbeat(self):
       print str(self.id) + "~~~HEARTBEAT~~~"
       message = Message.create_heart_beat_message(self.id, self.current_term)
       self.reset_heartbeat_timeout()
       self.get_new_election_timeout()
       self.send(message)

    def change_to_leader(self):
        print str(self.id) + "CHANGED TO LEADER!!!!!!"
        self.get_new_election_timeout()
        self.node_state = "L"
        self.leader_id = self.id
        self.send_heartbeat()



