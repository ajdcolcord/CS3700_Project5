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
        self.current_term = 0
        self.voted_for = None
        self.votes_recieved = 0
        self.quorum_size = 3
        self.leader_id = 0
        self.node_state = "F"

        self.key_value_store = {}

    def get_new_election_timeout(self):
        self.election_timeout = random.randint(150, 300)
        self.election_timeout_start = datetime.datetime.now()
        self.current_term += 1
        self.voted_for = None

    def client_action(self, message):
        if message.type == 'get':
            self.get(message)
        elif message.type == 'put':
            self.put(message)

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
        print str(self.id) + ": sending"

        try:
            self.sock.send(json.dumps(json_message) + '\n')
        except:
            raise Exception("Could not successfully send message" + str(json_message))

    def am_i_leader(self):
        return self.leader_id == self.id

    def election_timedout(self):
        return (datetime.datetime.now() - self.election_timeout_start).microseconds > self.election_timeout

    def send_vote(self, vote_request_from_candidate):
        """
        When a Follower, send a vote back to the requesting Candidate
        """
        if self.voted_for is None:
            self.get_new_election_timeout()
            self.voted_for = vote_request_from_candidate.src
            json_message = vote_request_from_candidate.create_vote_message(self.id)
            self.send(json_message)

    def send_vote_request(self):
        if self.voted_for is None:
            # Send a vote request message to all other followers
            vote = Message(self.id, "FFFF", self.id, "voteRequest", 1234567890)
            json_message = vote.create_vote_request_message(self.id, self.current_term)
            self.voted_for = self.id
            self.send(json_message)

    def initiate_election(self):
        self.get_new_election_timeout()
        self.node_state = "C"
        self.send_vote_request()

    def receive_vote(self, message):
        self.request_vote_RPC(message.term, message.src, 1, 1) #, msg['lastLogIndex'], msg['lastLogTerm'])

    def request_vote_RPC(self, term, candidateId, lastLogIndex, lastLogTerm):
        if term < self.current_term:
            return False  # reply false
        else:
            self.votes_recieved += 1

    def change_to_leader(self):
        self.node_state = "L"
