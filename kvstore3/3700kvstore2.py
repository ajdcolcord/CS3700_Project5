#!/usr/bin/env python

import sys, socket, select, time, json, random, datetime
from message import Message

# Your ID number
my_id = sys.argv[1]
# my_id = 1111

# The ID numbers of all the other replicas
replica_ids = sys.argv[2:]

# Connect to the network. All messages to/from other replicas and clients will
# occur over this socket
sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
sock.connect(my_id)

last = 0

ELECTION_TIMEOUT = random.randint(150,300)
ELECTION_TIMEOUT_START = datetime.datetime.now()
CURRENT_TERM = 0
VOTED_FOR = None
VOTES_RECIEVED = 0
QUORUM_SIZE = 3



def get_new_election_timeout():
    global CURRENT_TERM
    global ELECTION_TIMEOUT
    global ELECTION_TIMEOUT_START
    global VOTED_FOR

    ELECTION_TIMEOUT = random.randint(150,300)
    ELECTION_TIMEOUT_START = datetime.datetime.now()
    CURRENT_TERM += 1
    VOTED_FOR = None


KEY_VALUE_STORE = {}
# Node state: Follower, Candidate, Leader
NODE_STATE = "F"
LEADER_ADDRESS = 0


# If the node doesn't hear from a leader they become a candidate.
# The candidate then requests votes from other nodes
# Nodes reply with their votes
# Leader Election - Candidate becomes leader with majority of votes
# All changes to the system now go through the leader
# Each change is added as an entry in the leaders log
# The log entry is uncommitted so it won't update the node's value
# To commit the entry the node first replicates it to the follower nodes
# The leader waits until the majority have written the entry
# The entry is now committed on the leader
# The leader then notifies the followers that the entry is committed
# This is log replication
#
#
# Leader Election:
# There are two timeout settings that control elections
# Election timeout - The amount of time a follower waits to become a candidate
# ET is randomized between 150 and 300 ms
# After the election timeout the follower becomes a candidate and starts a new election term and votes for itself
# The candidate then sends out vote requests to other nodes
# if the receiving node has not yet voted in this election term then it votes for the candidate
# and the node resets its election timeout
# Once a candidate has the majority it becomes the leader
# The leader begins sending out Append Entries messages to its followers.
# These messages are sent in intervals specified by the heartbeat timeout.
# Followers then respond to each Append Entries message.
# This election term will continue until a follower stops receiving heartbeats and becomes a candidate.
# Let's stop the leader and watch a re-election happen.
# Node C is now leader of term 2.
# Requiring a majority of votes guarantees that only one leader can be elected per term.
# If two nodes become candidates at the same time then a split vote can occur.
# Let's take a look at a split vote example...
# Two nodes both start an election for the same term...
# ...and each reaches a single follower node before the other.
# Now each candidate has 2 votes and can receive no more for this term.
# The nodes will wait for a new election and try again.
# Node C received a majority of votes in term 5 so it becomes leader.
#
# Log Replication
# Once we have a leader elected we need to replicate all changes to our system to all nodes.
# This is done by using the same Append Entries message that was used for heartbeats.
# Let's walk through the process.
# First a client sends a change to the leader.
# The change is appended to the leader's log...
# ...then the change is sent to the followers on the next heartbeat.
# An entry is committed once a majority of followers acknowledge it...
# ...and a response is sent to the client.
# Now let's send a command to increment the value by "2".
# Our system value is now updated to "7". (Started at 5)
# Raft can even stay consistent in the face of network partitions.
# Let's add a partition to separate A & B from C, D & E.
# Because of our partition we now have two leaders in different terms.
# Let's add another client and try to update both leaders.
# One client will try to set the value of node B to "3".
# Node B cannot replicate to a majority so its log entry stays uncommitted.
# The other client will try to set the value of node C to "8".
# This will succeed because it can replicate to a majority.
# Now let's heal the network partition.
# Node B will see the higher election term and step down.
# Both nodes A & B will roll back their uncommitted entries and match the new leader's log.
# Our log is now consistent across our cluster.
#



def client_action(message):
    global KEY_VALUE_STORE

    if message.type == 'get':
        get(message)
    elif message.type == 'put':
        put(message)


def get(message):
    global KEY_VALUE_STORE

    print str(my_id) + ": Get"
    if message.key not in KEY_VALUE_STORE:
        send(message.create_fail_message())

    else:
        send(message.create_ok_get_message(KEY_VALUE_STORE[message.key]))


def put(message):
    global KEY_VALUE_STORE

    print str(my_id) + ": Put"

    put_into_store(message.key, message.value)
    print str(my_id) + ": Added " + str(message.key) + " with value " + str(message.value)

    #assuming successful
    send(message.create_ok_put_message())

def put_into_store(key, value):
    global KEY_VALUE_STORE

    KEY_VALUE_STORE[key] = value


def send(json_message):
    print str(my_id) + ": sending"

    try:
        sock.send(json.dumps(json_message) + '\n')
    except:
        raise Exception("Could not successfully send message" + str(json_message))

def am_i_leader():
    global LEADER_ADDRESS
    global my_id

    return LEADER_ADDRESS == my_id

def election_timedout():
    global ELECTION_TIMEOUT_START

    return (datetime.datetime.now() - ELECTION_TIMEOUT_START).microseconds > ELECTION_TIMEOUT


def send_vote(vote_request_from_candidate):
    """
    When a Follower, send a vote back to the requesting Candidate
    """
    global VOTED_FOR

    if VOTED_FOR is None:
        VOTED_FOR = vote_request_from_candidate.src
        json_message = vote_request_from_candidate.create_vote_message()
        send(json_message)


def send_vote_request():
    global VOTED_FOR

    if VOTED_FOR is None:
        # Send a vote request message to all other followers
        vote = Message(my_id, "FFFF", my_id, "voteRequest", 1234567890)
        json_message = vote.create_vote_request_message(CURRENT_TERM)
        VOTED_FOR = my_id
        send(json_message)


def initiate_election():
    global NODE_STATE
    get_new_election_timeout()
    NODE_STATE = "C"

    send_vote_request()


def receive_vote(message):

    request_vote_RPC(message.term, message.src, msg['lastLogIndex'], msg['lastLogTerm'])

def request_vote_RPC(term, candidateId, lastLogIndex, lastLogTerm):
    global CURRENT_TERM, VOTES_RECIEVED

    if term < CURRENT_TERM:
        return False # reply false
    else:
        VOTES_RECIEVED += 1


def change_to_leader():
    global NODE_STATE
    NODE_STATE = "L"




while True:
    print str(my_id) + ": Node State = " + str(NODE_STATE)



    ready = select.select([sock], [], [], 0.1)[0]

    if sock in ready:
        print str(my_id) + ": Ready"
        msg_raw = sock.recv(32768)

        if len(msg_raw) == 0:
            continue

        msg = json.loads(msg_raw)

        # if NODE_STATE == "L": - run leader directions
        #    - check self.timeout:
        #           if timedout: change NODE_STATE to "F", initiate election cycle
        #           else: continue (continue listening for messages from client, sending updates to followers)
        if NODE_STATE == "L":
            # For now, ignore get() and put() from clients
            if msg['type'] in ['get', 'put']:
                message = Message.create_message_from_json(msg)

                client_action(message)

        # if NODE_STATE == "C": - run candidate directions
        #    - check election_timeout:
        #           if election_timedut:...
        #           else: continue (listen for votes, if received enough votes, determine the leader, etc.)
        if NODE_STATE == "C":
            print str(my_id) + ": Message: " + str(msg)
            if msg['type'] =='vote':
                print str(my_id) + ": Got Vote-------------"

                message = Message.create_message_from_json(msg)
                receive_vote(message)
                if VOTES_RECIEVED >= QUORUM_SIZE:
                    change_to_leader()


        # if NODE_STATE == "F": - run follower directions
        #    - check leader_timeout:
        #           -if timedout, request election
        #           -else: continue (listen for messages from leader)
        if NODE_STATE == "F":
            if election_timedout():
                initiate_election()
            else:
                if msg['type'] == 'voteRequest':
                    vote_req_message = Message.create_message_from_json(msg)
                    send_vote(vote_req_message)


        # Handle noop messages. This may be removed from your final implementation
        elif msg['type'] == 'noop':
            print '%s received a NOOP from %s' % (msg['dst'], msg['src'])
    else:
        print str(my_id) + ": Not ready"

    '''
    clock = time.time()
    if clock-last > 2:
        # Send a no-op message to a random peer every two seconds, just for fun
        # You definitely want to remove this from your implementation
        msg = {'src': my_id, 'dst': random.choice(replica_ids), 'leader': 'FFFF', 'type': 'noop'}
        sock.send(json.dumps(msg))
        print '%s sending a NOOP to %s' % (msg['src'], msg['dst'])
        last = clock
    '''


