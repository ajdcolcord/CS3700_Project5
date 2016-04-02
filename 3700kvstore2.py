#!/usr/bin/env python

import sys, socket, select, time, json, random, datetime
from message import Message
from server import Server

my_id = sys.argv[1]

replica_ids = sys.argv[2:]

SERVER = Server(my_id, replica_ids)


while True:
    print str(SERVER.id) + ": Node State = " + str(SERVER.node_state)

    ready = select.select([SERVER.sock], [], [], 0.1)[0]

    if SERVER.sock in ready:
        print str(SERVER.id) + ": Ready"
        msg_raw = SERVER.sock.recv(32768)

        if len(msg_raw) == 0:
            continue

        msg = json.loads(msg_raw)

        # if NODE_STATE == "L": - run leader directions
        #    - check self.timeout:
        #           if timedout: change NODE_STATE to "F", initiate election cycle
        #           else: continue (continue listening for messages from client, sending updates to followers)
        if SERVER.node_state == "L":
            # For now, ignore get() and put() from clients
            if msg['type'] in ['get', 'put']:
                message = Message.create_message_from_json(msg)

                SERVER.client_action(message)

        # if NODE_STATE == "C": - run candidate directions
        #    - check election_timeout:
        #           if election_timedut:...
        #           else: continue (listen for votes, if received enough votes, determine the leader, etc.)
        if SERVER.node_state == "C":
            print str(SERVER.id) + ": Message: " + str(msg)
            if msg['type'] =='vote':
                print str(SERVER.id) + ": Got Vote-------------"

                message = Message.create_message_from_json(msg)
                SERVER.receive_vote(message)
                if SERVER.votes_recieved >= SERVER.quorum_size:
                    SERVER.change_to_leader()


        # if NODE_STATE == "F": - run follower directions
        #    - check leader_timeout:
        #           -if timedout, request election
        #           -else: continue (listen for messages from leader)
        if SERVER.node_state == "F":
            if SERVER.election_timedout():
                SERVER.initiate_election()
            else:
                if msg['type'] == 'voteRequest':
                    vote_req_message = Message.create_message_from_json(msg)
                    SERVER.send_vote(vote_req_message)


        # Handle noop messages. This may be removed from your final implementation
        elif msg['type'] == 'noop':
            print '%s received a NOOP from %s' % (msg['dst'], msg['src'])
    else:
        print str(SERVER.id) + ": Not ready"

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



