#!/usr/bin/python -u

# Austin Colcord and Nick Scheuring

import sys, socket, select, time, json, random, datetime
from message import Message
from Server import Server

my_id = sys.argv[1]

replica_ids = sys.argv[2:]
SERVER = Server(my_id, replica_ids)

time.sleep(0.2)
SERVER.get_new_election_timeout()

while True:

    if SERVER.node_state in ["F", "C"]:
        if SERVER.election_timedout():
            SERVER.initiate_election()

    if SERVER.node_state == "L":
        if SERVER.last_applied == SERVER.commit_index or (SERVER.last_applied == -1 and SERVER.commit_index == 0):
            SERVER.pull_from_queue()
            if SERVER.last_applied < len(SERVER.log) - 1:
                SERVER.send_append_entries()
        if SERVER.heart_beat_timedout():
            SERVER.send_append_entries()

    ready = select.select([SERVER.sock], [], [], 0.01)[0]

    if SERVER.sock in ready:
        msg_raw = SERVER.sock.recv(32768)

        if len(msg_raw) == 0:
           continue

        msg = json.loads(msg_raw)

        SERVER.all_receive_message(msg)

        if SERVER.node_state == "L":
            SERVER.leader_receive_message(msg)

        if SERVER.node_state == "C":
            SERVER.candidate_receive_message(msg)

        if SERVER.node_state == "F":
            SERVER.follower_receive_message(msg)
