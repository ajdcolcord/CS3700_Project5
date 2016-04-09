#!/usr/bin/python -u

# Austin Colcord and Nick Scheuring

class Message:

    def __init__(self, src, dst, leader, type, message_id, key=None, value=None, term=None):
        self.src = src
        self.dst = dst
        self.leader = leader
        self.type = type
        self.message_id = message_id
        self.key = key
        self.value = value
        self.term = term
        self.logEntry = None

    @staticmethod
    def create_message_from_json(json):
        try:
            newMessage = Message(json['src'], json['dst'], json['leader'], json['type'], json['MID'])

            if json.get('key'):
                newMessage.key = json['key']

            if json.get('value'):
                newMessage.value = json['value']

            if json.get('term'):
                newMessage.term = json['term']

            if json.get('logEntry'):
                newMessage.logEntry = json.get('logEntry')

            return newMessage
        except:
            raise Exception("Malformed message: " + str(json))

    def create_response_message(self, type):
        return Message(self.dst, self.src, self.leader, type, self.message_id, term=self.term)

    def create_ok_get_message(self, value):
        print 'createOK GET'
        message = self.create_response_message('ok')
        return {'src': message.src, 'dst': message.dst, 'leader':message.leader,
                'type': message.type, 'MID': message.message_id, 'value': value}

    def create_ok_put_message(self):
        print 'createOK'
        message = self.create_response_message('ok')
        return {'src': message.src, 'dst': message.dst, 'leader': message.leader,
                'type': message.type, 'MID': message.message_id}

    def create_fail_message(self):
        message = self.create_response_message('fail')
        return {'src': message.src, 'dst': message.dst, 'leader': message.leader,
                'type': message.type, 'MID': message.message_id}

    def create_vote_message(self, src, followers_leader):
        message = self.create_response_message('vote')
        return {'src': src, 'dst': message.dst, 'leader': followers_leader,
                'type': 'vote', 'MID': message.message_id, 'term': message.term}

    def create_vote_request_message(self, src, term):
        return {'src': src, 'dst': "FFFF", 'leader': "FFFF",
                'type': "voteRequest", 'MID': 1234567890, 'term': term}

    @staticmethod
    def create_heart_beat_message(src, term, leader_last_applied):
        return {'src': src, 'dst': "FFFF", 'leader': src,
                'type': 'heartbeat', 'MID': 1234567890, 'term': term, 'leader_last_applied': leader_last_applied}

    @staticmethod
    def create_append_entry_message(src, dst, term, prevLogIndex, prevLogTerm, entries, leader_last_applied):
        return {'src': src,
                'dst': dst,
                'leader': src,
                'type': 'appendEntry',
                'MID': 1234567890,
                'term': term,
                'logEntry': {'prevLogIndex': prevLogIndex,
                             'prevLogTerm': prevLogTerm,
                             'entries': entries,
                             'leader_last_applied': leader_last_applied}}

    def create_heart_beat_ACK_message(self, replica_id, follow_commit_index):
        message = self.create_response_message('heartbeatACK')
        return {'src': replica_id, 'dst': message.dst, 'leader': message.dst,
                'type': 'heartbeatACK', 'MID': 1234567890, 'term': message.term, 'follower_commit_index': follow_commit_index}

    def create_redirect_message(self, leader_id):
        message = self.create_response_message('redirect')
        return {'src': message.src, 'dst': message.dst, 'leader': leader_id,
                'type': 'redirect', 'MID': message.message_id}



