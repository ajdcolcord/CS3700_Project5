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

    @staticmethod
    def create_message_from_json(json):
        try:
            #print "JSON MESSAGE: " + str(json)
            #print "EACH FIELD: " + str(json['src']) + str(json['dst']) + str(json['leader']) + str(json['type']) + str(json['MID'])
            newMessage = Message(json['src'], json['dst'], json['leader'], json['type'], json['MID'])
            #print newMessage


            if json.get('key'):
                print 'adding key'
                newMessage.add_key(json['key'])
                print "added key: " + str(newMessage.key)

            if json.get('value'):
                print 'adding value'
                newMessage.add_value(json['value'])
                print "added value: " + str(newMessage.value)

            if json.get('term'):
                print 'adding term'
                newMessage.term = json['term']


            return newMessage
        except:
            raise Exception("Malformed message: " + str(json))

    # @staticmethod
    # def create_vote_message_from_json(json):
    #     try:
    #         newMessage = Message(json['src'], json['dst'], json['leader'], json['type'], json['MID'])
    #         if json['key']:
    #             newMessage.add_key(json['key'])
    #             print "added key: " + str(newMessage.key)
    #         if json.get('value'):
    #             newMessage.add_value(json['value'])
    #             print "added value: " + str(newMessage.value)
    #
    #         return newMessage
    #     except:
    #         raise Exception("Malformed message: " + str(json))

    def create_response_message(self, type):
        return Message(self.dst, self.src, self.leader, type, self.message_id, term=self.term)

    def add_key(self, key):
        self.key = key

    def add_value(self, value):
        self.value = value

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

    def create_vote_message(self, src):
        message = self.create_response_message('vote')
        return {'src': src, 'dst': message.dst, 'leader': message.leader,
                'type': 'vote', 'MID': message.message_id, 'term': message.term}

    def create_vote_request_message(self, src, term):
        return {'src': src, 'dst': "FFFF", 'leader': "none",
                'type': "voteRequest", 'MID': 1234567890, 'term': term}

    @staticmethod
    def create_heart_beat_message(src, term):
        return {'src': src, 'dst': "FFFF", 'leader': src,
                'type': 'heartbeat', 'MID': 1234567890, 'term': term}


