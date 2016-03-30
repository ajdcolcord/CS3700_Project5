class Message:

    def __init__(self, src, dst, leader, type, message_id = None, key=None, value=None):
        self.src = src
        self.dst = dst
        self.leader = leader
        self.type = type
        self.message_id = message_id
        self.key = key
        self.value = value

    @staticmethod
    def create_message_from_json(json):
        print "\n\nJSON = " + str(json) + "\n\n"

        try:
            newMessage = Message(json['src'], json['dst'], json['leader'], json['type'])
            print "NEW MESSAGE CREATED ###############################"
            if json['MID']:
                newMessage.message_id = json['MID']
            if json['key']:
                newMessage.key = json['key']
            if json['value']:
                newMessage.value = json['value']
            return newMessage
        except:
            print "\n\nBAD = " + str(json) + "\n\n"
            raise Exception("Malformed message")

    def create_response_message(self, type):
        Message(self.dst, self.src, self.leader, type, self.message_id)

    def add_key(self, key):
        self.key = key

    def add_value(self, value):
        self.value = value

    def create_ok_get_message(self, value):
        return self.create_response_message('ok').addValue(value)

    def create_ok_put_message(self):
        return self.create_response_message('ok')

    def create_fail_message(self):
        return self.create_response_message('fail').addValue("")