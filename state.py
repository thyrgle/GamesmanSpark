import uuid

class State:
    def is_primitive(self):
        return eval("("+self.primitive+")()")

    def get_id(self):
        return self.uuid

    def __init__(self, representation, primitive):
        self.representation = representation
        self.primitive = primitive
        self.uuid = uuid.uuid1()
