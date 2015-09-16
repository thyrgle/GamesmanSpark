WIN = 1
TIE = 0
LOSS = -1
UNKNOWN = -2

class State:
    def get_resolution(self):
        return eval("("+self.resolution+")()")

    def __init__(self, rep, resolution):
        self.rep = rep
        self.resolution = resolution
