WIN = "win"
TIE = "tie"
LOSE = "lose"
UNDECIDED = "undecided"

class State:
    def get_resolution(self):
        return eval("("+self.resolution+")()")

    def __init__(self, rep, resolution):
        self.rep = rep
        self.resolution = resolution
