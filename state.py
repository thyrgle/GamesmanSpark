import csv

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
        with open('resolutions.csv', 'a') as out:
            writer = csv.writer(out)
            writer.writerow((self.rep, self.get_resolution()))
