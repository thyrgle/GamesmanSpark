import imp
import argparse
import os
import gamesman

def main():
    parser = argparse.ArgumentParser(description='Locally solves games')
    parser.add_argument('game', help='The path to the game script to run.')
    arg = parser.parse_args()
    name = os.path.split(os.path.splitext(arg.game)[0])[-1]
    game = imp.load_source(name, arg.game)
    print(Solver(game).solve())

class Solver(object):

    def __init__(self, game):
        self.game = game
        self.cache = dict()

    def solve(self):
        return self._doSolve(self.game.initialPosition)

    def _doSolve(self, pos):
        if pos in self.cache:
            return self.cache[pos]
        res = self._solve(pos)
        self.cache[pos] = res
        return res

    def _solve(self, pos):
        game = self.game
        v = game.primitive(pos)
        if v is not gamesman.UNDECIDED:
            return v
        else:
            moves = game.generateMoves(pos)
            vals = [self._doSolve(game.doMove(pos, m)) for m in moves]
            if gamesman.LOSE in vals:
                return gamesman.WIN
            elif gamesman.TIE in vals:
                return gamesman.TIE
            else:
                return gamesman.LOSE

if __name__ == '__main__':
    main()
