from pyspark import SparkContext
import csv
import state
from state import State

GET_STATE = {state.UNKNOWN : "unknown", 
             state.LOSS : "loss",
             state.TIE : "tie",
             state.WIN : "win"}

def graph(gen_moves):
    """ 
    Add game nodes to internal structure using this decorator
    """
    def func_wrapper(state):
        moves = gen_moves(state)
        with open('results.csv', 'w') as out:
            csv_out = csv.writer(out)
            for move in moves:
                csv_out.writerow((state.rep, move.rep, GET_STATE[move.get_resolution()]))
        return moves
    return func_wrapper

class SparkSolver:
    def __init__(self, generate_moves, state):
        self.sc = SparkContext("local", "SparkSolver")
        self.generate_moves = generate_moves
        self.state = state
        self.queue = [state]

    def generate_graph(self):
        """
        Returns level, gamestate, [(parent, move)]
        """
        not_primitive = lambda x: x.get_resolution() == state.UNKNOWN
        while self.queue:
            children = self.sc.parallelize(self.queue) \
                              .flatMap(self.generate_moves)
            self.queue = children.filter(not_primitive).collect()

def main():
    ret_false = "lambda: -1"
    test_gen = graph(lambda x: [State('2', ret_false), State('2', ret_false)])
    solver = SparkSolver(test_gen, State('3', ret_false))
    solver.generate_graph()

if __name__ == '__main__':
    main()
