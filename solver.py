from pyspark import SparkContext
import state
from state import State


class SparkSolver:
    def __init__(self, generate_moves, st):
        self.sc = SparkContext("local", "SparkSolver")
        self.generate_moves = self.graph(generate_moves)
        self.state = st
        self.queue = self.sc.parallelize([st])

    def graph(self, gen_moves):
        """ 
        Add game nodes to internal structure using this decorator
        """
        def func_wrapper(state):
            moves = gen_moves(state)
            return [(state, move) for move in moves]
        return func_wrapper

    def generate_graph(self):
        """
        Returns level, gamestate, [(parent, move)]
        """
        not_primitive = lambda x: x.get_resolution() == state.UNDECIDED
        edges = self.sc.parallelize([])
        while not self.queue.isEmpty():
            new_edges = self.queue.flatMap(self.generate_moves)
            edges.union(new_edges)
            children = new_edges.map(lambda e: e[1])
            self.queue = children.filter(not_primitive)
        edges.saveAsTextFile('edges')

def main():
    ret_false = "lambda: -1"
    test_gen = lambda x: [State('2', ret_false), State('2', ret_false)]
    solver = SparkSolver(test_gen, State('3', ret_false))
    solver.generate_graph()

if __name__ == '__main__':
    main()
