from pyspark import SparkContext
from pyspark import RDD

class Node:
    def __init__(self, state, parents, children):
        self.state = state
        self.parents = parents
        self.children = children

class SparkSolver:
    def __init__(self, generate_moves, primitive, state=None):
        self.sc = SparkContext("local", "SparkSolver")
        self.generate_moves = generate_moves
        self.primitive = primitive
        self.state = state
        self.queue = []

    def generate_graph(self):
        """
        Returns level, gamestate, [(parent, move)]
        """
        self.queue.extend(self.generate_moves(self.state))
        children = self.sc.parallelize(self.queue) \
                          .flatMap(self.generate_moves) \
                          .map(self.primitive).collect()
        self.queue = children

    def upward_traversal(self):
        """
        TODO: think of better name.
        """
        pass

    def save_to_db(self):
        pass

    def solve(self):
        pass

def main():
    pass

if __name__ == '__main__':
    main()
