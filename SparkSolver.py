from pyspark import SparkContext

class SparkSolver:
    def __init__(self, do_move, generate_moves, primitive):
        self.sc = SparkContext("local", "SparkSolver")
        self.do_move = do_move
        self.generate_moves = generate_moves
        self.primitive = primitive

    def generate_graph(self):
        """
        Returns level, gamestate, [(parent, move)]
        """
        pass

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
