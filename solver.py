from pyspark import SparkContext
import state
from state import State

UNDECIDED, LOSS, TIE, WIN = range(4)

def solve(do_move, get_state, generate_moves, init_position):
    sc = SparkContext("local", "GamesmanSpark") #TODO: Move out of local

    value = sc.parallelize(())
    ready = sc.parallelize(())
    rev = sc.parallelize(())
    up = sc.parallelize(())
    unknowns = sc.parallelize([init_position])

    #Add remoteness to get_state
    get_state = value_map(get_state)

    while True:
        rev = rev.union(unknowns.flatMap(lambda p: [(do_move(p,m), p) for m in generate_moves(p)]))
        # unkonwns map (pos, prim) union with value -> partition by values
        value = value.union(unknowns.flatMap(lambda pos: (pos, get_state(pos))))
        ready = value.filter(lambda partition: partition[0] == UNDECIDED)
        up = up.union(unknowns.flatMap(lambda p: [(c, p) for c in generate_moves(p)]).map(do_move))
        value = value.union(up)

    value.saveAsTextFile("value")
    
def value_map(get_state):
    def wrapper(pos):
        s = get_state(pos)
        return s, -1 if s == UNDECIDED else 0
    return wrapper

def main():
    ret_false = lambda: -1
    test_gen = lambda x: ['2', '2']
    do_move = lambda x, y: x
    solve(do_move, ret_false, test_gen, '3',)

if __name__ == '__main__':
    main()
