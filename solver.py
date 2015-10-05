from pyspark import SparkContext
from operator import itemgetter

LOSS, TIE, WIN, UNDECIDED = range(4)

def solve(do_move, get_state, generate_moves, init_position):
    sc = SparkContext("local", "GamesmanSpark") #TODO: Move out of local

    unknowns = sc.parallelize([init_position])
    resolved = sc.parallelize(())
    up       = sc.parallelize(())
    locality = 0

    while not unknowns.isEmpty():
        #Create a list of (parent, (child, child_result)) tuples.
        #Where parent is the current game state in question,
        #child is a particular generated move from the parent,
        #and child_result is the current game state of the child:
        #win, loss, tie, draw, or unknown.
        children = unknowns.flatMap(lambda p: [((p, locality), (m, get_state(m))) for m in generate_moves(p)])

        #We wish to construct a tree of all known states to solve the game.
        #At this moment, filter out the states which are primitive and add
        #those to the tree since they are already known.
        first_pass_resolve = children.filter(lambda group: group[1][1] != UNDECIDED)

        #Now construct a mapping so that first_pass_resolve type changes
        #from [(parent, locality), (m, get_state(m))] -> ((m, locality), get_state(m))
        first_pass_resolve = first_pass_resolve.map(lambda group: ((group[1][0], group[0][1]+1), group[1][1]))
        resolved = resolved.union(first_pass_resolve)

        #Now that we have these lists, group everything by parent nodes.
        #This will create a structure as follows:
        #[((parent1, locality) [(child, state), (child, state) ... (child, state)]),
        #  (parent2 locality), [(child, state), (child, state) ... (child, state)])
        #   .
        #   .
        #   .
        #  (parentn, locality), [(child, state), (child, state) ... (child, state)])]
        up = children.groupByKey()

        #We may be able to determine a win or loss at this point.
        #To elaborate, consider the following:
        #Consider we have a game that has either WIN, or LOSS. No TIES or
        #DRAWS. Then we can consider WIN as True, and LOSS as False and 
        #UNKNOWN (as in a child has not been fully explored) as Maybe.
        #Now consider a parent that has no UNKNOWN children, i.e. it is
        #of the form:
        #(parent_i, [WIN, LOSS, ... WIN])
        #The parent will clearly chose to go down the winning path. In
        #otherwords we can "reduce the children" using the logical or
        #operator.
        #In this case:
        #(parent, [WIN, LOSS, ..., WIN]) -> WIN or LOSS or ... or WIN
        # -> WIN.
        #Now consider generalizing this to a series of series of finite
        #series of states > 3. In this case we must take into account
        #TIE and DRAW as well. As it can be shown, the max(x,y) is a
        #natural generalization of the or operator. (Proof is left as
        #an exercise to the reader.) Therefore, we can use it determine WIN, 
        #LOSS, as well as DRAW and TIE.
        #Now, we can return a list of parents that are now resolved and store
        #them in an RDD.
        #One last note: We must make UNKNOWN the maximum value of all values
        #given. (Do you see why?)
        child_max = lambda x: max(x, key=itemgetter(1))[1]
        #freshly_resolved may contain invalid pairings. Consider them "UNKNOWN
        #resolved" pairings.
        freshly_resolved = up.map(lambda group: (group[0], child_max(group[1])))
        #We filter those out and add them to resolved the "completely resolved"
        #to the resolved RDD.
        resolved = resolved.union(freshly_resolved.filter(lambda pairing: pairing[1] < UNDECIDED))
        #Add the pairings that are still unknown
        unknowns = freshly_resolved.filter(lambda pairing: pairing[1] == UNDECIDED).map(lambda pair: pair[0])
        locality += 1

        #Repeat until there are no unknowns.
    resolved = resolved.sortByKey(True, None, lambda pair: pair[1])
    resolved.saveAsTextFile("value")
    
def main():
    ret_false = lambda x: 0
    test_gen = lambda x: ['2', '1']
    do_move = lambda x, y: x
    solve(do_move, ret_false, test_gen, '3',)

if __name__ == '__main__':
    main()
