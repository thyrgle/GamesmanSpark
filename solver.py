from pyspark import SparkContext
from operator import itemgetter

LOSS, TIE, WIN, UNDECIDED = range(4)

def safe_min(x,y):
    if x == None:
        return y
    if y == None:
        return x
    return min(x, y)

def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def solve(get_state, generate_moves, init_position):
    sc = SparkContext("local", "GamesmanSpark") #TODO: Move out of local
    quiet_logs(sc)
    
    #Every new iteration 
    #only keeps the current children.
    frontier = sc.parallelize([init_position]).setName('frontier')

    #The "solutions" to the game state.
    #Of the form (position, result)
    resolved = sc.parallelize(()).setName('resolved')

    #Used for backtracking up the game tree.
    up       = sc.parallelize(()).setName('up')
    
    negation_lookup = [2, 1, 0, 3]
    negate = lambda state: negation_lookup[state]
    child_max = lambda x: negate(max(x, key=itemgetter(1))[1])

    while True:
        #Create a list of (parent, (child, child_result)) tuples.
        #Where parent is the current game state in question,
        #child is a particular generated move from the parent,
        #and child_result is the current game state of the child:
        #win, loss, tie, draw, or unknown.
        children = frontier.flatMap(lambda p: [(p, (m, get_state(m))) for m in generate_moves(p)])
        children.setName('children')
        #Get rid of None children.
        children = children.filter(lambda group: group[1][0] != None)

        #We wish to construct a tree of all known states to solve the game.
        #At this moment, filter out the states which are primitive and add
        #those to the tree since they are already known.
        #nodes -> [(child, state) ... ]
        nodes = children.map(lambda child: child[1]).distinct()
        nodes.setName('nodes')
        #Now get the nodes we can immediately resolve.
        first_pass_resolve = nodes.filter(lambda node: node[1] != UNDECIDED)
        first_pass_resolve.setName('first_pass_resolve')
        #Add these to resolved.
        resolved = resolved.union(first_pass_resolve)
        #Frontier is the newest unknown children.
        frontier = nodes.filter(lambda node: node[1] == UNDECIDED).map(lambda node: node[0])
        
        #Then map from (child, (parent, state)) -> (parent, (child,state))
        #Then group by key.

        #Now that we have these lists, group everything by parent nodes.
        #This will create a structure as follows:
        #[(parent1, [(child, state), (child, state) ... (child, state)]),
        # (parent2, [(child, state), (child, state) ... (child, state)])
        #   .
        #   .
        #   .
        # (parentn, [(child, state), (child, state) ... (child, state)])]
        up = up.union(children.groupByKey())
        # Take [(parent, [(child, state)]] -> (parent, (child, state))
        update = up.flatMapValues(lambda x: x)
        update.setName('update')
        #Create a RDD of (child, (parent, state)) from up. Then merge this
        #with resolved. 
        update = update.map(lambda group: (group[1][0], (group[0], group[1][1])))
        #(child, ((parent, update_state), resolved_state))
        update = update.leftOuterJoin(resolved).distinct()
        #In the case the state has no parent.
        update = update.filter(lambda group: group[1][0] != None)
        #Get most updated state. (child, ((parent, state), state)) -> (parent, (child, state))
        update = update.map(lambda g: (g[1][0][0], (g[0], safe_min(g[1][0][1], g[1][1]))))
        #Remove duplicate keys.
        update = update.reduceByKey(lambda x,y: (x[0], safe_min(x[1], y[1])))

        up = update.groupByKey()

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
        # -> ~WIN -> LOSS
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
        #freshly_resolved may contain invalid pairings. Consider them "UNKNOWN
        #resolved" pairings.
        freshly_resolved = up.map(lambda group: (group[0], child_max(group[1])))
        freshly_resolved.setName('freshly_resolved')
        #We filter those out and add them to resolved the "completely resolved"
        #to the resolved RDD.
        freshly_decided = freshly_resolved.filter(lambda pairing: pairing[1] != UNDECIDED)
        freshly_decided.setName('freshly_decided')
        resolved = resolved.union(freshly_decided)
        resolved = resolved.distinct()
        up = up.subtractByKey(resolved)
        #Repeat until there are no unknowns.
        if up.isEmpty():
            break
    resolved.coalesce(1, True).saveAsTextFile("value")

def game_state(x):
    if x == 0:
        return LOSS
    else:
        return UNDECIDED

def generate_moves(x):
    if x >= 2:
        return [x-1, x-2]
    if x == 1:
        return [x-1]
    return None

def main():
    solve(game_state, generate_moves, 6)

if __name__ == '__main__':
    main()
