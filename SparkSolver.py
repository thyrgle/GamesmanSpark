from pyspark import SparkContext
#from TicTacToe.ttt import initiateBoard, generateMove, isPrimitive, tie, isEmpty
import sys
import importlib

"""
Needed:
    -initiateBoard(width, height)
    -generateMove(pos, width, height)
    -isPrimitive(pos, width, height)
    -tie(pos, width, height)
    -isEmpty(pos, width, height)
"""

def opposite(letter):
    """
    Allows us to switch the game state of a board.
    Of course a tie or draw flipped would just be itself.
    """
    if letter == 'w':
        return 'l'
    if letter == 'l':
        return 'w'
    return letter


def traceBackUpMap(value):
    """
    Assigning all the wins/loses
    value = (boardState, (depthAway, gameState, remoteness, (children))
    """
    retVal = []
    #If we are not at the correct boardlevel, or we have one
    #of the conflicted mappings, move past and just append retval with
    #current value for next go around. This allows all the elements in the rdd
    #to continue being in the rdd.
    if type(value[1][1]) is str and boardLevel == value[1][0]:
        if not currMod.isEmpty(value[0], width, height):
            parents = value[1][3]

            retVal.append(value)

            #Go through all the parents and make sure we store them into our rdd
            #then assign the opposit game state to the board.
            for parent in parents:
                #Give decreasing boardLevel so we can ensure that we continue having the rdd
                #cycling through map/reduce until we have no more boards.
                boardInformation = [boardLevel - 1, opposite(value[1][1]), value[1][2] + 1, ()]
                #Tuple-fying everyingthing because we have to have immutable data structures
                #for spark
                parentTuple = [parent, tuple(boardInformation)]
                retVal.append(tuple(parentTuple))
    else:
        retVal.append(value)
    #Returning list because Spark doesn't have an emit keyword.
    return retVal


#Spark gives us the values of keys that repeat
#For instance, if we have (5, ('a')) and (5, ('b')), the reduce stage would receive
#either ('a') for value1 or value2 and ('b') for the other.
#So we have to ensure we do associative and commutative operations.
def traceBackUpReduce(value1, value2):
    """
    Possible merging of our values going down and values coming up
    Conflict between:
        possibleValue1: (depthAway, gameState, remoteness, (children)) <--- our new design
        possibleValue2: (boardLevel, (Children)) <--- our old design on the way to get primitives
    """
    #Checks to see if value1 or value2 is a value pair from when we were mapping downards
    #to get the primitives.

    #value[1] is either gameState or (Children) if you look at the comments above for
    #possibleValue1/2

    if isinstance(value1[1], tuple) and isinstance(value2[1], str):
        parentLst = []

        #Add the children of one value to a communal child list
        value1List = value1[1]
        for val in value1List:
            parentLst.append(val)

        #Add the children of the other value to the communal child list
        value2List = value2[3]
        for val in value2List:
            parentLst.append(val)

        #Returning a value that coincides with the new design when we traverse up
        #We keep value1's boardLevel to keep the original boardLevel of the board state
        #We take the game state from value2 since it is the new design
        #We take the remoteness from value2 since it is the new design
        #We include the parentLst as well.
        tempTuple = (value1[0], value2[1], value2[2], tuple(parentLst))
        return tempTuple
    elif isinstance(value2[1], tuple) and isinstance(value1[1], str):
        parentLst = []

        #Add the children of one value to a communal child list
        value2List = value2[1]
        for val in value2List:
            parentLst.append(val)

        #Add the children of the other value to the communal child list
        value1List = value1[3]
        for val in value1List:
            parentLst.append(val)

        #Returning a value that coincides with the new design when we traverse up
        #We keep value2's boardLevel to keep the original boardLevel of the board state
        #We take the game state from value1 since it is the new design
        #We take the remoteness from value1 since it is the new design
        #We include the parentLst as well.
        tempTuple = (value2[0], value1[1], value1[2], tuple(parentLst))
        return tempTuple
    #This is when we are only dealing with both values being apart of the 'new' design
    #when traversing upwards
    else:
        parentLst = []

        #Add the children of one value to a communal child list
        value1List = value1[3]
        for val in value1List:
            parentLst.append(val)

        #Add the children of the other value to the communal child list
        value2List = value2[3]
        for val in value2List:
            parentLst.append(val)

        #We have to keep track of the board state that we are going to assign to the key
        #and keep track of which remoteness we would want
        boardState = ''
        remote = -1

        #Game state from both boards
        gameState2 = value2[1]
        gameState1 = value1[1]

        remoteness1 = value1[2]
        remoteness2 = value2[2]

        #If one of the gamestates is a 'w', then we know the boardstate will automatically
        #be a win. We just have to worry about remoteness at that point
        #WE DON'T TAKE INTO ACCOUNT OF DRAWS YET. AT THE TIME OF CREATION WE DIDN'T WORRY
        #ABOUT GAMES WITH DRAWS
        if gameState2 == 'w':

            #If both gamestates are wins then we want the minimum route to win
            if gameState1 == 'w':
                remote = min(remoteness2, remoteness1)
            else:
                remote = remoteness2
            boardState = 'w'
        elif gameState1 == 'w':
            boardState = 'w'
            remote = remoteness1
        #If the gamestates aren't a win, but we have one that is a tie, we will treat that
        #with higher precedence
        elif gameState2 == 't':

            #If both are ties, then we want to end the game the fastest
            if gameState1 == 't':
                remote = min(remoteness2, remoteness1)
            else:
                remote = remoteness2
            boardState = 't'
        elif gameState1 == 't':
            boardState = 't'
            remote = remoteness1
        #Finally, we know that our gamestates are both losses
        else:
            boardState = 'l'
            if remoteness1 > remoteness2:
                remote = remoteness1
            else:
                remote = remoteness2

        #We choose whichever boardLevel is greatest (not sure why) and the rest has been
        #explained
        tempTuple = (max(value1[0], value2[0]), boardState, remote, tuple(parentLst))
        return tempTuple


def bfsMap(value):
    """
    Getting all the primitives
    value = (boardState, (depthAway, (children)))
    """
    retVal = []
    #Check to make sure that we only find the childrens of relevant gameBoards
    #boardLevel helps as a way to filter what tier we are on
    if value[1][0] == boardLevel:
        #If we are at a primitive board, we should just append that to our list instead
        #of find their children
        if not currMod.isPrimitive(value[0], width, height):
            #Get a list of children
            children = currMod.generateMove(value[0], width, height)

            #Cycle through children, assigning the new boardLevel (next tier), and gives
            #them the parents list of their parents
            for child in children:
                parentTuple = [boardLevel + 1, tuple( [tuple(value[0])] )]
                childTuple = [tuple(child), tuple(parentTuple)]

                retVal.append(tuple(childTuple))

            retVal.append(value)
        else:
            #Getting all the necessary information to create the new format of our values
            #So we can differentiate between the values and know which value is what in
            #TraceBackUpReduce
            boardState = value[0]
            boardLev = value[1][0]
            gameState = currMod.tie(value[0], width, height)
            remotenessAway = 0
            allChildren = value[1][1]
            #Creating the new design of our primitives for when we map back up
            tempTuple = tuple([boardState, tuple([boardLev, gameState, remotenessAway, \
                tuple(allChildren)])])

            retVal.append(tempTuple)
    else:
        retVal.append(value)

    return retVal

def bfsReduce(value1, value2):
    """
    possibleValue1: (depthAway, gameState, remoteness, (parents)) <--Only when we hit a primitive
    possibleValue2: (boardLevel, (parents)) <--Typical on mapping down to primitives
    """

    #Checks to see if value1 or value2 is a value pair for a primitve
    #value[1] is either gameState or (Children) if you look at the comments above for
    #possibleValue1/2
    if isinstance(value1[1], tuple) and isinstance(value2[1], tuple):
        allParents = []

        #Cycle through both parentLists and store in a communal list
        for eachParent in value1[1]:
            allParents.append(tuple(eachParent))

        for eachParent in value2[1]:
            allParents.append(tuple(eachParent))

        #Return the value pair similar to the structure of
        #possibleValue2
        #Want the minimum most boardLevel away
        return (min(value1[0], value2[0]), tuple(allParents))

    #If we encounter a value having a primitive design structure then we have to cover
    #Those edge cases
    else:
        if isinstance(value1[1], tuple):
            allParents = []

            #Cycle through the parents of both structures and add to a communal list
            for eachParent in value1[1]:
                allParents.append(tuple(eachParent))
            for eachParent in value2[3]:
                allParents.append(tuple(eachParent))
            
            #Format the new value to the primitive design specification
            return (value2[0], value2[1], value2[2], tuple(allParents))
        elif isinstance(value2[0], tuple):
            allParents = []

            #Cycle through the parents of both structures and add to a communal list
            for eachParent in value2[1]:
                allParents.append(tuple(eachParent))
            for eachParent in value1[3]:
                allParents.append(tuple(eachParent))

            #Format the new value to the primitive design specification
            return (value1[0], value1[1], value1[2], tuple(allParents))
        else:
            #This is in case there was something that was not thought of in our if/else
            #conditionals
            return ("blank", -1, ())

def traceBackUpMapList(value):
    """
    UNDERCONSTRUCTION! ~Shouldn't be using remoteness as the conditional filter
        -Should be using boardLevel. Look at traceBackUpMap


    Assigning all the wins/loses via undoMove <-- function that is useful for some games
    value = (boardState, (w/t/l, remoteValue))
    """
    retVal = []
    #If we are not at the correct remoteness level, or we have one
    #of the conflicted mapping, move past and just append retval with
    #current value for next go around
    if remoteness == value[1][1]:
        if not currMod.isEmpty(value[0], width, height):
            parents = currMod.undoMoveList(value[0], width, height)

            retVal.append(value)

            for parent in parents:
                #We flip the game state and choose our parent's state based
                #on these values
                if not currMod.isPrimitive(parent, width, height):
                    boardInformation = [opposite(value[1][0]), remoteness + 1, ]
                    parentTuple = [tuple(parent), tuple(boardInformation)]

                    retVal.append(tuple(parentTuple))
    else:
        retVal.append(value)

    return retVal


def traceBackUpReduceList(value1, value2):
    """
    SHOULD WORK. No promises though. Wasn't extensively tested.

    Possible merging of our values going down and values coming up
    Conflict between:
        possibleValue1: (w/t/l, remoteValue)
    """
    #Checks to see if one of the values is the information
    #from our mapping down
    # if remoteness == value1[1] and remoteness == value2[1]:
    boardState = ''
    remote = 1000

    gamePosition2 = value2[0]
    gamePosition1 = value1[0]

    remoteness1 = value1[1]
    remoteness2 = value2[1]

    if gamePosition2 == 'w':
        if gamePosition1 == 'w':
            if remoteness1 > remoteness2:
                remote = remoteness2
            else:
                remote = remoteness1
        else:
            remote = remoteness2
        boardState = 'w'
    elif gamePosition1 == 'w':
        boardState = 'w'
        remote = remoteness1
    elif gamePosition2 == 't':
        if gamePosition1 == 't':
            if remoteness1 > remoteness2:
                remote = remoteness2
            else:
                remote = remoteness1
        else:
            remote = remoteness2
        boardState = 't'
    elif gamePosition1 == 't':
        boardState = 't'
        remote = remoteness1
    else:
        boardState = 'l'
        if remoteness1 > remoteness2:
            remote = remoteness2
        else:
            remote = remoteness1

    tempTuple = (boardState, remote)
    return tempTuple

def bfsMapList(value):
    """
    Should work just fine.

    value = (board state, boardlevel)
    """
    retVal = []
    if value[1] == boardLevel:
        if not currMod.isPrimitive(value[0], width, height):
            children = currMod.generateMove(value[0], width, height)

            for child in children:
                childTuple = [tuple(child), boardLevel + 1]
                retVal.append(tuple(childTuple))
        else:
            tempTuple = tuple([value[0], tuple([currMod.tie(value[0], width, height), 0])])
            retVal.append(tempTuple)
    elif currMod.isPrimitive(value[0]):
        retVal.append(value)
    return retVal

def bfsReduceList(value1, value2):
    #All we care about is the minimum boardLevel, but that will change if we get the new
    #primitive design.
    #FORGOT TO TAKE INTO ACCOUNT THE PRIMITIVE DESIGN STRUCTURES
    return min(value1, value2)


def filteringPrimitives(value):
    """Function used to write all the primitives to a certain file"""
    return currMod.isPrimitive(value[0], width, height)

def printBFSFunction(rdd, fName):
    """
    Prints an rdd for us. Not entirely sure how this works.
    Typically used after the first map/reduce down to the primitives
    """
    #Running through the list and formatting it as level and then board set
    outputFile = open(fName, "w")
    writer = lambda line: outputFile.write(str(line) + "\n")

    #rdd.collect() returns a list of all the elements in the rdd
    compiledArr = rdd.collect()

    #sorted sorts the list based on the lambda function
    compiledArr = sorted(compiledArr, key = lambda value: value[1][0])

    for elem in compiledArr:
        writer(elem)

def printBFSListFunction(rdd, fName):
    """Same as printBFSFunction except we focous on the gamestate"""
    #Running through the list and formatting it as level and then board set
    outputFile = open(fName, "w")
    writer = lambda line: outputFile.write(str(line) + "\n")
    compiledArr = rdd.collect()
    compiledArr = sorted(compiledArr, key = lambda value: value[1])

    for elem in compiledArr:
        writer(elem)

def printTraceBackFunction(rdd, fName):
    """Used after the entire solving process."""
    outputFile = open(fName, "w")
    writer = lambda line: outputFile.write(str(line) + "\n")
    compiledArr = rdd.collect()

    for elem in compiledArr:
        writer(elem)

def relevantSet(value):
    """ Use this to help determin when there are no new elements in our rdd """
    return value[1][0] == boardLevel

def relevantSetList(value):
    """Same idea as relevantSet but we take into account the design for the listRDDs """
    return value[1] == boardLevel

def main():
    """
    We need to pass into terminal the width, height, filename without .py, package name,
    local[#comps]
    """
    #Current board level (tier) in our bfs
    global boardLevel
    boardLevel = 0

    #Converting command-line argument into an int
    global width, height
    width = int(sys.argv[1])
    height = int(sys.argv[2])

    #Module of game we are playing
    global currMod
    currMod = importlib.import_module(sys.argv[3], package=sys.argv[4])

    #Remoteness to win, lose, tie, draw (Using as a tier to traverse up the list)
    global remoteness
    remoteness = 0

    #Number of local[#ofcomps]
    sc = SparkContext(sys.argv[5], "python")

    blankBoard = currMod.initiateBoard(width, height)

    rdd = [(tuple(blankBoard), (boardLevel, ()))]
    rdd = sc.parallelize(rdd)

    num = 1
    maxBoardLevel = 0

    while num:
        maxBoardLevel = boardLevel
        rdd = rdd.flatMap(bfsMap)
        rdd = rdd.reduceByKey(bfsReduce)
        num = rdd.filter(relevantSet).count()
        boardLevel += 1

    #Output after BFS downwards
    # printBFSFunction(rdd, "Results/" + sys.argv[4] + "FirstMappingOutput.txt")

    allPrimRDD = rdd.filter(filteringPrimitives)

    #Output of all the primitives
    printBFSFunction(allPrimRDD, "Results/" + sys.argv[4] + "Primitives.txt")

    num = 1
    boardLevel = maxBoardLevel - 1
    while num:
        rdd = rdd.flatMap(traceBackUpMap)
        rdd = rdd.reduceByKey(traceBackUpReduce)
        tempRDD = rdd.filter(relevantSet)
        num = tempRDD.count()
        boardLevel -= 1

    rdd = rdd.sortBy(lambda value: value[1][2])
    printTraceBackFunction(rdd, "Results/" + sys.argv[4] + "FinalMappingOutput.txt")


if __name__ == "__main__":
    main()
