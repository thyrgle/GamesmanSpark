import gamesman as G

initialPosition = 0

def primitive(position):
    if position == 10:
        return G.LOSE
    else:
        return G.UNDECIDED

def generateMoves(position):
    if position == 9:
        return [1]
    else:
        return [1, 2]

def doMove(position, move):
    return position + move

def solve(position=initialPosition):
    if primitive(position) != G.UNDECIDED:
        return primitive(position)
    else:
        moves = generateMoves(position)
        positions = [doMove(position, move) for move in moves]
        values = [solve(pos) for pos in positions]
        if G.LOSE in values:
            return G.WIN
        elif G.TIE in values:
            return G.TIE
        else:
            return G.LOSE
