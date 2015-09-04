import imp
import argparse
import os
import gamesman
import collections

def main():
    parser = argparse.ArgumentParser(description='Locally solves games')
    parser.add_argument('game', help='The path to the game script to run.')
    arg = parser.parse_args()
    name = os.path.split(os.path.splitext(arg.game)[0])[-1]
    game = imp.load_source(name, arg.game)
    print(Solver(game).solve())

def get_children(game, pos):
    if game.primitive(pos) != gamesman.UNDECIDED:
        # The gamesman API says that generateMoves should never be called on
        # primitives.
        return tuple()
    else:
        return tuple([game.doMove(pos, m) for m in game.generateMoves(pos)])

class PositionData(collections.namedtuple('PositionDataBase',
                                          ['children',
                                           'parents',
                                           'unknown_count',
                                           'has_tie',
                                           'remoteness',
                                           'depth',
                                           'value'])):
    '''
    An immutable data structure recording all of the known information about a
    position.
    '''

    @staticmethod
    def from_children(children):
        '''
        Create a PositionData from a tuple of child positions.
        '''
        return PositionData(tuple(children), tuple(), len(children), False, -1, -1,
                            gamesman.UNDECIDED)

    @staticmethod
    def from_primitive(value, parents):
        '''
        Create a PositionData for a primitive position.
        '''
        return PositionData(tuple(), parents, 0, False, 0, -1, value)

    def a_child(self, new_child):
        '''
        Record the value of a solved child.
        '''
        # This function could use much more error handling.
        # It's possible to be passed the same child multiple times, for
        # example.
        tie = new_child.value == gamesman.TIE or self.has_tie
        win = new_child.value == gamesman.LOSE
        value = gamesman.WIN if win else self.value 
        return PositionData(self.children, self.parents,
                            self.unknown_count - 1, tie, -1, self.depth, value)

    def a_depth(self, depth):
        '''
        "Set" the depth.
        '''
        return PositionData(self.children, self.parents,
                            self.unknown_count, self.has_tie, self.remoteness,
                            self.depth, self.value)

    def a_parent(self, parent, parent_data):
        '''
        Record the existence of a new parent.
        '''
        depth = self.depth
        if self.depth == -1 or parent_data.depth + 1 < self.depth:
            depth = parent_data.depth + 1
        return PositionData(self.children, self.parents + (parent,),
                            self.unknown_count, self.has_tie, -1, depth, self.value)

    def a_value(self, value):
        '''
        "Set" the value. Mostly used for making draws.
        '''
        return PositionData(self.children, self.parents,
                            0, self.has_tie, -1, self.depth, value)

    def ready(self):
        '''
        Is it safe to call finish?
        '''
        return self.unknown_count == 0

    def finish(self, position_data):
        '''
        Compute our value from our children.
        '''
        assert self.ready()
        best_value = gamesman.LOSE
        best_remoteness = -1
        queue = [position_data[child] for child in self.children]

        while best_value == gamesman.LOSE and queue:
            child = queue.pop()
            if child.value == gamesman.TIE or child.value == gamesman.DRAW:
                best_value = child.value
                best_remoteness = child.remoteness + 1
            elif child.value == gamesman.LOSE:
                best_value = gamesman.WIN
                best_remoteness = child.remoteness + 1
            elif child.value == gamesman.WIN:
                best_remoteness = max(best_remoteness, child.remoteness + 1)
            else:
                print('Child not finished:', child)
                assert False

        while (best_value == gamesman.TIE or best_value == gamesman.DRAW) and queue:
            child = queue.pop()
            if child.value == gamesman.TIE or child.value == gamesman.DRAW:
                # TODO(zentner): Should this be a max or a min?
                best_remoteness = max(best_remoteness, child.remoteness + 1)
            elif child.value == gamesman.LOSE:
                best_value = gamesman.WIN
                best_remoteness = child.remoteness + 1
            elif child.value == gamesman.WIN:
                pass
            else:
                assert false
        
        for child in queue:
            if child.value == gamesman.LOSE:
                best_remoteness = min(best_remoteness, child.remoteness + 1)

        return PositionData(self.children, self.parents, 0, self.has_tie,
                            best_remoteness, self.depth, best_value)


class Solver(object):
    '''
    Creates a graph of the game, and solves for the value of all vertices.
    '''

    def __init__(self, game):
        self.game = game
        self.position_data = dict()
        self.store_graph_into_data()

    def store_graph_into_data(self):
        # Contains position, parent pairs ("edges").
        queue = [(self.game.initialPosition, None)]

        while queue:
            position, parent = queue.pop()

            if position not in self.position_data:
                # Add a new position to the graph.
                children = get_children(self.game, position)
                data = PositionData.from_children(children)
                if parent is None:
                    # We're the root. Set our depth to 0.
                    # This needs to happen before anyone passes the root to
                    # a_parent.
                    data = data.a_depth(0)
                else:
                    # We have a parent. Record them.
                    data = data.a_parent(parent, self.position_data[parent])
                self.position_data[position] = data
                for child in children:
                    queue.append((child, position))
            else:
                # Record our new parent.
                data = self.position_data[position]
                data = data.a_parent(parent, self.position_data[parent])
                self.position_data[position] = data

    def print_graph(self, root=None, indent='', visited=None):
        if visited is None:
            visited = set()
        if root is None:
            root = self.game.initialPosition
        if root not in visited:
            print('{}begin: {}'.format(indent, root))
            visited.add(root)
            for child in self.position_data[root].children:
                self.print_graph(child, indent + '  ', visited)
            print('{}end: {}'.format(indent, root))
        else:
            print('{}refer: {}'.format(indent, root))

    def solve(self):
        frontier = []
        total_positions = len(self.position_data)
        positions_left = total_positions

        # Find all the primitives
        for position, data in self.position_data.items():
            value = self.game.primitive(position)
            if value != gamesman.UNDECIDED:
                data = PositionData.from_primitive(value, data.parents)
                self.position_data[position] = data
                frontier.append((position, data))

        # Propagate up
        while positions_left:

            # Positions potentially in draw loops.
            frontier_candidates = []
            # Depth of all candidates above.
            max_depth = -1

            while frontier:
                position, data = frontier.pop()
                assert data.ready()

                # Do the actual processing of the step.
                data = data.finish(self.position_data)
                self.position_data[position] = data
                positions_left -= 1

                for parent in data.parents:

                    # Record the child into the parent's data.
                    parent_data = self.position_data[parent]
                    parent_data = parent_data.a_child(data)
                    self.position_data[parent] = parent_data

                    if parent_data.ready():
                        # We can now compute our parent, add them to the
                        # frontier.
                        frontier.append((parent, parent_data))
                        # And we no longer need them in the candidates.
                        if parent in frontier_candidates:
                            frontier_candidates.remove(parent)
                    else:
                        # We could have draws here.
                        if parent_data.depth > max_depth:
                            max_depth = parent_data.depth
                            frontier_candidates = [parent]
                        elif parent_data.depth == max_depth:
                            frontier_candidates.append(parent)

            # Our frontier is empty, mark all the candidates as draws and add
            # them to the frontier.
            for position in frontier_candidates:
                data = self.position_data[position].a_value(gamesman.DRAW)
                self.position_data[position] = data
                frontier.append((position, data))

        return self.position_data[self.game.initialPosition].value

if __name__ == '__main__':
    main()
