#!/usr/bin/env python
# encoding: utf-8

import unittest
from solver import SparkSolver

class BasicGame:
    """This basic game generates a list of 4 2s
       and then the game ends"""
    def primitive(self, state):
        if state == 2:
            return True
        else:
            False

    def generate_moves(self, state):
        return [2, 2, 2, 2]

    def do_move(self, state):
        pass

class GSparkTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

# This would go in tests/project_test.py
class BasicSparkTests(GSparkTestCase):
    def test_arithmetic(self):
        assert 2+2 == 4
    def test_basic_game_spark(self):
        bg = BasicGame()
        test_solver = SparkSolver(bg.generate_moves, bg.primitive)
        test_solver.generate_graph()
        assert test_solver.queue == [True] * 16

if __name__ == '__main__':
    unittest.main()
