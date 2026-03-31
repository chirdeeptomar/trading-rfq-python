"""This is a simple example of a Bytewax dataflow that takes a stream of integers, adds one to each integer, and outputs the result."""

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

flow = Dataflow("Simple Dataflow")

inp = op.input("input", flow, TestingSource([1, 2, 3, 4, 5]))


def add_one(x):
    """Add one to the input value."""
    return x + 1


output = op.map("add one", inp, add_one)

op.inspect("output", output)

if __name__ == "__main__":
    run_main(flow)
