#!/bin/bash

javac src/br/ufrj/ppgi/huffmanyarnmultithreadv2/*.java src/br/ufrj/ppgi/huffmanyarnmultithreadv2/encoder/*.java src/br/ufrj/ppgi/huffmanyarnmultithreadv2/decoder/*.java -d bin

ant
