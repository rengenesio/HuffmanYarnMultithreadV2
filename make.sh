#!/bin/bash

mkdir -p bin

javac src/br/ufrj/ppgi/huffmanyarnmultithreadv2/*.java src/br/ufrj/ppgi/huffmanyarnmultithreadv2/encoder/*.java src/br/ufrj/ppgi/huffmanyarnmultithreadv2/encoder/yarn/*.java src/br/ufrj/ppgi/huffmanyarnmultithreadv2/decoder/*.java src/br/ufrj/ppgi/huffmanyarnmultithreadv2/decoder/yarn/*.java src/br/ufrj/ppgi/huffmanyarnmultithreadv2/tdd/*.java -d bin

ant
