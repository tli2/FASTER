import os, sys

def concat_file(filename, files):
    output = open(filename + '.txt', 'w')
    for file in files:
        f = open(filename + str(file) + '.txt')
        output.writelines(f.readlines())
        f.close()
    output.close()
