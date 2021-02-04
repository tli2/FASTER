import os, sys

def concat_file(filename, files):
    output = open(filename + '.txt', 'w')
    for file in files:
        f = open(filename + str(file) + '.txt')
        output.writelines(f.readlines())
        f.close()
    output.close()

def aggregate_file(filename, files):
    output = open(filename + '.txt', 'w')
    data = [0 for i in range(180)]
    for file in files:
        f = open(filename + str(file) + '.txt')
        lines = f.readlines()
        for i in range(180):
            data[i] += float(lines[i])
        f.close()
    for line in data:
        output.write(str(line) + "\n")
    output.close()
