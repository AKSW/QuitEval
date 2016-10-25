#!/usr/bin/env python3

import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str)
    parser.add_argument('output', type=str)

    args = parser.parse_args()

    infile = open(args.input, 'r')
    outfile = open(args.output, 'w')

    count = 0
    newlines = []
    for line in list(infile):
        newlines.append(line)
        if (line.strip() == "#__SEP__"):
            print ("separate after", count, "lines")
            if (count > 289):
                print ("cut at", 289, "lines")
                outfile.writelines(newlines[:289])
                outfile.writelines(newlines[-2:])
            else:
                outfile.writelines(newlines)
            count = 0
            newlines = []
        elif line.strip() == "":
            pass
        else:
            count += 1;
    infile.close()
    outfile.close()
