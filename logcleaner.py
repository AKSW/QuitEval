#!/usr/bin/env python3

import sys
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i',
        '--input')
    parser.add_argument(
        '-o',
        '--output')
    parser.add_argument(
        '-s',
        '--selection')
    print(sys.argv[1:])
    args = parser.parse_args(sys.argv[1:])

    args.input
    args.output

    with open(args.input, "r") as input:
        with open(args.output, "w") as output:
            for line in input:
                lineFields = line.split(" ")
                selection = args.selection.split(",")
                writeFields = []
                for sel in selection:
                    value = lineFields[int(sel.strip())]
                    if (value == "NaN"):
                        break
                    writeFields.append(value)
                if (value != "NaN"):
                    output.write(" ".join(writeFields) + '\n')
