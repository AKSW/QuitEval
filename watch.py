#!/usr/bin/env python3

import argparse
import git
import time
import datetime

def align (quitrepo, resourcelog, runlog):

    offset = 0
    with open(runlog, 'r') as runlogFile:
        firstLine = runlogFile.readline()
        s = " ".join(firstLine.split()[0:2])
        offset = int(time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f").timetuple()))

    resourcelog = open(resourcelog, 'r')
    repo = git.Repo(quitrepo)
    log = repo.git.log('--date=raw', '--pretty=format:%cd')
    # | awk '{ print $1 }'

    print("time", "reposize", "mem", "countCommits")

    log = list(e.split()[0] for e in log.split("\n"))

    countCommits = 1
    logPop = int(log.pop())
    for line in list(resourcelog):
        date = line.split()[0]
        #if not isinstance(date, int):
        if date == "time":
            print(line.strip(), "\"count of commits\"")
            continue
        #print(int(date), ">", logPop)
        while (int(date) > logPop):
            if log:
                logPop = int(log.pop())
                countCommits += 1
                #print(int(date), ">", logPop)
                #print(countCommits, date)
            else:
                break
        values = line.split()
        print(str(int(values[0])-offset), values[1], values[2], countCommits)

if __name__ == "__main__":

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--align', action='store_true')
    argparser.add_argument('quitrepo', type=str)
    argparser.add_argument('resourcelog', nargs='?', type=str)
    argparser.add_argument('runlog', nargs='?', type=str)

    args = argparser.parse_args()

    if args.align:
        align(args.quitrepo, args.resourcelog, args.runlog)
