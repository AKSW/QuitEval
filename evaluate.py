#!/usr/bin/env python3

import xml.etree.ElementTree
import glob
import re
import itertools
import collections
import math

import argparse
import git
import time
import datetime

from jinja2 import Template

import os

basedir = os.path.dirname(os.path.abspath(__file__))


queryLabels = {
1: "INSERT DATA",
2: "DELETE WHERE",
3: "Query 1" ,
4: "Query 2" ,
5: "Query 3" ,
6: "Query 4" ,
7: "Query 5" ,
# 8 does not exist
9: "Query 7" ,
10: "Query 8" ,
11: "Query 9" ,
12: "Query 10",
13: "Query 11",
14: "Query 12"
}

colors = ["#FF4A46", "#008941", "#006FA6", "#A30059", "#FFDBE5", "#7A4900", "#0000A6", "#63FFAC", "#B79762", "#004D43",
          "#8FB0FF", "#997D87", "#5A0007", "#809693", "#FEFFE6", "#1B4400", "#4FC601", "#3B5DFF"]

runPattern = re.compile('quit-(?P<setup>[^⁻]*)?(?P<number>-[0-9]*)$')

def findRuns (directory):
    files = glob.glob(os.path.join(directory, "quit-*"))
    print ("I could find the following run files: ", files)

    # this is just for checking, if there is a logs folder for each run
    runs = {}
    for file in files:
        match = runPattern.match(os.path.basename(file))
        if match:
            runName = os.path.basename(file)

            setup = match.group("setup")
            runId = match.group("number")[1:]
            runs[runName] = {
                "setup": setup,
                "runId": runId
            }

    print("I've identified the following runs:", runs)
    return runs

def getQPS (directory):
    """
    Extract gps and QMpH from bsbm result XML, calculate average and standard deviation
    """
    runs = findRuns(directory)

    execSet = []
    qmph = []
    for runName, runProperties in runs.items():
        fileName = os.path.join(directory, runName, "logs", runName + ".xml")

        e = xml.etree.ElementTree.parse(fileName).getroot()
        subset = {}
        runProperties["qmph"] = float(e.find('querymix').find('qmph').text)
        for qu in e.find('queries').findall('query'):
            #print (qu.get('nr'))
            nr = qu.get('nr')
            if qu.find('qps') != None:
                #print(qu.find('qps').text)
                subset[int(nr)] = float(qu.find('qps').text)
        runProperties["queries"] = subset

    print("I've identified the following values for the runs:", runs)

    # https://stackoverflow.com/questions/1241029/how-to-filter-a-dictionary-by-value#1241354
    # covert runs to list sorted by setup value
    runs = sorted(runs.items(), key=lambda x:x[1]["setup"] if x[1]["setup"] else "")
    # group runs by setup value
    groupedRuns = itertools.groupby(runs, key=lambda x:x[1]["setup"])

    setups = {}
    for key, group in groupedRuns:
        runGroup = dict(group)
        #print(runGroup)
        setup = {}
        setupOptions = list(runGroup.values())[0]["setup"]

        # calculate sum
        for runName, runProperties in runGroup.items():
            for queryNum, queryQps in runProperties["queries"].items():
                if not queryNum in setup:
                    setup[queryNum] = [0, 0, 0]
                setup[queryNum][0] = setup[queryNum][0] + 1
                setup[queryNum][1] = setup[queryNum][1] + queryQps

            if not "qmph" in setup:
                setup["qmph"] = [0, 0, 0]
            setup["qmph"][0] = setup["qmph"][0] + 1
            setup["qmph"][1] = setup["qmph"][1] + runProperties["qmph"]

        #print("this setup is:", setup)

        # reduce for average
        for figure, result in setup.items():
            avg = result[1]/result[0]
            setup[figure][1] = avg

        # collect again for variance and standard deviation
        for runName, runProperties in runGroup.items():
            for queryNum, queryQps in runProperties["queries"].items():
                avg = setup[queryNum][1]
                setup[queryNum][2] = setup[queryNum][2] + (queryQps - avg) * (queryQps - avg)

            avg = setup["qmph"][1]
            setup["qmph"][2] = setup["qmph"][2] + (runProperties["qmph"] - avg) * (runProperties["qmph"] - avg)

        # reduce for variance and standard deviation
        for figure, result in setup.items():
            # variance
            var = result[2]/result[0]
            # set standard deviation
            setup[figure][2] = math.sqrt(var)

        setup["setup"] = setupOptions
        setup["runs"] = len(runGroup)
        #print("this setup is:", setup)
        setups[setupOptions] = setup

    setups = collections.OrderedDict(sorted(setups.items(), key=lambda x:x[0] if x[0] else ""))

    bsbm_qmph_dat = ""
    print ("QMpH")
    for setup in setups.keys():
        bsbm_qmph_dat += "\"{}\" \"{}\"\t".format(setup, setup)
    bsbm_qmph_dat += "\n"
    for numbers in setups.values():
        #print(numbers)
        bsbm_qmph_dat += "{} {}\t".format(numbers["qmph"][1], numbers["qmph"][2])
    bsbm_qmph_dat += "\n"

    with open(os.path.join(directory, "bsbm_qmph.dat"), "w") as bsbm_qmph_dat_file:
        bsbm_qmph_dat_file.write(bsbm_qmph_dat)

    print ("Queries")
    bsbm_dat = "labels\t"
    for setup in setups.keys():
        bsbm_dat += "\"{}\" \"{}\"\t".format(setup, setup)
    bsbm_dat += "\n"
    for id, label in queryLabels.items():
        bsbm_dat += "\"{}\"\t".format(label)
        for numbers in setups.values():
            #print(numbers)
            bsbm_dat += "{} {}\t".format(numbers[id][1], numbers[id][2])
        bsbm_dat += "\n"

    with open(os.path.join(directory, "bsbm.dat"), "w") as bsbm_dat_file:
        bsbm_dat_file.write(bsbm_dat)

    # Write gnuplot scripts

    bsbm_data = {
        "file": 'bsbm.dat',
        "file_qmph": 'bsbm_qmph.dat',
        "scenarios": []
    }

    column = 0
    for setup in setups.keys():
        column += 2
        if column/2 > len(colors):
            print("WARNING: colors can not be distinguished")
        bsbm_data["scenarios"].append({"setup": setup, "column": column, "color": colors[int(column/2)%len(colors)]})

    with open( os.path.join(basedir, "stuff", 'bsbm.plot.tpl'), "r" ) as bsbm_tpl:
        template = Template( bsbm_tpl.read() )
        with open(os.path.join(directory, "bsbm.plot"), "w") as bsbm_plot:
            bsbm_plot.write(template.render(bsbm_data))

    with open( os.path.join(basedir, "stuff", 'bsbm_qmph.plot.tpl'), "r" ) as bsbm_tpl:
        template = Template( bsbm_tpl.read() )
        print(template.render(bsbm_data))
        with open(os.path.join(directory, "bsbm_qmph.plot"), "w") as bsbm_plot:
            bsbm_plot.write(template.render(bsbm_data))

        with open( os.path.join(basedir, "stuff", 'bsbm.plot.tpl'), "r" ) as bsbm_tpl:
            #read it
            template = Template( bsbm_tpl.read() )
            #do the substitution
            gnuplot = template.render(bsbm_data)

def alignCommits (runDir):
    """
    This method adds another column to the resource/memory log, containing the number of commits
    The original input already contains the three columns "timestamp", "repo size", "memory consumption"
    """

    offset = 0
    run = os.path.basename(runDir)

    with open(os.path.join(runDir, "logs", runName + "-run.log"), 'r') as runlogFile:
        firstLine = runlogFile.readline()
        s = " ".join(firstLine.split()[0:2])
        offset = int(time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f").timetuple()))

    resourcelog = open(os.path.join(runDir, "logs", "resources-mem.log"), 'r')
    # TODO read from scenario configuration
    repo = git.Repo(os.path.join(runDir, "repo"))
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
        while (float(date) > logPop):
            if log:
                logPop = int(log.pop())
                countCommits += 1
                #print(int(date), ">", logPop)
                #print(countCommits, date)
            else:
                break
        values = line.split()
        print(str(float(values[0])-offset), values[1], values[2], countCommits)

def alignAddDelete (runDir):
    """
    This method adds another column to the resource/memory log, containing the number of added and removed lines per commit
    The original input already contains the three columns "timestamp", "repo size", "memory consumption"
    TODO: there might also be some option neccessary, which defines the width of the sliding window
    """

    offset = 0
    run = os.path.basename(runDir)
    with open(os.path.join(runDir + "-logs", run + "-run.log"), 'r') as runlogFile:
        firstLine = runlogFile.readline()
        s = " ".join(firstLine.split()[0:2])
        offset = int(time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f").timetuple()))

    resourcelog = open(os.path.join(runDir + "-logs", "mem-" + run), 'r')
    repo = git.Repo(runDir)
    log = repo.git.log('--date=raw', '--pretty=format:%cd', '--numstat')

    print("time", "reposize", "mem", "countCommits", "countStatements", "countAdd", "countDelete")

    log = list(e.split() for e in log.split("\n\n"))

    countCommits = 1
    logPop = log.pop()
    countStatements = 0
    countAdd = 0
    countDelete = 0
    for line in list(resourcelog):
        date = line.split()[0]
        # print title line
        if date == "time":
            print(line.strip(), "\"count of commits\"")
            continue
        values = line.split()
        if int(date) > int(logPop[0]):
            while (int(date) > int(logPop[0])):
                if log:
                    countCommits += 1
                    countAdd = int(logPop[2])
                    countDelete = int(logPop[3])
                    countStatements -= countDelete
                    countStatements += countAdd
                    print(str(int(values[0])-offset), values[1], values[2], countCommits, countStatements, countAdd, countDelete)
                    logPop = log.pop()
                    #print(int(date), ">", logPop)
                    #print(countCommits, date)
                else:
                    break
        else:
            print(str(int(values[0])-offset), values[1], values[2], countCommits, countStatements, countAdd, countDelete)

def plotForMem (directory):
    runs = findRuns(directory)

    # https://stackoverflow.com/questions/1241029/how-to-filter-a-dictionary-by-value#1241354
    # covert runs to list sorted by setup value
    runs = sorted(runs.items(), key=lambda x:x[1]["setup"] if x[1]["setup"] else "")
    # group runs by setup value
    groupedRuns = itertools.groupby(runs, key=lambda x:x[1]["setup"])

    for key, group in groupedRuns:
        runGroup = dict(group)
        setup = {}
        for runName, runProperties in runGroup.items():
            fileName = os.path.join(directory, runName + "-logs", "mem-" + runName)
            with open(fileName) as memlogFile:
                memlog = list(memlogFile)
                for line in memlog:
                    print(line.split())
                # decide, where to align and
                # write plot points to common structure


if __name__ == "__main__":
    """
    This script aligns the number of commits  with the resourcelog (mem_…-file) using the run log produced by the BSBM.
    It outputs the (mem_…-file) with a 4th column containing the number of commits.
    """

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--mem', action='store_true')
    argparser.add_argument('--bsbm', action='store_true')
    argparser.add_argument('--align', action='store_true')
    argparser.add_argument('--alignAD', action='store_true')
    argparser.add_argument('directory', default=".", nargs='?', type=str)

    args = argparser.parse_args()

    if args.mem:
        plotForMem(args.directory)
    elif args.bsbm:
        getQPS(args.directory)
    elif args.align:
        # directory in this case is a specific quit run repo
        alignCommits(args.directory)
    elif args.alignAD:
        # directory in this case is a specific quit run repo
        alignAddDelete(args.directory)
    else:
        argparser.print_help()
