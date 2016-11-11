#!/usr/bin/env python3

import xml.etree.ElementTree
import glob
import re
import itertools
import collections
import math

queryLabels = {
1: "INSERT DATA",
2: "DELETE WHERE",
3: "Explore 1" ,
4: "Explore 2" ,
5: "Explore 3" ,
6: "Explore 4" ,
7: "Explore 5" ,
# 8 does not exist
9: "Explore 7" ,
10: "Explore 8" ,
11: "Explore 9" ,
12: "Explore 10",
13: "Explore 11",
14: "Explore 12"
}

def getQPS ():
    """
    Extract gps and QMpH from bsbm result XML
    """
    lines = {}
    qmps = {}

    files = glob.glob("quit-*")
    print ("I could find the following run files: ", files)

    runPattern = re.compile('^quit(?P<setup>-gc|-nv)?(?P<number>-[0-9]*)(?P<logs>-logs)?$')

    # this is just for checking, if there is a logs folder for each run
    runs = {}
    runsHalf = set()
    for file in files:
        match = runPattern.match(file)
        if match:
            if match.group("logs"):
                runName = file[:-5]
            else:
                runName = file

            if (runName in runsHalf):
                runsHalf.remove(runName)
                setup = match.group("setup")
                runId = match.group("number")[1:]
                runs[runName] = {
                    "setup": setup,
                    "runId": runId
                }
            else:
                runsHalf.add(runName)


    print("I've identified the following runs:", runs)

    execSet = []
    qmph = []
    for runName, runProperties in runs.items():
        fileName = runName + "-logs/" + runName + ".xml"

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

    print("I've identified the following runs:", runs)

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
        print("this setup is:", setup)
        setups[setupOptions] = setup

    setups = collections.OrderedDict(sorted(setups.items(), key=lambda x:x[0] if x[0] else ""))
    print ("QMpH")
    for setup in setups.keys():
        print(setup, end="\t")
    print()
    for numbers in setups.values():
        #print(numbers)
        print(numbers["qmph"][1], numbers["qmph"][2], end="\t")
    print()
    print()
    print ("Queries")
    print("labels", end="\t")
    for setup in setups.keys():
        print(setup, end="\t")
    print()
    for id, label in queryLabels.items():
        print(label, end="\t")
        for numbers in setups.values():
            #print(numbers)
            print(numbers[id][1], numbers[id][2], end="\t")
        print()

if __name__ == "__main__":

    getQPS()
