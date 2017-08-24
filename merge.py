#!/usr/bin/env python3

import argparse
import git
import random
from verify import compareSets


def szenario(s, l, r):
    """Define the Szenarios.

    szenarios:
         base  A   B   result
      1   0    0   0     0
      2   X    X   X     X
      3   0    X   0     X
      4   0    0   X     X
      5   X    0   X     0
      6   X    X   0     0
      7   0    X   X     X
      8   X    0   0     0
    """
    a = bool(l) and not bool(s)
    b = bool(r) and not bool(s)
    c = bool(l) and bool(r)
    return a or b or c


def doMerges(quitrepo, dataset, seed, resultPath):
    random.seed(seed)

    baseString = []
    localString = []
    remoteString = []
    resultString = []

    with open(dataset, 'r') as datasetFile:
        for statement in list(datasetFile):
            s = bool(random.randint(0, 1))
            a = bool(random.randint(0, 1))
            b = bool(random.randint(0, 1))
            if s:
                baseString.append(statement)
            if a:
                localString.append(statement)
            if b:
                remoteString.append(statement)
            if szenario(s, a, b):
                resultString.append(statement)

    if resultPath:
        with open(resultPath, 'w') as resultFile:
            resultFile.writelines(sorted(resultString))

    graphFileName = "graph.nq"
    graphPath = quitrepo + "/" + graphFileName

    repo = git.Repo(quitrepo)
    try:
        repo.git.rm([graphFileName])
        repo.git.commit('-m', "wipe graph")
    except Exception:
        pass

    with open(graphPath, 'w') as graphFile:
        graphFile.writelines(baseString)

    repo.git.add([graphFileName])
    repo.git.commit('-m', "init graph")
    baseId = repo.head.commit

    with open(graphPath, 'w') as graphFile:
        graphFile.writelines(localString)

    repo.git.add([graphFileName])
    repo.git.commit('-m', "branch a")
    branchA = repo.head.commit
    repo.git.checkout(baseId)

    print("branch a:", branchA)

    with open(graphPath, 'w') as graphFile:
        graphFile.writelines(remoteString)

    repo.git.add([graphFileName])
    repo.git.commit('-m', "branch b")
    branchB = repo.head.commit

    print("branch b:", branchB)

    repo.git.checkout(branchA)
    repo.git.merge(branchB)

    with open(graphPath, 'r') as graphFile:
        actual = set(filter(lambda line: line.strip(), set(graphFile)))
        wanted = set(filter(lambda line: line.strip(), resultString))
        if compareSets(wanted, actual):
            print("everything is fine")
        else:
            print("could not verify merge")
            exit(1)


if __name__ == "__main__":
    """This script executes a bunch of merges on a quit repository and verifies the correctness of
    the merge result.
    """

    argparser = argparse.ArgumentParser()
    argparser.add_argument('quitrepo', type=str)
    argparser.add_argument('dataset', type=str)
    argparser.add_argument('--seed', type=str)
    argparser.add_argument('--result', type=str)

    args = argparser.parse_args()

    doMerges(args.quitrepo, args.dataset, args.seed, args.result)
