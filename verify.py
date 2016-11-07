#!/usr/bin/env python3

import argparse
import git
import rdflib
from rdflib.plugins.sparql import parser, algebra
from rdflib.plugins import sparql
import tempfile
import sys
from subprocess import call, CalledProcessError

sys.setrecursionlimit(3000)

def compareSets(right, left):

    add = right - left
    rem = left - right

    if not add and not rem:
        print("no difference")
        return True
    else:
        ll = len(left)
        lr = len(right)
        print ("len(left)", ll, "len(right)", lr)

        for a in add:
            sys.stdout.write("+ " + a)
        for a in rem:
            sys.stdout.write("- " + a)
        return False

def forwardAndVerifyStores (repo, store, updateStrings):

    try:
        parsedUpdate = parser.parseUpdate("".join(updateStrings))
        query = algebra.translateUpdate(parsedUpdate)
        before = len(store)
        store.update(query)
        after = len(store)
        #print ("Store had", before, "statements and has", after, "statements")
        if before != after:
            f = store.serialize(format="nquads").decode("utf-8")

            print ("currently on commit", repo.head.commit)
            ancestryPath = repo.git.log('--reverse', '--ancestry-path', '--pretty=format:"%h"', str(repo.head.commit)+'..master')
            ancestryPath = ancestryPath.split("\n")
            nextcommit = ancestryPath[0].strip("\"")
            print("checking out", nextcommit)
            repo.git.checkout(nextcommit)

            graphFile = open(args.quitrepo + "/graph.nq", 'r')
            left = set(filter(lambda line: line.strip(), set(e + "\n" for e in f.split("\n"))))
            right = set(filter(lambda line: line.strip(), set(graphFile)))
            graphFile.close()

            if not compareSets(right, left):
                return nextcommit

    except Exception as e:
        print('Something is wrong in the function:', e)
        import traceback
        traceback.print_tb(e.__traceback__, limit=20)
        #print ("".join(updateStrings))
        exit(1)

    return None

if __name__ == "__main__":
    """
    This script verifies a quit repository agains the query log executed on the store
    """

    argparser = argparse.ArgumentParser()
    argparser.add_argument('querylog', type=str)
    argparser.add_argument('quitrepo', type=str)
    argparser.add_argument('initialdata', type=str)

    args = argparser.parse_args()

    try:
        querylog = open(args.querylog, 'r')
        repo = git.Repo(args.quitrepo)
        store = rdflib.ConjunctiveGraph()
        store.parse(args.initialdata, format='nquads', publicID='http://localhost:5000/')
    except Exception as e:
        print('Something is wrong:', e)
        import traceback
        traceback.print_tb(e.__traceback__, limit=20)

    count = 0
    query = []
    mode = "garbage"
    execType = ""
    errors = []
    for line in list(querylog):
        if "Query string:" in line:
            mode = "query"
        elif "Query results" in line or "Query(Construct) result" in line:
            if execType == "update":
                result = forwardAndVerifyStores(repo, store, query)
                if result:
                    errors.append(result)
            mode = "garbage"
            query = []
            execType = ""
        elif mode == "query":
            if "INSERT" in line or "DELETE" in line:
                execType = "update"
            query.append(line)
    querylog.close()

    if errors:
        print ("Errors where detected in the following commits:", errors)
    else :
        print ("Everythin is fine, all update operations where recorded correctly")
