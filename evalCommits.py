#!/usr/bin/env python3
import random
import argparse
import requests
import sys
import pygit2
import os
import datetime

class QuitEvalRefQueries:
    logFile = ''
    commits = []

    QUERY = """
        SELECT * WHERE { graph ?g { ?s ?p ?o . ?o ?pp ?oo .}} LIMIT 10"""

    def __init__(self, endpoint='http://localhost:5000/sparql', repoPath='', logFile='', runs=10):
        self.endpoint = endpoint
        try:
            response = requests.post(endpoint, data={'query': self.QUERY}, headers={'Accept': 'application/json'})
        except Exception:
            raise Exception('Cannot access {}'.endpoint)

        if response.status_code == 200:
            pass
        else:
            raise Exception('Something wrong with sparql endpoint.')

        try:
            self.repo = pygit2.Repository(repoPath)
        except Exception:
            raise Exception('{} is no repository'.format(repoPath))

        try:
            with open(logFile, 'w') as f:
                pass
            f.close()
            self.logFile = logFile
        except Exception:
            raise Exception('Can\'t write file {}'.format(logFile))

        if isinstance(runs, int):
            self.runs = runs
        else:
            raise Exception('Expect integer for argument "runs", got {}, {}'.format(runs, type(runs)))

        # collect commits
        commits = []
        for commit in self.repo.walk(self.repo.head.target, pygit2.GIT_SORT_REVERSE):
            commits.append(str(commit.id))
        self.commits = commits
        print('Found these commits: {}'.format(commits))

    def runBenchmark(self):
        i = 1
        results = []
        while i < self.runs:
            ref = random.choice(self.commits)
            start, end = self.postRequest(ref)
            results.append([ref, str(end - start), str(start), str(end)])
            i = i + 1
        with open(self.logFile, 'w') as f:
            for line in results:
                f.write(' '.join(line) + '\n')
        f.close()

    def postRequest(self, ref):
        start = datetime.datetime.now()
        res = requests.post(
            self.endpoint + '/' + ref,
            data={'query': self.QUERY},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        print(res.status_code, res.json(), ref)
        return start, end


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-runs', '--runs',
        type=int,
        default=10)

    parser.add_argument(
        '-E', '--endpoint',
        type=str,
        default='http://localhost:5000/sparql',
        help='Link to the SPARQL-Endpoint')

    parser.add_argument(
        '-R',
        '--repopath',
        type=str)

    parser.add_argument(
        '-L',
        '--logfile',
        type=str,
        default='../docker.benchmark/quit-woGC-python3.6-1/logs/quit-woGC-python3.6-1-eval.log',
        help='The link to the BSBM run.log file containing the queries')

    return parser.parse_args()


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    bm = QuitEvalRefQueries(args.endpoint, args.repopath, args.logfile, args.runs)
    bm.runBenchmark()
