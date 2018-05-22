#!/usr/bin/env python3
import random
import argparse
import requests
import sys
import pygit2
import os
import datetime
from executeQueryLog import MonitorThread


class EvalCommits:
    logFile = ''
    commits = []

    QUERY = """
        SELECT * WHERE { graph ?g { ?s ?p ?o .}} LIMIT 10"""

    def __init__(
            self,
            endpoint='http://localhost:5000/sparql',
            repoDir='',
            logFile='',
            logDir='/var/logs',
            runs=10):

        self.endpoint = endpoint
        self.logDir = logDir
        self.logFile = os.path.join(self.logDir, logFile)
        try:
            self.repo = pygit2.Repository(repoDir)
        except Exception:
            raise Exception('{} is no repository'.format(repoDir))

        if isinstance(runs, int):
            self.runs = runs
        else:
            raise Exception('Expect integer for argument "runs", got {}, {}'.format(runs, type(runs)))

        # collect commits
        commits = {}
        i = 0
        for commit in self.repo.walk(self.repo.head.target, pygit2.GIT_SORT_REVERSE):
            commits[i] = (str(commit.id))
            i += 1
        self.commits = commits
        print('Found {} commits'.format(len(commits.items())))

    def runBenchmark(self):
        i = 1
        results = []

        while i < self.runs:
            with open(self.logFile, 'a') as executionLog:
                ref = random.choice(self.commits)
                start, end = self.postRequest(ref)
                data = [ref, str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')
                print(' '.join(data))
                i = i + 1

    def postRequest(self, ref):
        start = datetime.datetime.now()
        res = requests.post(
            self.endpoint + '/' + ref,
            data={'query': self.QUERY},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        # print('Query executed on', ref, res.status_code, res.json())
        return start, end


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-E', '--endpoint',
        type=str,
        default='http://localhost:5000/sparql',
        help='Link to the SPARQL-Endpoint')

    parser.add_argument(
        '-L',
        '--logdir',
        type=str,
        default='/var/logs/',
        help='The link where to log the benchmark')

    parser.add_argument(
        '-O',
        '--observeddir',
        default='.',
        help='The directory that should be monitored')

    parser.add_argument(
        '-P',
        '--processid',
        type=int,
        help='The command name of the process to be monitored')

    parser.add_argument(
        '-runs', '--runs',
        type=int,
        default=10)

    parser.add_argument(
        '-R',
        '--repodir',
        type=str)

    return parser.parse_args()


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])

    bm = EvalCommits(
        endpoint=args.endpoint,
        repoDir=args.repodir,
        logFile= 'eval.commits.log',
        logDir=args.logdir,
        runs=args.runs)

    if (args.processid):
        mon = MonitorThread(logDir=args.logdir, logFile='memory.commits.log')

        mon.setstoreProcessAndDirectory(
            pid=args.processid,
            observedDir=args.observeddir)
        mon.start()

    print('Starting benchmark')
    bm.runBenchmark()
    print('Benchmark finished')

    if (args.processid):
        mon.stop()
