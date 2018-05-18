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
            endpoint='http://localhost:8080/r43ples/sparql',
            revisions=10,
            logFile='',
            logDir='/var/logs',
            runs=10):

        self.endpoint = endpoint
        self.logDir = logDir
        self.logFile = os.path.join(self.logDir, logFile)

        if isinstance(revisions, int):
            self.revisions = revisions
        else:
            raise Exception('Expect integer for argument "revisions", got {}, {}'.format(revisions, type(revisions)))

        if isinstance(runs, int):
            self.runs = runs
        else:
            raise Exception('Expect integer for argument "runs", got {}, {}'.format(runs, type(runs)))

    def runBenchmark(self):
        i = 1
        choices = set([0, self.revisions, self.revisions/4, self.revisions*3/4])


        while i < self.runs:
            with open(self.logFile, 'a') as executionLog:
                ref = random.choice(choices)
                start, end = self.postRequest(ref)
                data = [ref, str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')
                print(' '.join(data))
                i = i + 1

    def postRequest(self, ref):
        query = "SELCET ? WHERE (graph <urn:bsbm> REVISION \"{{}}\" { ?s ?p ?o }} LIMIT 1".format(ref)
        print(query)
        start = datetime.datetime.now()
        res = requests.post(
            self.endpoint,
            data={'query': query},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        # print('Query executed on', ref, res.status_code, res.json())
        return start, end


def parseArgs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-E', '--endpoint',
        type=str,
        default='http://localhost:8080/r43ples/sparql',
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
        '--revisions',
        type=int,
        help='The number of the highest known revision number.')

    return parser.parse_args()


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')

    bm = EvalCommits(
        endpoint=args.endpoint,
        revisions=args.revisions,
        logFile='eval.revisions.log',
        logDir=args.logdir,
        runs=args.runs)

    if args.processid:
        mon = MonitorThread(logDir=args.logdir, logFile='memory.revisions.log')

        mon.setstoreProcessAndDirectory(
            pid=args.processid,
            observedDir=args.observeddir)
        mon.start()

    print('Starting Benchmark')
    bm.runBenchmark()
    print('Benchmark finished')

    if args.processid:
        mon.stop()
