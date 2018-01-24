#!/usr/bin/env python3
import random
import argparse
import requests
import sys
import pygit2
import os
import datetime
import time

class QuitEvalRefQueries:
    logFile = ''
    commits = []

    QUERY = """
        SELECT * WHERE { graph ?g { ?s ?p ?o . ?o ?pp ?oo .}} LIMIT 10"""

    def __init__(self, endpoint='http://localhost:8080/r43ples/sparql', logFile='', runs=10, queryLog=''):
        self.endpoint = endpoint
        self.queryLog = queryLog
        try:
            response = requests.post(endpoint, data={'query': self.QUERY}, headers={'Accept': 'application/json'})
        except Exception:
            raise Exception('Cannot access {}'.endpoint)

        if response.status_code == 200:
            pass
        else:
            raise Exception('Something wrong with sparql endpoint.')

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
        try:
            self.initQueryLog()
        except Exception:
            raise Exception('Could not read query log')

    def initQueryLog(self):
        if os.path.isfile(self.queryLog):
            write = False
            queries = []
            query = []
            with open(self.queryLog, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('Query string:'):
                        write = True
                        query = []
                    elif line.startswith('Query result'):
                        write = False
                        queries.append(' '.join(query))
                    elif write is True:
                        query.append(line)

        print('Found {} queries'.format(len(queries)))
        self.queries = queries

    def runQueries(self):
        results = []

        for query in self.queries:
            start, end = self.postRequest(query)
            time.sleep(3.5)
            results.append([str(end - start), str(start), str(end)])
        with open(self.logFile, 'w') as f:
            for line in results:
                f.write(' '.join(line) + '\n')


    def postRequest(self, query):
        start = datetime.datetime.now()
        print('Executing', query)
        res = requests.post(
            self.endpoint,
            data={'query': query},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        print(res.status_code)
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
        default='http://localhost:8080/r43ples/sparql',
        help='Link to the SPARQL-Endpoint')

    parser.add_argument(
        '-L',
        '--logfile',
        type=str,
        default='../docker.benchmark/quit-woGC-python3.6-1/logs/quit-woGC-python3.6-1-eval.log',
        help='The link where to log the benchmark')

    parser.add_argument(
        '-Q',
        '--querylog',
        type=str,
        default='run.log',
        help='The link where to find a bsbm run log benchmark')

    return parser.parse_args()


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    bm = QuitEvalRefQueries(args.endpoint, args.logfile, args.runs, args.querylog)
    bm.runQueries()
