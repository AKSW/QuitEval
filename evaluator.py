#!/usr/bin/env python3
import random
import argparse
import requests
import sys
import pygit2
import os
import datetime
from executeQueryLog import MonitorThread


class Evaluator:
    """A super class containg attributes and methods used in both subclasses."""

    platform=''
    endpoint=''
    logFile=''
    logDir='/var/logs'
    runs=10

    def postRequest(self, query, ref=None):
        if ref is not None:
            self.endpoint + '/' + ref
        else:
            endpoint = self.endpoint

        start = datetime.datetime.now()
        res = requests.post(
            endpoint,
            data={'query': query},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        return start, end

    def rawbaseRequest(self, query):
        start = datetime.datetime.now()
        parent = self.rwbaseGetParent()
        params = {"rwb-version": parent}
        res = requests.post(
            self.endpoint,
            data=query,
            params=params,
            headers={'Accept': 'application/json', "Content-Type": "application/sparql-update"})
        end = datetime.datetime.now()
        return start, end


class QueryLogExecuter(Evaluator):
    """A class that will execute generated queries for a given platform."""

    def __init__(
            self,
            endpoint='http://localhost:8080/r43ples/sparql',
            logFile='execution.log',
            logDir='/var/logs',
            queryLog='',
            mode='bsbm-log',
            store=None,
            virtuoso=None,
            triples=None,
            count=None):

        self.mode = mode
        self.endpoint = endpoint
        self.queryLog = queryLog
        self.logDir = logDir
        self.logFile = os.path.join(self.logDir, logFile)
        self.mode = mode
        self.store = store
        self.count = count
        self.virtuoso = virtuoso
        self.triples = triples
        self.revisionQuery = "prefix prov: <http://www.w3.org/ns/prov#> select ?entity where {"
        self.revisionQuery += "graph <urn:rawbase:provenance> {?entity a prov:Entity. "
        self.revisionQuery += "?activity prov:generated ?entity ; prov:atTime ?time}} order by desc(?time) limit 1"

        try:
            self.initQueryLog()
        except Exception:
            raise Exception('Could not read query log')

    def initQueryLog(self):

        from lsbm import lsbm
        lsbm_instance = lsbm("http://example.org/", "urn:bsbm", self.store)
        lsbm_instance.prepare(self.count, self.queryLog)

        self.queries = lsbm_instance.queryList

    def runQueries(self):
        if self.store == 'rawbase':
            self.runQueriesRawbase()
        else:
            self.runQueriesRest()

    def runQueriesRest(self):
        for query in self.queries:
            with open(self.logFile, 'a+') as executionLog:
                start, end = self.postRequest(query)
                data = [str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')

    def runQueriesRawbase(self):
        for query in self.queries:
            with open(self.logFile, 'a+') as executionLog:
                start, end = self.rawbaseRequest(query)
                data = [str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')

    def rwbaseGetParent(self):
        query = "prefix prov: <http://www.w3.org/ns/prov#> select ?entity where {graph <urn:rawbase:provenance> {?entity a prov:Entity. ?activity prov:generated ?entity ; prov:atTime ?time}} order by desc(?time) limit 1"

        response = requests.post(self.virtuoso, data={'query': query},
                                 headers={'Accept': 'text/csv'})

        if len(response.text.split("\n")) > 0:
            parent = response.text.split("\n")[1].strip("\"")
            return parent
        return ""

    def get_size(self, start_path='database/dataset'):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            total_size += os.path.getsize(dirpath)
            # self.logger.debug("size {} of {}".format(os.path.getsize(dirpath), dirpath))
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
                # self.logger.debug("size {} of {}".format(os.path.getsize(fp), fp))
        return total_size / 1024


class RandomAccess(Evaluator):
    """Execute Select Queries randomly over existing reviosions."""

    logFile = ''
    commits = []

    def __init__(
            self,
            platform='',
            endpoint='',
            revisions=10,
            logFile='',
            logDir='/var/logs',
            runs=10):

        self.endpoint = endpoint
        self.logDir = logDir
        self.logFile = os.path.join(self.logDir, logFile)

        if platform not in ['quit', 'r43ples', 'rawbase']:
            print('No platform selected.')
            sys.exit()

        self.platform = platform
        self.logdir = logDir
        self.logFile = os.path.join(self.logDir, logFile)

    def initQuit():
        if platform == 'quit':
            try:
                self.repo = pygit2.Repository(repoDir)
            except Exception:
                raise Exception('{} is no repository'.format(repoDir))

        if isinstance(revisions, int):
            self.revisions = revisions
        else:
            raise Exception('Expect integer for argument "revisions", got {}, {}'.format(revisions, type(revisions)))

        if isinstance(runs, int):
            self.runs = runs
        else:
            raise Exception('Expect integer for argument "runs", got {}, {}'.format(runs, type(runs)))

    def getRevisions(self):
        if self.platform == 'quit':
            self.getQuitRevisions()
        elif self.platform == 'rawbase':
            self.getRawbaseRevisions()

    def getQuitRevisions(self):
        commits = {}
        i = 0
        for commit in self.repo.walk(self.repo.head.target, pygit2.GIT_SORT_REVERSE):
            commits[i] = (str(commit.id))
            i += 1
        self.commits = commits
        print('Found {} commits'.format(len(commits.items())))

    def getRawbaseRevisions(self):
        query = "prefix prov: <http://www.w3.org/ns/prov#> select ?entity where {graph <urn:rawbase:provenance> {?entity a prov:Entity. ?activity prov:generated ?entity ; prov:atTime ?time}} order by desc(?time)"

        response = requests.post(self.endpoint, data={'query': query},
                                 headers={'Accept': 'text/csv'})

        if len(response.text.split("\n")) > 0:
            revisions = response.text.split("\n")
            for line in revisions[1:]:
                self.revisions.append(strip("\""))

    def runBenchmark(self):
        if self.platform == 'quit':
            self.getRunQuitBenchmark()
        elif self.platform == 'r43ples':
            self.getRunR43ples()
        elif self.platform == 'rawbase':
            self.getRunRawbaseBenchmark()


    def runQuitBenchmark(self):
        i = 1
        query = """
            SELECT * WHERE { graph ?g { ?s ?p ?o .}} LIMIT 10"""


        while i < self.runs:
            with open(self.logFile, 'a') as executionLog:
                ref = random.choice(self.commits)
                start, end = self.postRequest(query, ref)
                data = [ref, str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')
                print(' '.join(data))
                i = i + 1

    def runR43plesBenchmark(self):
        i = 1
        choices = set([0, self.revisions, self.revisions/4, self.revisions*3/4])

        while i < self.runs:
            with open(self.logFile, 'a') as executionLog:
                ref = random.choice(choices)
                query = "SELECT ? WHERE {{ graph <urn:bsbm> REVISION \"{}\" {{ ?s ?p ?o }} }} LIMIT 1".format(ref)
                start, end = self.postRequest(query)
                data = [ref, str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')
                print(' '.join(data))
                i = i + 1

    def runRawbaseBenchmark(self):
        i = 0
        while i < self.runs:
            with open(self.logFile, 'a') as executionLog:
                ref = random.choice(self.revisions)
                start, end = self.postRequest(ref)
                data = [ref, str(end - start), str(start), str(end), str((end - start) + self.timePerRevision)]
                executionLog.write(' '.join(data) + '\n')
                i = i + 1


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

    bm = EvalRevisions(
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
