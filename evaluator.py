#!/usr/bin/env python3
import random
import requests
import sys
import pygit2
import os
import datetime


class Evaluator:
    """A super class containg attributes and methods used in both subclasses."""

    platform=''
    endpoint=''
    logFile=''
    logDir='/var/logs'
    count=10

    def postRequest(self, query, ref=None):
        if ref is not None:
            endpoint = self.endpoint + '/' + ref
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
            runs=None):

        self.mode = mode
        self.endpoint = endpoint
        self.queryLog = queryLog
        self.logDir = logDir
        self.logFile = os.path.join(self.logDir, logFile)
        self.mode = mode
        self.store = store
        self.runs = runs
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
        lsbm_instance.prepare(self.triples, self.queryLog)

        self.queries = lsbm_instance.queryList

    def run(self):
        if self.store == 'rawbase':
            self.runRawbase()
        else:
            self.runRest()
        return len(self.queries)

    def runRest(self):
        for query in self.queries:
            with open(self.logFile, 'a+') as executionLog:
                start, end = self.postRequest(query)
                data = [str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')

    def runRawbase(self):
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

    # def get_size(self, start_path='database/dataset'):
    #     total_size = 0
    #     for dirpath, dirnames, filenames in os.walk(start_path):
    #         total_size += os.path.getsize(dirpath)
    #         # self.logger.debug("size {} of {}".format(os.path.getsize(dirpath), dirpath))
    #         for f in filenames:
    #             fp = os.path.join(dirpath, f)
    #             total_size += os.path.getsize(fp)
    #             # self.logger.debug("size {} of {}".format(os.path.getsize(fp), fp))
    #     return total_size / 1024
    #

class RandomAccessExecuter(Evaluator):
    """Execute Select Queries randomly over existing reviosions."""

    logFile = ''
    commits = []
    revisions = []

    def __init__(
            self,
            platform='',
            repo='',
            graph='urn:bsbm',
            endpoint='',
            expectedRevisions=10,  # important for r43ples
            logFile='',
            logDir='/var/logs',
            count=10):

        self.endpoint = endpoint
        self.logDir = logDir
        self.graph = graph

        if platform not in ['quit', 'r43ples', 'rawbase']:
            print('No platform selected.')
            sys.exit()

        if logFile != '':
            self.logFile = os.path.join(self.logDir, logFile)
        else:
            self.logFile = os.path.join(self.logDir, 'ra-' + platform + '.log')

        if isinstance(count, int):
            self.count = count
        else:
            raise Exception('Expect integer for argument "runs", got {}, {}'.format(runs, type(runs)))

        self.platform = platform

        if platform == 'quit':
            try:
                self.repo = pygit2.Repository(repo)
            except Exception:
                raise Exception('{} is no repository'.format(repo))

        if isinstance(expectedRevisions, int):
            self.expectedRevisions = expectedRevisions
        else:
            raise Exception('Expect integer for argument "revisions", got {}, {}'.format(
                expectedRevisions, type(expectedRevisions)))

    def getRevisions(self):
        if self.platform == 'quit':
            self.getQuitRevisions()
        elif self.platform == 'r43ples':
            self.getR43plesRevisions()
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

    def getR43plesRevisions(self):
        query = """select ?rev where {{
            graph <{}-revisiongraph> {{
                ?s <http://eatld.et.tu-dresden.de/rmo#revisionNumber> ?rev .}} }} ORDER BY ?rev""".format(
            self.graph)

        response = requests.post(self.endpoint, data={'query': query},
                                 headers={'Accept': 'application/json'})

        data = response.json()
        self.revisions = len(data['results']['bindings']) - 1

    def getRawbaseRevisions(self):
        query = "prefix prov: <http://www.w3.org/ns/prov#> select ?entity where {graph <urn:rawbase:provenance> {?entity a prov:Entity. ?activity prov:generated ?entity ; prov:atTime ?time}} order by desc(?time)"

        response = requests.post(self.endpoint, data={'query': query},
                                 headers={'Accept': 'text/csv'})

        if len(response.text.split("\n")) > 0:
            revisions = response.text.split("\n")
            for line in revisions[1:]:
                self.revisions.append(line.strip("\""))

    def run(self):
        if self.platform == 'quit':
            self.runQuitBenchmark()
        elif self.platform == 'r43ples':
            self.runR43plesBenchmark()
        elif self.platform == 'rawbase':
            self.runRawbaseBenchmark()


    def runQuitBenchmark(self):
        if len(self.commits) == 0:
            print('There are no revisions')
            return
        i = 1
        query = """
            SELECT * WHERE { graph ?g { ?s ?p ?o .}} LIMIT 10"""


        while i < self.count:
            with open(self.logFile, 'w+') as executionLog:
                ref = random.choice(self.commits)
                start, end = self.postRequest(query, ref)
                data = [ref, str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')
                print(' '.join(data))
                i = i + 1

    def runR43plesBenchmark(self):
        if self.revisions == 0:
            print('There are no revisions')
            return

        i = 1
        choices = set([0, self.revisions, self.revisions/4, self.revisions*3/4])

        while i < self.count:
            with open(self.logFile, 'w+') as executionLog:
                ref = random.choice(choices)
                query = "SELECT ? WHERE {{ graph <urn:bsbm> REVISION \"{}\" {{ ?s ?p ?o }} }} LIMIT 1".format(ref)
                start, end = self.postRequest(query)
                data = [ref, str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')
                print(' '.join(data))
                i = i + 1

    def runRawbaseBenchmark(self):
        if len(self.revisions) == 0:
            print('There are no revisions')
            return
        i = 0
        while i < self.count:
            with open(self.logFile, 'w+') as executionLog:
                ref = random.choice(self.revisions)
                start, end = self.postRequest(ref)
                data = [ref, str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')
                i = i + 1
