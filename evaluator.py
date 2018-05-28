#!/usr/bin/env python3
import random
import requests
import sys
import pygit2
import os
import datetime


class Evaluator:
    """A super class containg attributes and methods used in both subclasses."""

    store=''
    endpoint=''
    logFile=''
    logDir='/var/logs'
    queries=10

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

    def rawbaseQueryRequest(self, query):
        start = datetime.datetime.now()
        # params = {"rwb-version": ref}
        res = requests.post(
            self.endpoint,
            data={'query': query},
            # params=params,
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        return start, end

    def rawbaseUpdateRequest(self, query):
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
            endpoint='',
            virtuoso=None,
            logFile='execution.log',
            logDir='/var/logs',
            queryLog='',
            mode='bsbm-log',
            store=None,
            maxTriplesPerQuery=150,
            triples=None):

        self.mode = mode
        self.endpoint = endpoint
        self.virtuoso = virtuoso
        self.queryLog = queryLog
        self.logDir = logDir
        self.logFile = os.path.join(self.logDir, logFile)
        self.mode = mode
        self.store = store
        self.maxTriplesPerQuery = maxTriplesPerQuery
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
        lsbm_instance = lsbm("http://example.org/", "urn:bsbm", self.store, self.maxTriplesPerQuery)
        lsbm_instance.prepare(self.triples, self.queryLog)

        self.queries = lsbm_instance.queryList

    def run(self):
        if self.store == 'rawbase':
            self.runRawbase()
        else:
            self.runRest()
        return len(self.queries)

    def runRest(self):
        with open(self.logFile, 'a+') as executionLog:
            for query in self.queries:
                start, end = self.postRequest(query)
                data = [str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')

    def runRawbase(self):
        with open(self.logFile, 'a+') as executionLog:
            for query in self.queries:
                start, end = self.rawbaseUpdateRequest(query)
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


class RandomAccessExecuter(Evaluator):
    """Execute Select Queries randomly over existing reviosions."""

    logFile = ''
    commits = []
    revisions = []

    def __init__(
            self,
            store='',
            repo='',
            graph='urn:bsbm',
            endpoint='',
            logFile='',
            virtuoso='',
            logDir='/var/logs',
            queries=10):

        self.endpoint = endpoint
        self.virtuoso = virtuoso
        self.logDir = logDir
        self.graph = graph

        if store not in ['quit', 'r43ples', 'rawbase']:
            print('No store selected.')
            sys.exit()

        if logFile != '':
            self.logFile = os.path.join(self.logDir, logFile)
        else:
            self.logFile = os.path.join(self.logDir, 'ra-' + store + '.log')

        if isinstance(queries, int):
            self.queries = queries
        else:
            raise Exception('Expect integer for argument "queries", got {}, {}'.format(queries, type(queries)))

        self.store = store

        if store == 'quit':
            try:
                self.repo = pygit2.Repository(repo)
            except Exception:
                raise Exception('{} is no repository'.format(repo))

    def getRevisions(self):
        if self.store == 'quit':
            self.getQuitRevisions()
        elif self.store == 'r43ples':
            self.getR43plesRevisions()
        elif self.store == 'rawbase':
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

        response = requests.post(self.virtuoso, data={'query': query},
                                 headers={'Accept': 'text/csv'})

        if len(response.text.split("\n")) > 0:
            revisions = response.text.split("\n")
            for line in revisions[1:]:
                self.revisions.append(line.strip("\""))

    def run(self):
        if self.store == 'quit':
            self.runQuitBenchmark()
        elif self.store == 'r43ples':
            self.runR43plesBenchmark()
        elif self.store == 'rawbase':
            self.runRawbaseBenchmark()

    def runQuitBenchmark(self):
        if len(self.commits) == 0:
            print('There are no revisions')
            return
        i = 0
        query = """
            SELECT * WHERE { graph ?g { ?s ?p ?o .}} LIMIT 1000"""

        with open(self.logFile, 'w+') as executionLog:
            while i < self.queries:
                ref = random.choice(self.commits)
                start, end = self.postRequest(query, ref)
                data = [ref, str(end - start), str(start), str(end)]
                print(', '.join(data))
                executionLog.write(' '.join(data) + '\n')
                i = i + 1

    def runR43plesBenchmark(self):
        if self.revisions <= 0:
            print('There are no revisions')
            return

        i = 0
        with open(self.logFile, 'w+') as executionLog:
            while i < self.queries:
                ref = random.choice(range(0, self.revisions))
                query = "SELECT * WHERE {{ graph <urn:bsbm> REVISION \"{}\" {{ ?s ?p ?o }} }} LIMIT 1000".format(ref)
                start, end = self.postRequest(query)
                data = [str(ref), str(end - start), str(start), str(end)]
                print(', '.join(data))
                executionLog.write(' '.join(data) + '\n')
                i = i + 1

    def runRawbaseBenchmark(self):

        if len(self.revisions) == 1:
            print('There are no revisions')
            return
        i = 0
        with open(self.logFile, 'w+') as executionLog:
            while i < self.queries:
                ref = random.choice(self.revisions)
                query = """
                    SELECT * FROM <{}> WHERE {{ ?s ?p ?o .}} LIMIT 1000""".format(ref)
                start, end = self.rawbaseQueryRequest(query)
                data = [ref, str(end - start), str(start), str(end)]
                print(', '.join(data))
                executionLog.write(' '.join(data) + '\n')
                i = i + 1
