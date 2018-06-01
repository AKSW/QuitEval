#!/usr/bin/env python3
import random
import requests
import sys
import pygit2
import os
import datetime


class Evaluator:
    """A super class containg attributes and methods used in both subclasses."""

    graph = 'urn:bsbm'
    store = ''
    endpoint = ''
    logFile = ''
    logDir = '/var/logs'
    queries = 10

    def postRequest(self, query, ref=None):
        start = datetime.datetime.now()
        res = requests.post(
            self.endpoint.format(revision=ref),
            data={'query': query},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        return start, end, res.status_code

    def getRequest(self, query, ref=None):
        start = datetime.datetime.now()
        res = requests.get(
            self.endpoint,
            params={'query': query},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        return start, end, res.status_code

    def rawbaseQueryRequest(self, query, ref=None):
        start = datetime.datetime.now()
        res = requests.post(
            self.endpoint,
            data={'query': query},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        return start, end, res.status_code

    def rawbaseUpdateRequest(self, query):
        start = datetime.datetime.now()
        parent = self.rwbaseGetParent()
        params = {"rwb-version": parent}
        res = requests.post(
            self.endpoint,
            params=params,
            data=query,
            headers={'Accept': 'application/json', "Content-Type": "application/sparql-update"})
        end = datetime.datetime.now()
        return start, end, res.status_code


class QueryLogExecuter(Evaluator):
    """A class that will execute generated queries for a given platform."""

    def __init__(
            self,
            endpoint='',
            virtuoso=None,
            logFile='execution.log',
            logDir='/var/logs',
            queryLog='',
            queryLogSeed='default',
            mode='bsbm-log',
            store=None,
            maxTriplesPerQuery=150,
            triples=None):

        self.mode = mode
        self.endpoint = endpoint
        self.virtuoso = virtuoso
        self.queryLog = queryLog
        self.queryLogSeed = queryLogSeed
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
        lsbm_instance.prepare(self.triples, self.queryLog, self.queryLogSeed)

        self.queries = lsbm_instance.queryList

    def run(self, requestMethod):
        with open(self.logFile, 'a+') as executionLog:
            number = 0
            for query_type, query in self.queries:
                number += 1
                start, end, status = requestMethod(query)
                execTime = str(end.timestamp()-start.timestamp())
                execTimeInsert = "NaN"
                execTimeDelete = "NaN"
                if query_type == "insert":
                    execTimeInsert = execTime
                if query_type == "delete":
                    execTimeDelete = execTime
                data = [number, execTimeInsert, execTimeDelete, str(start), str(end), str(status)]
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
            i = 0
            self.revisions = []
            for commit in self.repo.walk(self.repo.head.target, pygit2.GIT_SORT_TIME):
                self.revisions.append((i, str(commit.id)))
                i += 1
            print('Found {} git commits'.format(len(self.revisions)))
            with open(os.path.join(self.logDir, 'revisions.log'), 'a+') as revisionsLog:
                revisionsLog.write("{store}: {revisions}".format(store="quit", revisions=str(len(self.revisions))))
        elif self.store == 'r43ples':
            query = """select ?rev where {{
                graph <{}-revisiongraph> {{
                    ?s <http://eatld.et.tu-dresden.de/rmo#revisionNumber> ?rev .}} }} ORDER BY ?rev""".format(
                self.graph)

            response = requests.post(self.endpoint, data={'query': query},
                                     headers={'Accept': 'application/json'})

            data = response.json()
            self.revisions = list((r, r) for r in range(0, len(data['results']['bindings']) - 1))
            print('Found {} R43ples-revisions'.format(len(self.revisions)))
            with open(os.path.join(self.logDir, 'revisions.log'), 'a+') as revisionsLog:
                revisionsLog.write("{store}: {revisions}".format(store="r43ples", revisions=str(len(self.revisions))))

        elif self.store == 'rawbase':
            query = "prefix prov: <http://www.w3.org/ns/prov#> select ?entity where {graph <urn:rawbase:provenance> {?entity a prov:Entity. ?activity prov:generated ?entity ; prov:atTime ?time}} order by ?time"

            response = requests.post(self.virtuoso, data={'query': query},
                                     headers={'Accept': 'text/csv'})

            if len(response.text.split("\n")) > 0:
                revisions = response.text.split("\n")
                i = 0
                for line in revisions[1:]:
                    self.revisions.append((i, line.strip("\"")))
                    i += 1
                print('Found {} rawbase revisions'.format(len(self.revisions)))
                with open(os.path.join(self.logDir, 'revisions.log'), 'a+') as revisionsLog:
                    revisionsLog.write("{store}: {revisions}".format(store="rawbase", revisions=str(len(self.revisions))))

    def run(self, requestMethod):
        if len(self.revisions) == 0:
            print('There are no revisions')
            return
        elif len(self.revisions) < self.queries:
            print('There are not enough revisions')
            return

        selectedRevisions = random.sample(self.revisions, self.queries)

        limit = 1000
        query = {'quit': 'SELECT ?s ?p ?o WHERE {{GRAPH <{graph}> {{?s ?p ?o}}}} LIMIT {limit}',
                 'r43ples': 'SELECT ?s ?p ?o WHERE {{GRAPH <{graph}> REVISION "{revision}" {{?s ?p ?o}}}} LIMIT {limit}',
                 'rawbase': 'SELECT ?s ?p ?o FROM <{revision}> WHERE {{?s ?p ?o}} LIMIT {limit}'}

        # quit
        with open(self.logFile, 'w+') as executionLog:
            for number, ref in selectedRevisions:
                start, end, status = requestMethod(query[self.store].format(limit=limit, revision=str(ref), graph=self.graph), ref)
                data = [str(number), '"{}"'.format(str(ref)), str(end.timestamp()-start.timestamp()), str(start), str(end), str(status)]
                print(', '.join(data))
                executionLog.write(' '.join(data) + '\n')
