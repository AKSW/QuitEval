#!/usr/bin/env python3

import sys
import math
import requests
import argparse
from random import seed, randint, sample

class lsbm:

    query_patterns = {
        'quit': '{query_type} {{GRAPH <{graph}> {{ {body} }} }}',
        'r43ples': 'USER "radtke" MESSAGE "RASBM" {query_type} {{GRAPH <{graph}> REVISION "master" {{{body}}}}}',
        'rawbase': '{query_type} {{ {body} }}'}

    def __init__(self, defaultGraph, store, maxTriplesPerQuery):
        self.defaultGraph = defaultGraph
        self.store = store
        self.maxTriplesPerQuery = maxTriplesPerQuery
        self.stats = []

    def prepare(self, numberOfStatements, queryLog, randSeed='default'):
        # https://stackoverflow.com/questions/11526975/set-random-seed-programwide-in-python#11527011
        seed(randSeed)
        self.toInsert = []
        with open(queryLog, 'r') as f:
            for line in f:
                if len(self.toInsert) >= numberOfStatements:
                    break
                if line.strip() == "#__SEP__":
                    continue
                line = line.strip()
                self.toInsert.append(line)
        self.toDelete = []
        self.prepareQueryList()

    def prepareQueryList(self):
        self.queryList = []
        while True:
            direction = randint(0, 1)
            try:
                if direction == 0:
                    self.queryList.append(("insert", self.prepareDelete()))
                else:
                    self.queryList.append(("delete", self.prepareInsert()))
                self.stats.append((len(self.queryList), len(self.toDelete)))
                if len(self.queryList)%100 == 0:
                    print("querylist: {}, toInsert: {}, toDelete: {}".format(
                        str(len(self.queryList)), str(len(self.toInsert)), str(len(self.toDelete))))
                if len(self.toInsert) < 1 and len(self.toDelete) < 1:
                    print("all done")
                    break
            except ValueError as e:
                pass
        print("done prepare query list")

    def removeListFromList(self, orig, remove):
        return list((item for item in orig if item not in remove))

    def prepareInsert(self):
        if math.ceil(len(self.toInsert)/4) < self.maxTriplesPerQuery:
            maxTripleSize = math.ceil(len(self.toInsert)/4)
        else:
            maxTripleSize = self.maxTriplesPerQuery
        statementSample = sample(self.toInsert, randint(1, maxTripleSize))
        self.toInsert = self.removeListFromList(self.toInsert, statementSample)
        self.toDelete.extend(statementSample)
        query = self.query_patterns[self.store].format(
            query_type='INSERT DATA', graph=self.defaultGraph,
            body=" ".join(statementSample))

        return query

    def prepareDelete(self):
        if math.ceil(len(self.toDelete)/4) < self.maxTriplesPerQuery:
            maxTripleSize = math.ceil(len(self.toDelete)/4)
        else:
            maxTripleSize = self.maxTriplesPerQuery
        statementSample = sample(self.toDelete, randint(1, maxTripleSize))
        self.toDelete = self.removeListFromList(self.toDelete, statementSample)
        query = self.query_patterns[self.store].format(
            query_type='DELETE DATA', graph=self.defaultGraph,
            body=" ".join(statementSample))

        return query

    def rwbaseGetParent(self, rwbVirtuoso):
        print("get parent commit")
        #query = "prefix prov: <http://www.w3.org/ns/prov#> select ?activity where {graph <urn:rawbase:provenance> {?activity a prov:Activity; prov:atTime ?time}} order by desc(?time) limit 1"
        query = "prefix prov: <http://www.w3.org/ns/prov#> select ?entity where {graph <urn:rawbase:provenance> {?entity a prov:Entity. ?activity prov:generated ?entity ; prov:atTime ?time}} order by desc(?time) limit 1"

        response = requests.post(rwbVirtuoso, data={'query': query},
                                 headers={'Accept': 'text/csv'})

        if len(response.text.split("\n")) > 0:
            return response.text.split("\n")[1].strip("\"")
        return ""

    def run(self, endpoint, endpointType=None, rwbVirtuoso=None):

        for query in self.queryList:
            params = {}

            if endpointType == "rwb":
                parent = self.rwbaseGetParent(rwbVirtuoso)
                params["rwb-version"] = parent

            print("exec: {} with params: {}".format(query, params))
            response = requests.post(endpoint, data=query, params=params,
                                     headers={'Accept': 'application/json', "Content-Type": "application/sparql-update"})

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s',
        '--seed',
        type=str,
        default='0',
        help='The seed for the random generator')
    parser.add_argument(
        '-e',
        '--endpoint',
        type=str,
        default=None,
        help='The SPARQL Endpoint to test')
    parser.add_argument(
        '-et',
        '--endpointType',
        type=str,
        default=None,
        help='The SPARQL Endpoint type none or "rwb"')
    parser.add_argument(
        '--rwb-virtuoso',
        type=str,
        default=None,
        help='The Virtuoso SPARQL Endpoint of R&Wbase')
    parser.add_argument(
        '-d',
        '--defaultGraph',
        type=str,
        default=None,
        help='The default graphs URI for the test (default None)')
    parser.add_argument(
        '-st',
        '--storeType',
        type=str,
        default="quit",
        help='The type of store, for which the querylog should be created. (quit (*), rawbase, r43ples)')
    parser.add_argument(
        '--maxTriplesPerQuery',
        type=int,
        default=300,
        help='The maximal number of tripels added or deleted in a single query.')
    parser.add_argument(
        '-q',
        '--queryLog',
        type=str,
        help='The path to the query log file.')
    parser.add_argument(
        '-n',
        '--numberOfStatements',
        type=int,
        default='100000',
        help='The total numer of statements to insert and delete with the querylog')
    args = parser.parse_args(sys.argv[1:])
    print('Args', args)

    lsbm_instance = lsbm(args.defaultGraph, args.storeType, args.maxTriplesPerQuery)
    #lsbm.rwbaseGetParent()

    lsbm_instance.prepare(args.numberOfStatements, args.queryLog, args.seed)
    # lsbm.run(args.endpoint, args.endpointType, args.rwb_virtuoso)
    revision = 0
    for revision, stat in lsbm_instance.stats:
        # revision += 1
        print("{}: {}".format(str(revision), str(stat)))
