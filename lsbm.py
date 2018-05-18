#!/usr/bin/env python3

import sys
import math
import requests
import argparse
from random import seed, randint, sample

class lsbm:

    def __init__(self, baseUri, defaultGraph):
        self.baseUri = baseUri
        self.defaultGraph = defaultGraph

    def prepare(self, numberOfResources):
        self.toInsert = set([self.baseUri + str(r) for r in set(range(numberOfResources))])
        self.toDelete = set()
        self.prepareQueryList()

    def prepareQueryList(self):
        self.queryList = []
        while True:
            direction = randint(0,1)
            try:
                if direction:
                    self.queryList.append(self.prepareInsert())
                else:
                    self.queryList.append(self.prepareDelete())
            except ValueError as e:
                if len(self.toInsert) < 1 and len(self.toDelete) < 1:
                    print("all done")
                    break
        for query in self.queryList:
            print(query)

    def prepareInsert(self):
        queryBody = ""
        for resource in sample(self.toInsert, randint(1, math.ceil(len(self.toInsert)/2))):
            queryBody += '''<{resource}> <urn:pred> "objectA", "objectB" ;
                           <http://someother/pred> <http://example.org/obj> .
            '''.format(resource=resource)
            self.toInsert.remove(resource)
            self.toDelete.add(resource)
            #print("add {}".format(resource))
        if self.defaultGraph:
            query = "INSERT DATA {{GRAPH <{graph}> {{ {body} }}}}".format(
                graph=self.defaultGraph, body=queryBody)
        else:
            query = "INSERT DATA {{ {body} }}".format(body=queryBody)

        return query

    def prepareDelete(self):
        queryBody = ""
        for resource in sample(self.toDelete, randint(1, math.ceil(len(self.toDelete)/2))):
            queryBody += '''<{resource}> <urn:pred> "objectA", "objectB" ;
                           <http://someother/pred> <http://example.org/obj> .
            '''.format(resource=resource)
            self.toDelete.remove(resource)
            #print("del {}".format(resource))
        if self.defaultGraph:
            query = "DELETE DATA {{GRAPH <{graph}> {{ {body} }}}}".format(
                graph=self.defaultGraph, body=queryBody)
        else:
            query = "DELETE DATA {{ {body} }}".format(body=queryBody)
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
        type=int,
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
        '-b',
        '--baseUri',
        type=str,
        default="http://example.org/",
        help='The base URI for the test resources')
    parser.add_argument(
        '-d',
        '--defaultGraph',
        type=str,
        default=None,
        help='The default graphs URI for the test (default None)')
    parser.add_argument(
        '-n',
        '--numberOfResources',
        type=int,
        default='10',
        help='The of resources to generate')
    args = parser.parse_args(sys.argv[1:])
    print('Args', args)

    # https://stackoverflow.com/questions/11526975/set-random-seed-programwide-in-python#11527011
    seed(args.seed)

    lsbm = lsbm(args.baseUri, args.defaultGraph)
    #lsbm.rwbaseGetParent()

    lsbm.prepare(args.numberOfResources)
    lsbm.run(args.endpoint, args.endpointType, args.rwb_virtuoso)
