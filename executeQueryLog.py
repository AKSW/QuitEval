#!/usr/bin/env python3
import psutil
import argparse
import requests
import sys
import os
import datetime
import time
import threading
from subprocess import check_output


class QueryLogExecuter:
    logFile = ''
    commits = []

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
        queries = []
        if self.mode.lower() == 'bsbm-log':
            if os.path.isfile(self.queryLog):
                write = False
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
                            if len(queries) == self.count:
                                break
                        elif write is True:
                            query.append(line)
        elif self.mode.lower() == 'dataset_update':
            query = []
            delete_triples = 0
            patterns = {'insert': {
                            'quit': 'INSERT DATA {{GRAPH <urn:bsbm> {{ {} }} }}',
                            'r43ples': 'INSERT DATA {GRAPH <urn:bsbm> REVISION "master" INSERT DATA {{ {} }}',
                            'rawbase': 'INSERT DATA {{ {} }} '},
                        'delete': {
                            'quit': 'DELETE DATA {{GRAPH <urn:bsbm> {{ {} }} }}',
                            'r43ples': 'DELETE DATA {GRAPH <urn:bsbm> REVISION "master" {{ {} }}',
                            'rawbase': 'DELETE DATA {{ {} }}'}}

            queryType = 'insert'
            with open(self.queryLog, 'r') as f:
                for i, line in enumerate(f):
                    if len(queries)+1 > self.count/2:
                        break
                    line = line.strip()
                    query.append(line)
                    if i != 0 and i % self.triples == 0:
                        queries.append(patterns[queryType][self.store].format(' '.join(query)))
                        query = []

            queryType = 'delete'
            with open(self.queryLog, 'r') as f:
                for i, line in enumerate(f):
                    if len(queries)+1 > self.count:
                        break
                    line = line.strip()
                    query.append(line)
                    if i != 0 and i % self.triples == 0:
                        queries.append(patterns[queryType][self.store].format(' '.join(query)))
                        query = []

        if len(queries) < self.count:
            print('Did not get enough queries. Found {} queries'.format(len(queries)))
            sys.exit()
        else:
            print('Found {} queries'.format(len(queries)))
            self.queries = queries

    def runQueries(self):
        if self.store == 'rawbase':
            self.runQueriesRawbase()
        else:
            self.runQueriesRest()

    def runQueriesRest(self):
        for query in self.queries:
            with open(self.logFile, 'a') as executionLog:
                start, end = self.postRequest(query)
                data = [str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')

    def runQueriesRawbase(self):
        for query in self.queries:
            with open(self.logFile, 'a') as executionLog:
                start, end = self.rawbaseRequest(query)
                data = [str(end - start), str(start), str(end)]
                executionLog.write(' '.join(data) + '\n')

    def postRequest(self, query):
        start = datetime.datetime.now()
        res = requests.post(
            self.endpoint,
            data={'query': query},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        return start, end

    def rwbaseGetParent(self):
        query = "prefix prov: <http://www.w3.org/ns/prov#> select ?entity where {graph <urn:rawbase:provenance> {?entity a prov:Entity. ?activity prov:generated ?entity ; prov:atTime ?time}} order by desc(?time) limit 1"

        response = requests.post(self.virtuoso, data={'query': query},
                                 headers={'Accept': 'text/csv'})

        if len(response.text.split("\n")) > 0:
            parent = response.text.split("\n")[1].strip("\"")
            return parent
        return ""

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


class MonitorThread(threading.Thread):
    """The Monitor Thread.

    Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition.
    """

    def __init__(self, logFile='memory.log', logDir='.'):
        self.logDir = logDir
        self.logFile = os.path.join(self.logDir, logFile)
        super(MonitorThread, self).__init__()
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def setstoreProcessAndDirectory(self, pid, observedDir='.', logDir='var/logs', logFile='memory.log'):
        # self.process = process
        print(pid, observedDir, logDir, logFile)
        self.PID = pid
        self.observedDir = observedDir

    def run(self):
        print("Start monitor on pid: {} in directory: {}".format(self.PID, self.observedDir))
        psProcess = psutil.Process(int(self.PID))
        du = 0
        mem = 0

        while not self.stopped():
            with open(self.logFile, 'a') as memoryLog:
                timestamp = float(round(time.time() * 1000) / 1000)
                try:
                    mem = float(psProcess.memory_info().rss) / 1024
                except Exception as exc:
                    print("Monitor exception: mem", exc)
                try:
                    du = self.get_size(self.observedDir)
                except Exception as exc:
                    print("Monitor exception: du {}".format(str(exc)))
                    try:
                        du = self.get_size(self.observedDir)
                    except Exception as exc:
                        print("Monitor exception failed again: du {}".format(str(exc)))
                        print("using old value for du {}".format(str(du)))
                memoryLog.write("{} {} {}\n".format(timestamp, du, mem))
                time.sleep(1)
        print("Monitor stopped")


    def get_size(self, start_path='.'):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            total_size += os.path.getsize(dirpath)
            # self.logger.debug("size {} of {}".format(os.path.getsize(dirpath), dirpath))
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
                # self.logger.debug("size {} of {}".format(os.path.getsize(fp), fp))
        return total_size / 1024


def getPID(name):
    return int(check_output(["pidof", name]))

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
        help='The process id of the process to be monitored')

    parser.add_argument(
        '-Q',
        '--querylog',
        type=str,
        default='run.log',
        help='The link where to find a bsbm run log benchmark')

    parser.add_argument(
        '-M',
        '--mode',
        type=str,
        default='bsbm-log',
        help='The mode the log will be parsed. Chose between "bsbm-log" or "dataset_update".')

    parser.add_argument(
        '-S',
        '--store',
        type=str,
        help='Queries will be serialized for "quit", "r43ples" or "rawbase"')

    parser.add_argument(
        '-C',
        '--count',
        type=int,
        default=1000,
        help='The total number of queries that will be executed.')

    parser.add_argument(
        '-V',
        '--virtuoso',
        type=str,
        default='http://localhost:8890/sparql',
        help='The total number of queries that will be executed.')

    parser.add_argument(
        '-T',
        '--triples',
        type=int,
        default=40,
        help='The number of triples a insert/delete data query will use.')

    return parser.parse_args()


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    print('Args', args)

    exe = QueryLogExecuter(
        endpoint=args.endpoint,
        logDir=args.logdir,
        logFile=now + '_execution.log',
        mode=args.mode,
        count=args.count,
        store=args.store,
        virtuoso=args.virtuoso,
        triples=args.triples,
        queryLog=args.querylog)

    exe.initQueryLog()

    if args.processid:
        mon = MonitorThread(logDir=args.logdir, logFile=now + '_memory.log')

        mon.setstoreProcessAndDirectory(
            pid=args.processid,
            observedDir=args.observeddir)
        mon.start()

    print('Starting Benchmark')
    exe.runQueries()
    print('Benchmark finished')

    if args.processid:
        mon.stop()
