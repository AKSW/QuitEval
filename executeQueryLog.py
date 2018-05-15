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
            queryLog=''):

        self.endpoint = endpoint
        self.queryLog = queryLog
        self.logDir = logDir
        self.logFile = os.path.join(self.logDir, logFile)

        try:
            response = requests.post(endpoint, data={'query': 'SELECT * WHERE {?s ?p ?o} LIMIT 1'}, headers={'Accept': 'application/json'})
        except Exception:
            raise Exception('Cannot access {}'.endpoint)

        if response.status_code == 200:
            pass
        else:
            raise Exception('Something wrong with sparql endpoint.')

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

        # print('Found {} queries'.format(len(queries)))
        self.queries = queries

    def runQueries(self):
        for query in self.queries:
            with open(self.logFile, 'a') as executionLog:
                start, end = self.postRequest(query)
                data = [str(end - start), str(start), str(end)]
                print(' '.join(data))
                executionLog.write(' '.join(data) + '\n')

    def postRequest(self, query):
        print('Executing query')
        start = datetime.datetime.now()
        res = requests.post(
            self.endpoint,
            data={'query': query},
            headers={'Accept': 'application/json'})
        end = datetime.datetime.now()
        print(res.status_code)
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
    # print("Monitor Run finished and all resources are closed")
    # try:
    #     timestamp = float(round(time.time() * 1000) / 1000)
    #     try:
    #         mem = float(psProcess.memory_info().rss) / 1024
    #     except psutil.NoSuchProcess:
    #         mem = 0
    #         try:
    #             du = self.get_size(self.observedDir)
    #         except Exception as exc:
    #             du = 0
    #             logging.info("{} {} {}\n".format(timestamp, du, mem))
    #         except Exception as exc:
    #             print("Monitor exception when writing the last line: {}".format(str(exc)))


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

    return parser.parse_args()


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    print('Args', args)

    exe = QueryLogExecuter(
        endpoint=args.endpoint,
        logDir=args.logdir,
        logFile=now + '_execution.log',
        queryLog=args.querylog)

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
