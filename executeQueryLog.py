#!/usr/bin/env python3
import psutil
import logging
import random
import argparse
import requests
import sys
import pygit2
import os
import datetime
import time
import threading
from subprocess import check_output

class QueryLogExecuter:
    logFile = ''
    commits = []

    QUERY = """
        SELECT * WHERE { graph ?g { ?s ?p ?o . ?o ?pp ?oo .}} LIMIT 10"""

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
            response = requests.post(endpoint, data={'query': self.QUERY}, headers={'Accept': 'application/json'})
        except Exception:
            raise Exception('Cannot access {}'.endpoint)

        if response.status_code == 200:
            pass
        else:
            raise Exception('Something wrong with sparql endpoint.')

        try:
            with open(self.logFile, 'w') as f:
                pass
            f.close()
            self.logFile = logFile
        except Exception:
            raise Exception('Can\'t write file {}'.format(logFile))

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
        logging.basicConfig(format='%(message)s', filename=self.logFile, level=logging.INFO)
        for query in self.queries:
            start, end = self.postRequest(query)
            data = [str(end - start), str(start), str(end)]
            print(' '.join(data))
            logging.info(' '.join(data))

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


    def __init__(self):
        super(MonitorThread, self).__init__()
        self.logFile = logFile

        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def setstoreProcessAndDirectory(self, pid, observedDir='.', logDir='var/logs', logFile='memory.log'):
        # self.process = process
        self.PID = pid
        self.observedDir = observedDir
        self.logDir = logDir
        self.logFile = logFile

    def run(self):
        print("Start monitor on pid: {} in directory: {}".format(self.PID, self.observedDir))
        logging.basicConfig(
            format='%(message)s',
            filename=os.path.join(self.logDir, self.logFile),
            level=logging.INFO)
        psProcess = psutil.Process(int(self.PID))
        du = 0
        mem = 0

        while not self.stopped():
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
                    self.logger.debug("Monitor exception failed again: du {}".format(str(exc)))
                    self.logger.debug("using old value for du {}".format(str(du)))
            reslog.write("{} {} {}\n".format(timestamp, du, mem))
            time.sleep(1)
        print("Monitor stopped")
    try:
        with open(os.path.join(self.logPath, "resources-mem.log"), "a") as reslog:
            timestamp = float(round(time.time() * 1000) / 1000)
            try:
                mem = float(psProcess.memory_info().rss) / 1024
            except psutil.NoSuchProcess:
                mem = 0
            try:
                du = self.get_size(self.observedDir)
            except Exception as exc:
                du = 0
            reslog.write("{} {} {}\n".format(timestamp, du, mem))
    except Exception as exc:
        print("Monitor exception when writing the last line: {}".format(str(exc)))
    print("Monitor Run finished and all resources are closed")


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


def getProcess(name):
    pid = getPID(name)
    process = psutil.Process(int(pid))
    print(process)
    return(process)

def getPID(name):
    return check_output(["pidof", name])

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
        '--observedDir',
        default='.',
        help='The directory that should be monitored')

    parser.add_argument(
        '-P',
        '--process',
        default='java',
        help='The command name of the process to be monitored')

    parser.add_argument(
        '-Q',
        '--querylog',
        type=str,
        default='run.log',
        help='The link where to find a bsbm run log benchmark')

    return parser.parse_args()


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    now = str(datetime.datetime.now())
    exe = QueryLogExecuter(
        endpoint=args.endpoint,
        logDir=args.logdir,
        logFile=now + '_execution.log',
        queryLog=args.querylog)

    mon = MonitorThread()
    mon.setstoreProcessAndDirectory(
        pid=getPID(args.process),
        observedDir=args.observeddir,
        logDir=args.logdir,
        logFile=now + '_memory.log')
    mon.start()
    exe.runQueries()
    mon.stop()
