#!/usr/bin/env python3

import sys
import os
import signal
import yaml
import subprocess
import shlex
import time
import datetime
from memory_profiler import memory_usage, LogFile
import psutil
import threading

class BSQBMRunner:
    executionQueue = []

    def run(self, block = False):
        for execution in self.executionQueue:
            execution.run(block)
            if (block):
                execution.terminate()

    def addExecutionToQueue(self, execution):
        self.executionQueue.append(execution)

    def terminate(self):
        print("Terminate all executions (", len(self.executionQueue),")")
        for execution in self.executionQueue:
            execution.terminate()
            execution = None

class Execution:

    running = False

    runName = None
    quitExecutable = None
    bsbmLocation = None
    bsbmWarmup = None
    bsbmRuns = None
    repositoryPath = None
    logPath = None
    quitArgs = None

    def run(self, block = False):

        print ("start scenario {} with configuration:".format(self.runName))
        print ("quit:", self.quitExecutable)
        print ("bsbm:", self.bsbmLocation)
        print ("bsbm config: runs={} warmup={}".format(self.bsbmRuns, self.bsbmWarmup))
        print ("repositoryPath:", self.repositoryPath)
        print ("logPath:", self.logPath)
        print ("args:", self.quitArgs)
        print ()

        return

        self.running = True
        os.makedirs(self.logPath, exist_ok=True)
        self.runQuit()
        monitor = threading.Thread(target=self.runMonitor, args=(self.quitProcess, self.repositoryPath))
        monitor.start()
        print("Monitor started")
        # self.runMonitor()
        time.sleep(10)
        self.runBSBM()
        if (block):
            self.bsbmProcess.wait()

    def runQuit(self):
        quitArgs = shlex.split(self.quitArgs)
        print("Start", self.quitExecutable, "with", quitArgs, ": (", [self.quitExecutable] + quitArgs, ")")
        self.quitProcess = subprocess.Popen([self.quitExecutable, "-t", self.repositoryPath] + quitArgs)
        # "mprof", "run", "--multiprocess",
        print(self.quitProcess.pid)

    def runMonitor(self, process, directory):
        print("Start monitor on pid: {} in direcotry: {}".format(self.quitProcess.pid, self.repositoryPath))
        # self.memory_log = open("memory_log.txt", "w", encoding="utf-8")
        # self.mem_usage = memory_usage(self.quitProcess.pid, interval=.1, include_children=True, stream=self.memory_log)
        # print(self.mem_usage)
        # sys.stdout = LogFile('memory_profile_log')
        reslog = open(self.logPath + "resources-mem.log", "w")
        psProcess = psutil.Process(process.pid)
        while(process.poll() == None):
            # timestamp = datetime.datetime.now()
            timestamp = float(round(time.time() * 1000)/1000)
            mem = psProcess.memory_info().rss
            #du = os.statvfs(directory)
            du = self.get_size(directory)
            reslog.write(str(timestamp) + " " + str(du) + " " + str(mem) + "\n")
            time.sleep(1)
        reslog.close()

    def get_size(self, start_path = '.'):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            total_size += os.path.getsize(dirpath)
            # print(str(os.path.getsize(dirpath)), dirpath)
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
                # print(str(os.path.getsize(fp)), fp)
        return total_size/1024

    def runBSBM(self):
        arguments = "http://localhost:5000/sparql -runs {} -w {} -dg \"urn:bsbm\" -o {} -ucf usecases/exploreAndUpdate/sparql.txt -udataset dataset_update.nt -u http://localhost:5000/sparql".format(
            self.bsbmRuns,
            self.bsbmWarmup,
            os.path.abspath(self.logPath + self.runName + ".xml")
        )
        self.bsbmArgs = shlex.split(arguments)
        print("Start BSBM in", self.bsbmLocation, "with", arguments)

        self.bsbmProcess = subprocess.Popen(["./testdriver"] + self.bsbmArgs, cwd=self.bsbmLocation)
        print(self.bsbmProcess.pid)

    def __del__(self):
        print("Destructor called for", self.quitProcess.pid, "and", self.bsbmProcess.pid)
        self.terminate()

    def terminate(self):
        if self.running:
            # print(self.mem_usage)
            #self.memory_log.close()
            self.terminateProcess(self.bsbmProcess)
            # mv bsbm/run.log $QUIT_EVAL_DIR/$LOGDIR/$RUNDIR-run.log
            if (os.path.exists(self.bsbmLocation + "run.log")):
                os.rename(self.bsbmLocation + "run.log", self.logPath + self.runName + "-run.log")
            self.terminateProcess(self.quitProcess)
            self.running = False

    def terminateProcess(self, process):
        retVal = process.poll()
        if retVal == None:
            process.terminate()
            try:
                process.wait(3)
                retVal = process.poll()
                print("Terminated", process.pid, "(exited with:", str(retVal) + ")")
            except subprocess.TimeoutExpired:
                process.kill()
                retVal = process.poll()
                print("Killed", process.pid, "(exited with:", str(retVal) + ")")
        else:
            print("Already exited", process.pid, "(exited with:", str(retVal) + ")")

def getScenarioPathFunction(runName, runDirectory, runConfig):
    def scenarioPathFunction(key, default):
        basePath = runConfig[key] if key in runConfig else default
        if os.path.isabs(basePath):
            return os.path.join(basePath, runName)
        else:
            return os.path.abspath(os.path.join(runDirectory, basePath))
    return scenarioPathFunction

def main(scenarioPath):
    """Start the BSQBM."""

    def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')

        runner.terminate()

        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    print(scenarioPath)

    stream = open(scenarioPath, "r")
    docs = yaml.safe_load(stream)

    bsbmLocation = docs["bsbmLocation"]
    quitExecutable = docs["quitExecutable"]

    repetitions = docs["repetitions"] if "repetitions" in docs else "3"
    bsbmRuns = docs["bsbmRuns"] if "bsbmRuns" in docs else "100"
    bsbmWarmup = docs["bsbmWarmup"] if "bsbmWarmup" in docs else "5"

    resultDirectory = os.path.abspath(docs["resultDirectory"])
    repositoryBasePath = docs["repositoryBasePath"] if "repositoryBasePath" in docs else "repo"
    logBasePath = docs["logBasePath"] if "logBasePath" in docs else "logs"

    bareRepo = docs["bareRepo"]

    runner = BSQBMRunner()

    for scenario in docs["scenarios"]:
        print(scenario.items())
        for runName, runConfig in scenario.items():

            # these lines could go into a factory
            execution = Execution()
            execution.quitExecutable = quitExecutable
            execution.bsbmLocation = bsbmLocation
            execution.bsbmRuns = bsbmRuns
            execution.bsbmWarmup = bsbmWarmup

            # these parameters are individual per scenario
            execution.runName = runName
            runDirectory = os.path.join(resultDirectory, runName)
            getScenarioPath = getScenarioPathFunction(runName, runDirectory, runConfig)

            execution.repositoryPath = getScenarioPath("repositoryBasePath", repositoryBasePath)
            execution.logPath = getScenarioPath("logBasePath", logBasePath)

            storeArguments = runConfig["storeArguments"] if "storeArguments" in runConfig else ""
            execution.quitArgs = storeArguments

            runner.addExecutionToQueue(execution)

    # start benchmarks
    runner.run(block = True)

if __name__ == '__main__':

    if (len(sys.argv) < 2) :
        print("You need to specify a scenario")
        sys.exit(1)

    scenarioPath = sys.argv[1]
    main(scenarioPath)
