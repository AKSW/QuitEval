#!/usr/bin/env python3

import sys
import os
import signal
import shutil
import yaml
import subprocess
import shlex
import time
import datetime
import psutil
import threading
import pygit2

class BSQBMRunner:
    executionQueue = []
    prepared = False

    def prepare(self):
        for execution in self.executionQueue:
            execution.prepare()
        self.prepared = True

    def run(self, block = False):
        if not self.prepared:
            raise Exception("The Run was not prepared")
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
    bareRepo = None
    configGarbageCollection = None
    profiling = False

    def prepare(self):

        print ("prepare scenario \"{}\" with configuration:".format(self.runName))
        print ("quit:", self.quitExecutable)
        print ("bsbm:", self.bsbmLocation)
        print ("bsbm config: runs={} warmup={}".format(self.bsbmRuns, self.bsbmWarmup))
        print ("repositoryPath:", self.repositoryPath)
        print ("logPath:", self.logPath)
        print ("args:", self.quitArgs)
        print ("bareRepo:", self.bareRepo)
        print ("configGarbageCollection:", self.configGarbageCollection)
        print ("profiling:", self.profiling)
        print ()

        os.makedirs(self.logPath, exist_ok=True)
        os.makedirs(self.repositoryPath, exist_ok=True)

        if self.bareRepo:
            self.prepare_repository()
        else:
            self.prepare_repository(self.repositoryPath)

    def prepare_repository(self, directory):
        repo = pygit2.init_repository(directory) # git init $directory
        if self.configGarbageCollection:
            repo.config.set_multivar("gc.auto", "*", 257)
        #gitattributes = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "stuff", ".gitattributes")
        #shutil.copy(gitattributes, directory)
        #cp stuff/.gitattributes $directory/
        configttl= os.path.join(os.path.dirname(os.path.abspath(__file__)), "stuff", "config.ttl")
        shutil.copy(configttl, os.path.join(self.repositoryPath, "config.ttl"))

        # sed "s/.$/<urn:bsbm> ./g" $BSBM_DIR/dataset.nt | LC_ALL=C sort -u > $REPOSITORY/graph.nq
        with open(os.path.join(self.bsbmLocation, "dataset.nt"), 'r') as sourceGraph:
            with open(os.path.join(directory, "graph.nq"), 'w') as targetGraph:
                for line in sorted(list(sourceGraph)):
                    targetGraph.write(line.rstrip()[:-1] + "<urn:bsbm> .\n")

        with open(os.path.join(directory, "graph.nq.graph"), 'w') as targetGraphDotGraph:
                 targetGraphDotGraph.write("urn:bsbm\n")

        index = repo.index
        index.read()
        index.add("graph.nq")
        index.add("graph.nq.graph")
        index.add("config.ttl")
        index.write()
        tree = index.write_tree()
        author = pygit2.Signature("bsqbm", "bsqbm@experiment.example.org")
        commiter = author
        oid = repo.create_commit("HEAD", author, commiter, "init for bsqbm", tree, [])
        print(type(oid), oid, str(oid), str(oid)[:5])
        #repo.create_tag("init-graph", str(oid)[:5], pygit2.GIT_OBJ_BLOB, author, "init-graph\n") # git tag init-graph


    def run(self, block = False):

        print ("start scenario {}".format(self.runName))
        print ()

        # return

        self.running = True
        self.runQuit()
        monitor = threading.Thread(target=self.runMonitor, args=(self.quitProcess, self.repositoryPath))
        monitor.start()
        print("Monitor started")
        # self.runMonitor()
        time.sleep(20)
        self.runBSBM()
        if (block):
            self.bsbmProcess.wait()

    def runQuit(self):
        quitArgs = shlex.split(self.quitArgs)
        if self.profiling:
            quitCommand = ["python", "-m", "cProfile", "-o", os.path.join(self.logPath, "profile_data.pyprof")]
        else:
            quitCommand = []
        quitCommand += [self.quitExecutable, "-cm", "localconfig", "-c", os.path.join(self.repositoryPath, "config.ttl"), "-t", self.repositoryPath] + quitArgs
        print("Start quit:", quitCommand)
        self.quitProcess = subprocess.Popen(quitCommand)
        # "mprof", "run", "--multiprocess",
        print(self.quitProcess.pid)

    def runMonitor(self, process, directory):
        print("Start monitor on pid: {} in directory: {}".format(self.quitProcess.pid, self.repositoryPath))
        # self.memory_log = open("memory_log.txt", "w", encoding="utf-8")
        # self.mem_usage = memory_usage(self.quitProcess.pid, interval=.1, include_children=True, stream=self.memory_log)
        # print(self.mem_usage)
        # sys.stdout = LogFile('memory_profile_log')
        reslog = open(os.path.join(self.logPath, "resources-mem.log"), "w")
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
        arguments = "{} -runs {} -w {} -dg \"urn:bsbm\" -o {} -ucf usecases/exploreAndUpdate/sparql.txt -udataset dataset_update.nt -u {}".format(
            "http://localhost:5000/sparql",
            self.bsbmRuns,
            self.bsbmWarmup,
            os.path.abspath(os.path.join(self.logPath, self.runName + ".xml")),
            "http://localhost:5000/sparql"
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
            if hasattr(self, "bsbmProcess"):
                self.terminateProcess(self.bsbmProcess)
            # mv bsbm/run.log $QUIT_EVAL_DIR/$LOGDIR/$RUNDIR-run.log
            if (os.path.exists(os.path.join(self.bsbmLocation, "run.log"))):
                os.rename(os.path.join(self.bsbmLocation, "run.log"), os.path.join(self.logPath, self.runName + "-run.log"))
            if hasattr(self, "quitProcess"):
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

    print("Use scenario configuration from:", scenarioPath)

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

    bareRepo = docs["bareRepo"] if "bareRepo" in docs else False
    profiling = docs["profiling"] if "profiling" in docs else False
    configGarbageCollection = docs["configGarbageCollection"] if "configGarbageCollection" in docs else False

    runner = BSQBMRunner()

    for repetition in range(1, repetitions+1):
        for scenario in docs["scenarios"]:
            print(scenario.items())
            for runName, runConfig in scenario.items():

                runName = runName + "-" + str(repetition)

                # these lines could go into a factory
                execution = Execution()
                execution.bsbmLocation = bsbmLocation
                execution.bsbmRuns = bsbmRuns
                execution.bsbmWarmup = bsbmWarmup

                # these parameters are individual per scenario
                runDirectory = os.path.join(resultDirectory, "quit-" + runName)
                getScenarioPath = getScenarioPathFunction("quit-" + runName, runDirectory, runConfig)

                execution.runName = "quit-" + runName
                execution.quitExecutable = runConfig["quitExecutable"] if "quitExecutable" in runConfig else quitExecutable
                execution.repositoryPath = getScenarioPath("repositoryBasePath", repositoryBasePath)
                execution.logPath = getScenarioPath("logBasePath", logBasePath)
                execution.quitArgs = runConfig["storeArguments"] if "storeArguments" in runConfig else ""
                execution.bareRepo = runConfig["bareRepo"] if "bareRepo" in runConfig else bareRepo
                execution.profiling = runConfig["profiling"] if "profiling" in runConfig else profiling
                execution.configGarbageCollection = runConfig["configGarbageCollection"] if "configGarbageCollection" in runConfig else configGarbageCollection


                runner.addExecutionToQueue(execution)

    # start benchmarks
    runner.prepare()
    runner.run(block = True)

if __name__ == '__main__':

    if (len(sys.argv) < 2) :
        print("You need to specify a scenario")
        sys.exit(1)

    scenarioPath = sys.argv[1]
    main(scenarioPath)
