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
import logging

logger = logging.getLogger('quit-eval')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)


class BSQBMRunner:

    logger = logging.getLogger('quit-eval.bsqbmrunner')

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

    def addExecutionsToQueue(self, executions):
        self.executionQueue += executions
        self.logger.debug("Execution Queue now contains: ".format(self.executionQueue))

    def terminate(self):
        self.logger.debug("Terminate all executions ({})".format(len(self.executionQueue)))
        for execution in self.executionQueue:
            execution.terminate()
            execution = None

class MonitorThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    logger = logging.getLogger('quit-eval.monitor')

    def __init__(self):
        super(MonitorThread, self).__init__()
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def setQuitProcessAndDirectory(self, process, repositoryPath, logPath):
        self.process = process
        self.repositoryPath = repositoryPath
        self.logPath = logPath

    def run(self):
        self.logger.debug("Start monitor on pid: {} in directory: {}".format(self.process.pid, self.repositoryPath))
        with open(os.path.join(self.logPath, "resources-mem.log"), "a") as reslog:
            psProcess = psutil.Process(self.process.pid)
            while(self.process.poll() is None and not self.stopped()):
                timestamp = float(round(time.time() * 1000)/1000)
                mem = float(psProcess.memory_info().rss)/1024
                du = self.get_size(self.repositoryPath)
                reslog.write("{} {} {}\n".format(timestamp, du, mem))
                time.sleep(1)
            self.logger.debug("Monitor for {} on {} stopped, reason: process.poll() = {}; self.stopped() = {}".format(self.process.pid, self.repositoryPath, self.process.poll(), self.stopped()))

    def get_size(self, start_path = '.'):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            total_size += os.path.getsize(dirpath)
            # self.logger.debug("size {} of {}".format(os.path.getsize(dirpath), dirpath))
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
                # self.logger.debug("size {} of {}".format(os.path.getsize(fp), fp))
        return total_size/1024

class Execution:

    logger = logging.getLogger('quit-eval.execution')

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

        self.logger.debug("prepare scenario \"{}\" with configuration:".format(self.runName))
        self.logger.debug("quit: {}".format(self.quitExecutable))
        self.logger.debug("bsbm: {}".format(self.bsbmLocation))
        self.logger.debug("bsbm config: runs={} warmup={}".format(self.bsbmRuns, self.bsbmWarmup))
        self.logger.debug("repositoryPath: {}".format(self.repositoryPath))
        self.logger.debug("logPath: {}".format(self.logPath))
        self.logger.debug("args: {}".format(self.quitArgs))
        self.logger.debug("bareRepo: {}".format(self.bareRepo))
        self.logger.debug("configGarbageCollection: {}".format(self.configGarbageCollection))
        self.logger.debug("profiling: {}".format(self.profiling))

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
        configttl = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stuff", "config.ttl")
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
        #self.logger.debug("try to creat tag for {} {} {} {}".format(type(oid), oid, str(oid), str(oid)[:5]))
        #repo.create_tag("init-graph", str(oid)[:5], pygit2.GIT_OBJ_BLOB, author, "init-graph\n") # git tag init-graph


    def run(self, block = False):

        self.logger.debug("start scenario {}".format(self.runName))

        self.running = True
        self.runQuit()
        self.monitor = MonitorThread()
        self.monitor.setQuitProcessAndDirectory(self.quitProcess, self.repositoryPath, self.logPath)
        self.monitor.start()
        time.sleep(20)
        self.runBSBM()
        if (block):
            self.bsbmProcess.wait()
        self.logger.debug("Run has finished")

    def runQuit(self):
        quitArgs = shlex.split(self.quitArgs)
        if self.profiling:
            quitCommand = ["python", "-m", "cProfile", "-o", os.path.join(self.logPath, "profile_data.pyprof")]
        else:
            quitCommand = []
        quitCommand += [self.quitExecutable, "-cm", "localconfig", "-c", os.path.join(self.repositoryPath, "config.ttl"), "-t", self.repositoryPath] + quitArgs
        self.logger.debug("Start quit: {}".format(quitCommand))
        self.quitProcess = subprocess.Popen(quitCommand)
        self.logger.debug("Quit process is: {}".format(self.quitProcess.pid))

    def runBSBM(self):
        arguments = "{} -runs {} -w {} -dg \"urn:bsbm\" -o {} -ucf usecases/exploreAndUpdate/sparql.txt -udataset dataset_update.nt -u {}".format(
            "http://localhost:5000/sparql",
            self.bsbmRuns,
            self.bsbmWarmup,
            os.path.abspath(os.path.join(self.logPath, self.runName + ".xml")),
            "http://localhost:5000/sparql"
        )
        self.bsbmArgs = shlex.split(arguments)
        self.logger.debug("Start BSBM in {} with {}".format(self.bsbmLocation, arguments))

        self.bsbmProcess = subprocess.Popen(["./testdriver"] + self.bsbmArgs, cwd=self.bsbmLocation)
        self.logger.debug("BSBM Process ID is: {}".format(self.bsbmProcess.pid))

    def __del__(self):
        if self.running:
            self.logger.debug("Destructor called for {} and {}".format(self.quitProcess.pid, self.bsbmProcess.pid))
            self.terminate()

    def terminate(self):
        self.logger.debug("Terminate has been called on execution")
        if self.running:
            # self.logger.debug(self.mem_usage)
            #self.memory_log.close()
            if hasattr(self, "bsbmProcess"):
                self.terminateProcess(self.bsbmProcess)
            # mv bsbm/run.log $QUIT_EVAL_DIR/$LOGDIR/$RUNDIR-run.log
            if (os.path.exists(os.path.join(self.bsbmLocation, "run.log"))):
                os.rename(os.path.join(self.bsbmLocation, "run.log"), os.path.join(self.logPath, self.runName + "-run.log"))
            if hasattr(self, "quitProcess"):
                self.terminateProcess(self.quitProcess)
            self.monitor.stop()
            self.running = False

    def terminateProcess(self, process):
        retVal = process.poll()
        if retVal is None:
            process.terminate()
            try:
                process.wait(10)
                retVal = process.poll()
                self.logger.debug("Terminated {} (exited with: {})".format(process.pid, retVal))
            except subprocess.TimeoutExpired:
                process.kill()
                retVal = process.poll()
                self.logger.debug("Killed {} (exited with: {})".format(process.pid, retVal))
        else:
            self.logger.debug("Already exited {} (exited with: {})".format(process.pid, retVal))

class ScenarioReader:

    logger = logging.getLogger('quit-eval.scenarioreader')

    def readScenariosFromDir(self, runDir):
        scenarioPath = os.path.join(runDir, "scenario.yml")
        if not os.path.exists(scenarioPath):
            raise Exception("There is no index of scenarios, looking for {}".format(scenarioPath))

        stream = open(scenarioPath, "r")
        docs = yaml.safe_load(stream)

        return ScenarioReader().readScenarios(docs, runDir)

    def readScenarios(self, docs, basePath):

        generalConfig = {}
        scenarios = []

        resultDirectory = os.path.abspath(os.path.join(basePath, docs["resultDirectory"]))
        generalConfig["resultDirectory"] = resultDirectory

        bsbmLocation = docs["bsbmLocation"]
        quitExecutable = docs["quitExecutable"]

        repetitions = docs["repetitions"] if "repetitions" in docs else "3"
        bsbmRuns = docs["bsbmRuns"] if "bsbmRuns" in docs else "100"
        bsbmWarmup = docs["bsbmWarmup"] if "bsbmWarmup" in docs else "5"

        repositoryBasePath = docs["repositoryBasePath"] if "repositoryBasePath" in docs else "repo"
        logBasePath = docs["logBasePath"] if "logBasePath" in docs else "logs"

        bareRepo = docs["bareRepo"] if "bareRepo" in docs else False
        profiling = docs["profiling"] if "profiling" in docs else False
        configGarbageCollection = docs["configGarbageCollection"] if "configGarbageCollection" in docs else False

        for repetition in range(1, repetitions+1):
            for scenario in docs["scenarios"]:
                self.logger.debug("scenario items: {}".format(scenario.items()))
                for runName, runConfig in scenario.items():

                    runName = runName + "-" + str(repetition)

                    # these lines could go into a factory
                    execution = Execution()
                    execution.bsbmLocation = bsbmLocation
                    execution.bsbmRuns = bsbmRuns
                    execution.bsbmWarmup = bsbmWarmup

                    # these parameters are individual per scenario
                    runDirectory = os.path.join(resultDirectory, "quit-" + runName)
                    getScenarioPath = self.__getScenarioPathFunction("quit-" + runName, runDirectory, runConfig)

                    execution.runName = "quit-" + runName
                    execution.quitExecutable = runConfig["quitExecutable"] if "quitExecutable" in runConfig else quitExecutable
                    execution.repositoryPath = getScenarioPath("repositoryBasePath", repositoryBasePath)
                    execution.logPath = getScenarioPath("logBasePath", logBasePath)
                    execution.quitArgs = runConfig["storeArguments"] if "storeArguments" in runConfig else ""
                    execution.bareRepo = runConfig["bareRepo"] if "bareRepo" in runConfig else bareRepo
                    execution.profiling = runConfig["profiling"] if "profiling" in runConfig else profiling
                    execution.configGarbageCollection = runConfig["configGarbageCollection"] if "configGarbageCollection" in runConfig else configGarbageCollection

                    scenarios.append(execution)

        return generalConfig, scenarios

    def __getScenarioPathFunction(self, runName, runDirectory, runConfig):
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
        logger.info("Terminated with Ctrl+C")

        runner.terminate()

        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    stream = open(scenarioPath, "r")
    docs = yaml.safe_load(stream)

    generalConfig, scenarios = ScenarioReader().readScenarios(docs, os.path.dirname(scenarioPath))

    if os.path.exists(generalConfig["resultDirectory"]):
        logger.error("The result directory ({}) already exists, please provide an empty location".format(generalConfig["resultDirectory"]))
        sys.exit(1)

    os.makedirs(generalConfig["resultDirectory"])

    logfile = os.path.join(generalConfig["resultDirectory"], "scenario.log")

    try:
        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.debug('Logfile: '+ logfile)
    except FileNotFoundError:
        logger.error('Logfile not found: ' + logfile)
    except PermissionError:
        logger.error('Can not create logfile: ' + logfile)

    logger.info("Use scenario configuration from: {}".format(scenarioPath))

    runner = BSQBMRunner()
    runner.addExecutionsToQueue(scenarios)

    with open(os.path.join(generalConfig["resultDirectory"], "scenario.yml"), "w") as resultScenario:
        docs["resultDirectory"] = "."
        resultScenario.write(yaml.dump(docs))

    # shutil.copy(scenarioPath, )

    # start benchmarks
    runner.prepare()
    runner.run(block = True)

if __name__ == '__main__':

    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    if (len(sys.argv) < 2) :
        logger.error("You need to specify a scenario")
        sys.exit(1)

    scenarioPath = sys.argv[1]
    main(scenarioPath)
