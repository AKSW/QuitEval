#!/usr/bin/env python3

import sys
import os
import signal
import shutil
import yaml
import subprocess
import shlex
import time
import psutil
import threading
import pygit2
import logging
from urllib import parse
from urllib import request

logger = logging.getLogger('quit-eval')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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

    def run(self, block=False):
        if not self.prepared:
            raise Exception("The Run was not prepared")
        for execution in self.executionQueue:
            execution.run(block)
            if (block):
                execution.terminate()

    def addExecutionsToQueue(self, executions):
        self.executionQueue += executions
        self.logger.debug(
            "Execution Queue now contains: ".format(self.executionQueue))

    def terminate(self):
        self.logger.debug("Terminate all executions ({})".format(
            len(self.executionQueue)))
        for execution in self.executionQueue:
            execution.terminate()
            execution = None


class MonitorThread(threading.Thread):
    """The Monitor Thread.

    Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition.
    """

    logger = logging.getLogger('quit-eval.monitor')

    def __init__(self):
        super(MonitorThread, self).__init__()
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def setstoreProcessAndDirectory(self, process, repositoryPath, logPath):
        self.process = process
        self.repositoryPath = repositoryPath
        self.logPath = logPath

    def run(self):
        self.logger.debug("Start monitor on pid: {} in directory: {}".format(
            self.process.pid, self.repositoryPath))
        with open(os.path.join(self.logPath, "resources-mem.log"), "a") as reslog:
            psProcess = psutil.Process(self.process.pid)
            du = 0
            mem = 0
            while(self.process.poll() is None and not self.stopped()):
                timestamp = float(round(time.time() * 1000) / 1000)
                try:
                    mem = float(psProcess.memory_info().rss) / 1024
                except Exception as exc:
                    self.logger.debug("Monitor exception: mem", exc)
                try:
                    du = self.get_size(self.repositoryPath)
                except Exception as exc:
                    self.logger.debug("Monitor exception: du {}".format(str(exc)))
                    try:
                        du = self.get_size(self.repositoryPath)
                    except Exception as exc:
                        self.logger.debug("Monitor exception failed again: du {}".format(str(exc)))
                        self.logger.debug("using old value for du {}".format(str(du)))
                reslog.write("{} {} {}\n".format(timestamp, du, mem))
                time.sleep(1)
            self.logger.debug(
                "Monitor for {} on {} stopped, reason: process.poll() = {}; self.stopped() = {}"
                .format(self.process.pid, self.repositoryPath, self.process.poll(), self.stopped()))
        try:
            with open(os.path.join(self.logPath, "resources-mem.log"), "a") as reslog:
                timestamp = float(round(time.time() * 1000) / 1000)
                try:
                    mem = float(psProcess.memory_info().rss) / 1024
                except psutil.NoSuchProcess:
                    mem = 0
                try:
                    du = self.get_size(self.repositoryPath)
                except Exception as exc:
                    du = 0
                reslog.write("{} {} {}\n".format(timestamp, du, mem))
        except Exception as exc:
            self.logger.warning("Monitor exception when writing the last line: {}".format(str(exc)))
        self.logger.debug("Monitor Run finished and all resources are closed")


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


class Execution:
    logger = logging.getLogger('quit-eval.execution')

    running = False

    usecase = 'exploreAndUpdate'
    two_graphs = False
    runName = None
    executable = None
    wsgimodule = None
    pythonpath = None
    bsbmLocation = None
    bsbmWarmup = None
    bsbmRuns = None
    logPath = None
    storeArguments = None
    profiling = False

    def prepare_repository(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(os.path.join(self.bsbmLocation, "dataset.nt"), 'r') as sourceGraph:
            with open(os.path.join(directory, "data.nq"), 'w') as targetGraph:
                for line in sorted(list(sourceGraph)):
                    targetGraph.write(line.rstrip()[:-1] + "<urn:bsbm> .\n")

        if self.two_graphs:
            with open(os.path.join(self.bsbmLocation, "dataset.nt"), 'r') as sourceGraph:
                with open(os.path.join(directory, "graph2.nq"), 'w') as targetGraph:
                    for line in sorted(list(sourceGraph)):
                        targetGraph.write(line.rstrip()[:-1] + "<urn:bsbm2> .\n")


    def terminate(self):
        self.logger.debug("Terminate has been called on execution")
        if self.running:
            # self.logger.debug(self.mem_usage)
            # self.memory_log.close()
            if hasattr(self, "bsbmProcess"):
                self.terminateProcess(self.bsbmProcess)
            # mv bsbm/run.log $QUIT_EVAL_DIR/$LOGDIR/$RUNDIR-run.log
            if (os.path.exists(os.path.join(self.bsbmLocation, "run.log"))):
                os.rename(os.path.join(self.bsbmLocation, "run.log"),
                          os.path.join(self.logPath, self.runName + "-run.log"))
            if hasattr(self, "storeProcess"):
                self.terminateProcess(self.storeProcess)
            self.logger.debug("Call monitor.stop()")
            self.monitor.stop()
            self.logger.debug("monitor.stop() called")
            self.monitor.join()
            self.logger.debug("monitor.join() finished")
            self.running = False

    def terminateProcess(self, process):
        retVal = process.poll()
        if retVal is None:
            process.terminate()
            try:
                process.wait(10)
                retVal = process.poll()
                self.logger.debug(
                    "Terminated {} (exited with: {})".format(process.pid, retVal))
            except subprocess.TimeoutExpired:
                process.kill()
                retVal = process.poll()
                self.logger.debug(
                    "Killed {} (exited with: {})".format(process.pid, retVal))
        else:
            self.logger.debug(
                "Already exited {} (exited with: {})".format(process.pid, retVal))

    def __del__(self):
        if self.running:
            self.logger.debug("Destructor called for {} and {}".format(
                self.storeProcess.pid, self.bsbmProcess.pid))
            self.terminate()


class R43plesExecution(Execution):

    repositoryPath = None

    def prepare(self):

        self.logger.debug(
            "prepare scenario \"{}\" with configuration:".format(self.runName))
        self.logger.debug("store executable: {}".format(self.executable))
        self.logger.debug("bsbm: {}".format(self.bsbmLocation))
        self.logger.debug("bsbm config: runs={} warmup={}".format(
            self.bsbmRuns, self.bsbmWarmup))
        self.logger.debug("args: {}".format(self.storeArguments))
        self.logger.debug("profiling: {}".format(self.profiling))
        self.logger.debug("repositoryPath: {}".format(self.repositoryPath))

        os.makedirs(self.logPath, exist_ok=True)
        os.makedirs(self.repositoryPath, exist_ok=True)

        self.prepare_repository(self.repositoryPath)

    def runBSBM(self):
        arguments = "{} -runs {} -w {} -dg \"urn:bsbm\" -o {} -ucf {} -udataset {} -u {}".format(
            "http://localhost:8080/r43ples/sparql",
            self.bsbmRuns,
            self.bsbmWarmup,
            os.path.abspath(os.path.join(self.logPath, self.runName + ".xml")),
            os.path.join("usecases", self.usecase, "r43ples.sparql.txt"),
            "dataset_update.nt",
            "http://localhost:8080/r43ples/sparql"
        )
        self.bsbmArgs = shlex.split(arguments)
        self.logger.debug("Start BSBM in {} with {}".format(
            self.bsbmLocation, arguments))

        self.bsbmProcess = subprocess.Popen(
            ["./testdriver"] + self.bsbmArgs, cwd=self.bsbmLocation)
        self.logger.debug(
            "BSBM Process ID is: {}".format(self.bsbmProcess.pid))


class QuitExecution(Execution):

    repositoryPath = None
    bareRepo = None

    def prepare(self):

        self.logger.debug(
            "prepare scenario \"{}\" with configuration:".format(self.runName))
        self.logger.debug("quit: {}".format(self.executable))
        self.logger.debug("wsgimodule: {}".format(self.wsgimodule))
        self.logger.debug("pythonpath: {}".format(self.pythonpath))
        self.logger.debug("bsbm: {}".format(self.bsbmLocation))
        self.logger.debug("bsbm config: runs={} warmup={}".format(
            self.bsbmRuns, self.bsbmWarmup))
        self.logger.debug("repositoryPath: {}".format(self.repositoryPath))
        self.logger.debug("logPath: {}".format(self.logPath))
        self.logger.debug("args: {}".format(self.storeArguments))
        self.logger.debug("bareRepo: {}".format(self.bareRepo))
        self.logger.debug("profiling: {}".format(self.profiling))

        os.makedirs(self.logPath, exist_ok=True)
        os.makedirs(self.repositoryPath, exist_ok=True)

        if self.bareRepo:
            self.prepare_repository()
        else:
            self.prepare_repository(self.repositoryPath)

    def prepare_repository(self, directory):
        repo = pygit2.init_repository(directory)  # git init $directory
        configttl = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), "stuff", "config.ttl")

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

        if self.two_graphs:
            with open(os.path.join(self.bsbmLocation, "dataset.nt"), 'r') as sourceGraph:
                with open(os.path.join(directory, "graph2.nq"), 'w') as targetGraph:
                    for line in sorted(list(sourceGraph)):
                        targetGraph.write(line.rstrip()[:-1] + "<urn:bsbm2> .\n")
            with open(os.path.join(directory, "graph2.nq.graph"), 'w') as targetGraphDotGraph:
                targetGraphDotGraph.write("urn:bsbm2\n")

            index.add("graph2.nq")
            index.add("graph2.nq.graph")

            configttl = os.path.join(os.path.dirname(
                os.path.abspath(__file__)), "stuff", "config.two_graphs.ttl")

        shutil.copy(configttl, os.path.join(self.repositoryPath, "config.ttl"))

        index.add("config.ttl")
        index.write()
        tree = index.write_tree()
        author = pygit2.Signature("bsqbm", "bsqbm@experiment.example.org")
        commiter = author
        oid = repo.create_commit(
            "HEAD", author, commiter, "init for bsqbm", tree, [])
        # self.logger.debug(
        #   "try to creat tag for {} {} {} {}".format(type(oid), oid, str(oid), str(oid)[:5])
        # )
        # git tag init-graph
        # repo.create_tag("init-graph", str(oid)[:5], pygit2.GIT_OBJ_BLOB, author, "init-graph\n")

    def run(self, block=False):

        self.logger.debug("start scenario {}".format(self.runName))

        self.running = True
        self.runStore()
        self.monitor = MonitorThread()
        self.monitor.setstoreProcessAndDirectory(
            self.storeProcess, self.repositoryPath, self.logPath)
        self.monitor.start()
        time.sleep(20)
        self.runBSBM()
        if (block):
            self.bsbmProcess.wait()
        self.logger.debug("Run has finished")

    def getStoreCommand(
        self,
        target=None,
        mode='localconfig',
        config=None
    ):
        storeArguments = shlex.split(self.storeArguments)
        if not target:
            target = self.repositoryPath
        if not config:
            config = os.path.join(self.repositoryPath, 'config.ttl')

        return [self.executable, "-cm", mode, "-c", config, "-t", target] + storeArguments

    def runStore(self):
        storeArguments = shlex.split(self.storeArguments)
        if self.profiling:
            quitCommand = ["python", "-m", "cProfile", "-o",
                           os.path.join(self.logPath, "profile_data.pyprof")]
        else:
            quitCommand = []
        quitCommand += [self.executable, "-cm", "localconfig", "-c", os.path.join(
            self.repositoryPath, "config.ttl"), "-t", self.repositoryPath] + storeArguments
        self.logger.debug("Start quit: {}".format(quitCommand))
        self.storeProcess = subprocess.Popen(quitCommand)
        self.logger.debug("Quit process is: {}".format(self.storeProcess.pid))

    def runBSBM(self):
        arguments = "{} -runs {} -w {} -dg \"urn:bsbm\" -o {} -ucf {} -udataset {} -u {}".format(
            "http://localhost:5000/sparql",
            self.bsbmRuns,
            self.bsbmWarmup,
            os.path.abspath(os.path.join(self.logPath, self.runName + ".xml")),
            os.path.join("usecases", self.usecase, "quit.sparql.txt"),
            "dataset_update.nt",
            "http://localhost:5000/sparql"
        )
        self.bsbmArgs = shlex.split(arguments)
        self.logger.debug("Start BSBM in {} with {}".format(
            self.bsbmLocation, arguments))

        self.bsbmProcess = subprocess.Popen(
            ["./testdriver"] + self.bsbmArgs, cwd=self.bsbmLocation)
        self.logger.debug(
            "BSBM Process ID is: {}".format(self.bsbmProcess.pid))


class UwsgiExecution(QuitExecution):

    def runStore(self):
        storeArguments = shlex.split(self.storeArguments)
        arguments = ["-cm", "localconfig", "-c", os.path.join(self.repositoryPath, "config.ttl"),
                     "-t", self.repositoryPath] + storeArguments
        argumentString = " ".join(arguments)
        uwsgiCommand = ["uwsgi", "--http", "0.0.0.0:5000", "--pythonpath", self.pythonpath,
                        "-w", self.wsgimodule, "--pyargv", "\"{}\"".format(argumentString)]
        self.logger.debug("Start quit with uwsgi: {}".format(uwsgiCommand))
        self.storeProcess = subprocess.Popen(uwsgiCommand)
        self.logger.debug("Uwsgi process is: {}".format(self.storeProcess.pid))


class QuitOldExecution(QuitExecution):

    def prepare_repository(self, directory):
        repo = pygit2.init_repository(directory)  # git init $directory
        configttl = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), "stuff", "config_old.ttl")

        # sed "s/.$/<urn:bsbm> ./g" $BSBM_DIR/dataset.nt | LC_ALL=C sort -u > $REPOSITORY/graph.nq
        with open(os.path.join(self.bsbmLocation, "dataset.nt"), 'r') as sourceGraph:
            with open(os.path.join(directory, "graph.nq"), 'w') as targetGraph:
                for line in sorted(list(sourceGraph)):
                    targetGraph.write(line.rstrip()[:-1] + "<urn:bsbm> .\n")

        index = repo.index
        index.read()
        index.add("graph.nq")

        shutil.copy(configttl, os.path.join(self.repositoryPath, "config.ttl"))

        index.add("config.ttl")
        index.write()
        tree = index.write_tree()
        author = pygit2.Signature("bsqbm", "bsqbm@experiment.example.org")
        commiter = author
        oid = repo.create_commit(
            "HEAD", author, commiter, "init for bsqbm", tree, [])

    def runStore(self):
        storeArguments = shlex.split(self.storeArguments)
        # quit-store --pathspec
        quitCommand = [self.executable] + storeArguments
        self.logger.debug("Start quit: {} in {}".format(quitCommand, self.repositoryPath))
        self.storeProcess = subprocess.Popen(quitCommand, cwd=self.repositoryPath)
        self.logger.debug("Quit process is: {}".format(self.storeProcess.pid))

class QuitDockerExecution(QuitExecution):
    logger = logging.getLogger('quit-eval.docker_execution')

    running = False

    image = 'aksw/quitstore'
    portMappings = ['5000:5000']
    envVariables = []

    def run(self, block=False):

        self.logger.debug("start scenario {}".format(self.runName))
        self.hostTargetDir = self.repositoryPath
        self.repositoryPath = '/data'

        self.running = True
        self.runStore()
        self.monitor = MonitorThread()
        self.monitor.setstoreProcessAndDirectory(
            self.storeProcess, self.repositoryPath, self.logPath)
        self.monitor.start()
        time.sleep(20)
        self.runBSBM()
        if (block):
            self.bsbmProcess.wait()
        self.logger.debug("Run has finished")

    def runStore(self):
        self.volumeMounts = [self.hostTargetDir + ':' + self.repositoryPath]

        if self.profiling:
            self.logger.info('Profiling not implemented for docker environment, yet.')
            dockerCommand = []
        else:
            dockerCommand = []

        dockerCommand += ['docker', 'run', '--name', 'bsbm.docker']
        for portMapping in self.portMappings:
            dockerCommand += ['-p', portMapping]
        for volumeMount in self.volumeMounts:
            dockerCommand += ['-v', volumeMount]
        for envVariable in self.envVariables:
            dockerCommand += ['-e', envVariable]
        dockerCommand += ['-i', '--rm', self.image]
        dockerCommand += self.getStoreCommand()
        self.logger.debug("Start quit container: {}".format(dockerCommand))
        print(' '.join(dockerCommand))
        self.storeProcess = subprocess.Popen(dockerCommand)
        self.logger.debug("Quit docker process is: {}".format(self.storeProcess.pid))
        self.repositoryPath = self.hostTargetDir


class R43plesDockerExecution(R43plesExecution):
    logger = logging.getLogger('quit-eval.docker_execution')

    running = False

    dataDir = ''
    graph = 'urn:bsbm'
    image = 'aksw/r43ples'
    portMappings = ['8080:80']
    volumeMounts = [dataDir + ':/var/r43ples/data']
    envVariables = ['GRAPH_URI=' + graph]

    def run(self, block=False):

        self.logger.debug("start scenario {}".format(self.runName))
        self.hostTargetDir = self.repositoryPath
        self.repositoryPath = '/var/r43ples/data'

        self.running = True
        self.runStore()
        time.sleep(25)
        self.postPrepare('urn:bsbm')
        time.sleep(2)
        self.postPrepare('urn:bsbm2')
        time.sleep(2)
        self.monitor = MonitorThread()
        self.monitor.setstoreProcessAndDirectory(
            self.storeProcess, self.repositoryPath, self.logPath)
        self.monitor.start()
        time.sleep(5)
        self.runBSBM()
        if (block):
            self.bsbmProcess.wait()
        self.logger.debug("Run has finished")

    def runStore(self):
        self.volumeMounts = [self.hostTargetDir + ':' + self.repositoryPath]

        if self.profiling:
            self.logger.info('Profiling not implemented for docker environment, yet.')
            dockerCommand = []
        else:
            dockerCommand = []

        dockerCommand += ['docker', 'run', '--name', 'bsbm.docker']
        for portMapping in self.portMappings:
            dockerCommand += ['-p', portMapping]
        for volumeMount in self.volumeMounts:
            dockerCommand += ['-v', volumeMount]
        for envVariable in self.envVariables:
            dockerCommand += ['-e', envVariable]
        dockerCommand += ['--rm', '-t', self.image]
        self.logger.debug("Start r43ples container: {}".format(' '.join(dockerCommand)))
        self.storeProcess = subprocess.Popen(dockerCommand)
        self.logger.debug("R43ples docker process is: {}".format(self.storeProcess.pid))
        self.repositoryPath = self.hostTargetDir

    def postPrepare(self, graphuri):
        arguments = parse.urlencode({'query': 'CREATE GRAPH <' + graphuri + '>'})
        conn = request.urlopen('http://localhost:8080/r43ples/sparql', (arguments.encode('utf-8')))

    def terminate(self):
        self.logger.debug("Terminate has been called on execution")
        if self.running:
            self.logger.debug('Trying to stop container')
            subprocess.Popen(['docker', 'rm', '-f', 'bsbm.docker'])
            time.sleep(2)
            self.logger.debug('Container stopped')
            # self.logger.debug(self.mem_usage)
            # self.memory_log.close()
            if hasattr(self, "bsbmProcess"):
                self.terminateProcess(self.bsbmProcess)
            # mv bsbm/run.log $QUIT_EVAL_DIR/$LOGDIR/$RUNDIR-run.log
            if (os.path.exists(os.path.join(self.bsbmLocation, "run.log"))):
                os.rename(os.path.join(self.bsbmLocation, "run.log"),
                          os.path.join(self.logPath, self.runName + "-run.log"))
            if hasattr(self, "storeProcess"):
                self.terminateProcess(self.storeProcess)
            self.logger.debug("Call monitor.stop()")
            self.monitor.stop()
            self.logger.debug("monitor.stop() called")
            self.monitor.join()
            self.logger.debug("monitor.join() finished")
            self.running = False

    def __del__(self):
        if self.running:
            self.logger.debug("Destructor called for {} and {}".format(
                self.storeProcess.pid, self.bsbmProcess.pid))
            self.terminate()


class ScenarioReader:

    logger = logging.getLogger('quit-eval.scenarioreader')

    def readScenariosFromDir(self, runDir):
        scenarioPath = os.path.join(runDir, "scenario.yml")
        if not os.path.exists(scenarioPath):
            raise Exception(
                "There is no index of scenarios, looking for {}".format(scenarioPath))

        stream = open(scenarioPath, "r")
        docs = yaml.safe_load(stream)

        return ScenarioReader().readScenarios(docs, runDir)

    def readScenarios(self, docs, basePath):

        generalConfig = {}
        scenarios = []

        resultDirectory = os.path.abspath(
            os.path.join(basePath, docs["resultDirectory"]))
        generalConfig["resultDirectory"] = resultDirectory

        bsbmLocation = docs["bsbmLocation"]
        executable = None
        wsgimodule = None
        pythonpath = None
        if "executable" in docs:
            executable = docs["executable"]
        elif "wsgimodule" in docs and "pythonpath" in docs:
            wsgimodule = docs["wsgimodule"]
            pythonpath = docs["pythonpath"]
        else:
            raise Exception("Don't now what to run in scenario: {}".format(resultDirectory))

        repetitions = docs["repetitions"] if "repetitions" in docs else "3"
        bsbmRuns = docs["bsbmRuns"] if "bsbmRuns" in docs else "100"
        bsbmWarmup = docs["bsbmWarmup"] if "bsbmWarmup" in docs else "5"

        repositoryBasePath = docs["repositoryBasePath"] if "repositoryBasePath" in docs else "repo"
        logBasePath = docs["logBasePath"] if "logBasePath" in docs else "logs"

        bareRepo = docs["bareRepo"] if "bareRepo" in docs else False
        profiling = docs["profiling"] if "profiling" in docs else False
        docker = docs["docker"] if "docker" in docs else False
        two_graphs = docs["two_graphs"] if "two_graphs" in docs else False
        usecase = docs["usecase"] if "usecase" in docs else False

        for repetition in range(1, repetitions + 1):
            for scenario in docs["scenarios"]:
                self.logger.debug(
                    "scenario items: {}".format(scenario.items()))
                for runName, runConfig in scenario.items():

                    runName = runName + "-" + str(repetition)

                    # these lines could go into a factory
                    scenario_docker = runConfig[
                        "docker"] if "docker" in runConfig else False

                    if scenario_docker in ['r43ples', 'quit', 'oldquit', 'uwsgi']:
                        container = scenario_docker
                    else:
                        container = docker

                    image = runConfig["image"] if ("image") in runConfig else False
                    tg = runConfig["two_graphs"] if ("two_graphs") in runConfig else False
                    uc = runConfig["usecase"] if ("usecase") in runConfig else False

                    if container == 'r43ples':
                        execution = R43plesDockerExecution()
                        if image:
                            execution.image = image
                    elif container == 'quit':
                        execution = QuitDockerExecution()
                        if image:
                            execution.image = image
                    elif container == 'oldquit':
                        execution = QuitOldExecution()
                    elif container == "uwsgi":
                        execution = UwsgiExecution()
                    else:
                        execution = QuitExecution()

                    execution.bsbmLocation = bsbmLocation
                    execution.bsbmRuns = bsbmRuns
                    execution.bsbmWarmup = bsbmWarmup

                    # these parameters are individual per scenario
                    runDirectory = os.path.join(
                        resultDirectory, "quit-" + runName)
                    getScenarioPath = self.__getScenarioPathFunction(
                        "quit-" + runName, runDirectory, runConfig)

                    execution.runName = "quit-" + runName

                    if uc:
                        execution.usecase = uc
                    elif usecase:
                        execution.usecase = usecase

                    if tg:
                        execution.two_graphs = uc
                    elif two_graphs:
                        execution.two_graphs = two_graphs

                    execution.executable = runConfig[
                        "executable"] if "executable" in runConfig else executable
                    execution.wsgimodule = runConfig[
                        "wsgimodule"] if "wsgimodule" in runConfig else wsgimodule
                    execution.pythonpath = runConfig[
                        "pythonpath"] if "pythonpath" in runConfig else pythonpath
                    execution.repositoryPath = getScenarioPath(
                        "repositoryBasePath", repositoryBasePath)
                    execution.logPath = getScenarioPath(
                        "logBasePath", logBasePath)
                    execution.storeArguments = runConfig["storeArguments"] if (
                        "storeArguments") in runConfig else ""
                    execution.bareRepo = runConfig["bareRepo"] if (
                        "bareRepo") in runConfig else bareRepo
                    execution.profiling = runConfig["profiling"] if (
                        "profiling") in runConfig else profiling

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

    try:
        stream = open(scenarioPath, "r")
        docs = yaml.safe_load(stream)
    except FileNotFoundError:
        logger.error('Can not create stream. Path not found: ' + scenarioPath)
        sys.exit(1)

    generalConfig, scenarios = ScenarioReader().readScenarios(
        docs, os.path.dirname(scenarioPath))

    if os.path.exists(generalConfig["resultDirectory"]):
        logger.error(
            "The result directory ({}) already exists, please provide a new location".format(
                generalConfig["resultDirectory"]))
        sys.exit(1)

    os.makedirs(generalConfig["resultDirectory"])

    logfile = os.path.join(generalConfig["resultDirectory"], "scenario.log")

    try:
        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.debug('Logfile: ' + logfile)
    except FileNotFoundError:
        logger.error('Logfile not found: ' + logfile)
    except PermissionError:
        logger.error('Can not create logfile: ' + logfile)

    logger.info("Use scenario configuration from: {}".format(scenarioPath))

    runner = BSQBMRunner()
    runner.addExecutionsToQueue(scenarios)

    with open(
        os.path.join(generalConfig["resultDirectory"], "scenario.yml"), "w"
    ) as resultScenario:
        docs["resultDirectory"] = "."
        resultScenario.write(yaml.dump(docs))

    # shutil.copy(scenarioPath, )

    # start benchmarks
    runner.prepare()
    runner.run(block=True)


if __name__ == '__main__':

    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    if (len(sys.argv) < 2):
        logger.error("You need to specify a scenario")
        sys.exit(1)

    scenarioPath = sys.argv[1]
    main(scenarioPath)
