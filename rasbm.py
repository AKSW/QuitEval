#! /usr/bin/env python3

import logging
import sys
import os
from bsqbm import Execution, ScenarioReader, BSQBMRunner, QuitDockerExecution, QuitExecution, main as bsqbmMain
from bsqbm import R43plesExecution, R43plesDockerExecution, RawbaseDockerExecution
from evaluator import QueryLogExecuter, RandomAccessExecuter

logger = logging.getLogger('quit-eval.rasbm')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)


class RARunner(BSQBMRunner):
    """Will be passed to bsqmMain."""

    pass


class RandomAccessExecution(Execution):
    """Execute Random Access Queries or a BSBM Query Log on supported Backends."""

    logger = logging.getLogger('quit-eval.rasbm.ra-execution')
    default_endpoints = {'query': {'quit': 'http://localhost:5000/sparql',
                                   'r43ples': 'http://localhost:8080/r43ples/sparql',
                                   'rawbase': 'http://localhost:8080/rawbase/sparql'},
                         'update': {'quit': 'http://localhost:5000/sparql',
                                    'r43ples': 'http://localhost:8080/r43ples/sparql',
                                    'rawbase': 'http://localhost:8080/rawbase/update'}}

    def runBSBM(self):
        self.logger.info('Starting RASBM with mode {}'.format(self.evalMode))
        if self.evalMode.lower() in ['ra', 'randomaccess', 'random-access']:
            self.runRandomAccess()
        elif self.evalMode.lower() in ['ql', 'querylog', 'query-log']:
            self.runQueryLog()
        elif self.evalMode.lower() in ['both', 'complete']:
            self.runQueryLog()
            self.runRandomAccess()

    def runQueryLog(self):
        self.logger.info("Query Log Execution")
        self.logger.info("store: {}".format(self.store))
        ql = QueryLogExecuter(
            endpoint=self.default_endpoints['update'][self.store],  # endpoint
            logDir=os.path.abspath(self.logPath),  # log dir
            queryLog=self.bsbmQueryLogFile,  # query log file
            queryLogSeed=self.bsbmQueryLogSeed,  # query log seed
            mode=self.bsbmLogMode,  # mode
            store=self.store,  # store
            triples=self.bsbmQueryLogTriples,  # amount of triples that will be picked
            maxTriplesPerQuery=self.rasbmMaxTriplesPerQuery,  # amount of triples that will be picked
            virtuoso=self.rasbmVirtuoso)  # virtuoso

        self.logger.debug("Start QueryLogExecuter for {}".format(
            self.store))
        ql.run()


    def runRandomAccess(self):
        self.logger.info("Random Access Execution")
        self.logger.info("Platform: {}".format(self.store))
        if self.store == 'quit':
            ra = RandomAccessExecuter(
                endpoint=self.default_endpoints['query'][self.store],  # endpoint
                store=self.store,  # store
                queries=self.rasbmQueryExecutions,  # number of queries
                logDir=os.path.abspath(self.logPath),  # log dir
                repo=self.repositoryPath)  # repodir
        elif self.store == 'r43ples':
            ra = RandomAccessExecuter(
                endpoint=self.default_endpoints['query'][self.store],  # endpoint
                store=self.store,  # store
                queries=self.rasbmQueryExecutions,  # number of queries
                logDir=os.path.abspath(self.logPath))  # log dir
        elif self.store == 'rawbase':
            self.repodir=os.path.abspath(os.path.join(self.logPath, 'repo')),  # log dir
            ra = RandomAccessExecuter(
                endpoint=self.default_endpoints['query'][self.store],  # endpoint
                store=self.store,  # store
                queries=self.rasbmQueryExecutions,  # number of queries
                virtuoso=self.rasbmVirtuoso,  # virtuoso
                logDir=os.path.abspath(self.logPath))  # log dir

        self.logger.debug("Start Random Access for {}".format(
            self.store))
        ra.getRevisions()
        ra.run()


    def __del__(self):
        if self.running:
            self.terminate()
            self.logger.debug("Destructor called for storeProces {}".format(
                self.storeProcess.pid))


class RaQuitExecution(RandomAccessExecution, QuitExecution):
    """A class with overloaded method."""

    pass


class RaR43plesExecution(RandomAccessExecution, R43plesDockerExecution):
    """A class with overloaded method."""

    pass


class RaRawbaseExecution(RandomAccessExecution, RawbaseDockerExecution):
    """A class with overloaded method."""

    pass


class RaQuitDockerExecution(RandomAccessExecution, QuitDockerExecution):
    """A class with overloaded method."""

    pass


class RaR43plesDockerExecution(RandomAccessExecution, R43plesDockerExecution):
    """A class with overloaded method."""

    pass


class RaRawbaseDockerExecution(RandomAccessExecution, RawbaseDockerExecution):
    """A class with overloaded method."""

    pass


class RaScenarioReader(ScenarioReader):
    """A yml file reader with more arguments than ScenarioReader."""

    logger = logging.getLogger('quit-eval.rasbm.scenarioreader')

    def readScenarios(self, docs, basePath):

        generalConfig = {}
        scenarios = []

        rasbmMode = {'r43ples': 'r43ples',
                     'r43plesdocker': 'r43ples',
                     'rawbase': 'rawbase',
                     'rawbasedocker': 'rawbase',
                     'quit': 'quit',
                     'quitdocker': 'quit',
                     'uwsgi': 'quit'}

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

        # New features of rasbm
        bsbmQueryLogFile = docs["bsbmQueryLogFile"] if "bsbmQueryLogFile" in docs else None
        bsbmQueryLogTriples = docs["bsbmQueryLogTriples"] if "bsbmQueryLogTriples" in docs else 40
        bsbmQueryLogSeed = docs["bsbmQueryLogSeed"] if "bsbmQueryLogSeed" in docs else 'default'
        bsbmLogMode = docs["bsbmLogMode"] if "bsbmLogMode" in docs else None
        evalMode = docs["evalMode"] if "evalMode" in docs else "ra"
        repoDir = docs["repoDir"] if "repoDir" in docs else None
        rasbmMaxTriplesPerQuery = docs["rasbmMaxTriplesPerQuery"] if "rasbmMaxTriplesPerQuery" in docs else 150
        rasbmQueryExecutions = docs["rasbmQueryExecutions"] if "rasbmQueryExecutions" in docs else 1000
        rasbmVirtuoso = docs["rasbmVirtuoso"] if "rasbmVirtuoso" in docs else 'http://localhost:8890/sparql'
        if "executionType" in docs:
            store = rasbmMode[docs["executionType"].lower()]

        repetitions = docs["repetitions"] if "repetitions" in docs else "3"
        bsbmRuns = docs["bsbmRuns"] if "bsbmRuns" in docs else "100"
        bsbmWarmup = docs["bsbmWarmup"] if "bsbmWarmup" in docs else "5"

        repositoryBasePath = docs["repositoryBasePath"] if "repositoryBasePath" in docs else "repo"
        logBasePath = docs["logBasePath"] if "logBasePath" in docs else "logs"

        bareRepo = docs["bareRepo"] if "bareRepo" in docs else False
        profiling = docs["profiling"] if "profiling" in docs else False
        docker = docs["docker"] if "docker" in docs else False
        executionType = docs["executionType"] if "executionType" in docs else "Quit"
        two_graphs = docs["two_graphs"] if "two_graphs" in docs else False
        usecase = docs["usecase"] if "usecase" in docs else False

        for repetition in range(1, repetitions + 1):
            for scenario in docs["scenarios"]:
                self.logger.debug(
                    "scenario items: {}".format(scenario.items()))
                for runName, runConfig in scenario.items():

                    runName = runName + "-" + str(repetition)

                    # these lines could go into a factory
                    scenario_docker = runConfig["docker"] if "docker" in runConfig else docker
                    executionType = runConfig["executionType"] if (
                        "executionType") in runConfig else executionType

                    if scenario_docker in self.dockerToExecution.keys() and executionType is None:
                        self.logger.info("Please, don't use 'docker' keyword!")
                        executionType = self.dockerToExecution[scenario_docker]

                    tg = runConfig["two_graphs"] if ("two_graphs") in runConfig else False
                    uc = runConfig["usecase"] if ("usecase") in runConfig else False

                    execution = getattr(sys.modules[__name__], 'Ra' + executionType + "Execution")()

                    execution.bsbmLocation = bsbmLocation
                    execution.bsbmRuns = bsbmRuns
                    execution.bsbmWarmup = bsbmWarmup

                    # these parameters are individual per scenario
                    runDirectory = os.path.join(
                        resultDirectory, "quit-" + runName)
                    getScenarioPath = self.getScenarioPathFunction(
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

                    # New RA features
                    execution.bsbmQueryLogTriples = runConfig[
                        "bsbmQueryLogTriples"] if "bsbmQueryLogTriples" in runConfig else bsbmQueryLogTriples
                    execution.bsbmQueryLogFile = runConfig[
                        "bsbmQueryLogFile"] if "bsbmQueryLogFile" in runConfig else bsbmQueryLogFile
                    execution.bsbmQueryLogSeed = runConfig[
                        "bsbmQueryLogSeed"] if "bsbmQueryLogSeed" in runConfig else bsbmQueryLogSeed
                    execution.bsbmLogMode = runConfig[
                        "bsbmLogMode"] if "bsbmLogMode" in runConfig else bsbmLogMode
                    execution.repoDir = runConfig[
                        "repoDir"] if "repoDir" in runConfig else repoDir
                    execution.evalMode = runConfig[
                        "evalMode"] if "evalMode" in runConfig else evalMode
                    execution.rasbmMaxTriplesPerQuery = runConfig[
                        "rasbmMaxTriplesPerQuery"] if "rasbmMaxTriplesPerQuery" in runConfig else rasbmMaxTriplesPerQuery
                    execution.rasbmQueryExecutions = runConfig[
                        "rasbmQueryExecutions"] if "rasbmQueryExecutions" in runConfig else rasbmQueryExecutions
                    execution.rasbmVirtuoso = runConfig[
                        "rasbmVirtuoso"] if "rasbmVirtuoso" in runConfig else rasbmVirtuoso
                    execution.executable = runConfig[
                        "executable"] if "executable" in runConfig else executable
                    if "executionType" in runConfig:
                        execution.store = rasbmMode[runConfig["executionType"].lower()]
                    else:
                        if store is None:
                            logger.error('We need to know which store we use. Exiting')
                            sys.exit()
                        execution.store = store
                        logger.info('Found the store: {}'.format(store))

                    if "image" in runConfig:
                        execution.image = runConfig["image"]
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


if __name__ == '__main__':

    # ch.setLevel(logging.DEBUG)
    # logger.addHandler(ch)

    if (len(sys.argv) < 2):
        logger.error("You need to specify a scenario")
        sys.exit(1)

    scenarioPath = sys.argv[1]
    bsqbmMain(scenarioPath, RaScenarioReader(), BSQBMRunner())
