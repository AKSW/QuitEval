#! /usr/bin/env python3

import logging
import sys
import os
from bsqbm import Execution, ScenarioReader, BSQBMRunner, QuitExecution, main as bsqbmMain


logger = logging.getLogger('quit-eval-randomaccess')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)


class RARunner(BSQBMRunner):
    pass


class RandomAccessExecution(Execution):
    def runBSBM(self):
        if self.uc == 'RA':
            self.runRandomAccess
        elif self.uc == 'QL':
            self.runQueryLog

    def runQueryLog(self):
        pass

    def runRandomAccess(self):
        pass


class RaQuitExecution(RandomAccessExecution, QuitExecution):
    def runRandomAccess(self):
        pass


class RaQuitDockerExecution(RandomAccessExecution, QuitDockerExecution):
    def runRandomAccess(self):
        pass


class RaScenarioReader(ScenarioReader):
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

                    execution = getattr(sys.modules[__name__], executionType + "Execution")

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
                    execution.image = runConfig["image"] if "image" in runConfig else None
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

    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    if (len(sys.argv) < 2):
        logger.error("You need to specify a scenario")
        sys.exit(1)

    scenarioPath = sys.argv[1]
    bsqbmMain(scenarioPath, RaScenarioReader(), BSQBMRunner())
