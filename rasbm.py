#! /usr/bin/env python3

import logging
import sys
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


class RandomeAccessExecution(Execution):
    def runQueryLog(self):
        pass

    def runRandomAccess(self):
        pass


class RaQuitExecution(QuitExecution, RandomeAccessExecution):
    def runRandomAccess(self):
        pass


class RaScenarioReader(ScenarioReader):
    pass


if __name__ == '__main__':

    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    if (len(sys.argv) < 2):
        logger.error("You need to specify a scenario")
        sys.exit(1)

    scenarioPath = sys.argv[1]
    bsqbmMain(scenarioPath, RaScenarioReader(), BSQBMRunner())
