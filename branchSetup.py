#!usr/bin/env python3

import bsqbm
import logging
import sys
import os
import signal
import yaml
import FileNotFoundError
import PermissionError

logger = logging.getLogger('quit-eval')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)


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

    generalConfig, scenarios = bsqbm.ScenarioReader().readScenarios(
        docs, os.path.dirname(scenarioPath))

    if os.path.exists(generalConfig["resultDirectory"]):
        logger.error("The result directory ({}) already exists, please provide an empty location".format(
            generalConfig["resultDirectory"]))
        sys.exit(1)

    os.makedirs(generalConfig["resultDirectory"])

    logfile = os.path.join(generalConfig["resultDirectory"], "scenario.log")

    try:
        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.debug('Logfile: {}'.format(logfile))
    except FileNotFoundError:
        logger.error('Logfile not found: ' + logfile)
    except PermissionError:
        logger.error('Can not create logfile: ' + logfile)

    logger.info("Use scenario configuration from: {}".format(scenarioPath))

    runner = bsqbm.BSQBMRunner()
    runner.addExecutionsToQueue(scenarios)

    with open(os.path.join(generalConfig["resultDirectory"], "scenario.yml"), "w") as resultScenario:
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
