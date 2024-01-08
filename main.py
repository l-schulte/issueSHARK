import os
import logging
import logging.config
import json
import sys
import argparse

from issueshark.backends.basebackend import BaseBackend
from issueshark.config import Config, ConfigValidationException
from issueshark.issueshark import IssueSHARK
from pycoshark.utils import get_base_argparser


def setup_logging(
    default_path=os.path.dirname(os.path.realpath(__file__)) + "/loggerConfiguration.json", default_level=logging.INFO
):
    """
    Setup logging configuration

    :param default_path: path to the logger configuration
    :param default_level: defines the default logging level if configuration file is not found(default:logging.INFO)
    """
    path = default_path
    if os.path.exists(path):
        with open(path, "rt") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def start():
    """
    Starts the application. First parses the different command line arguments and then it gives these to
    :class:`~issueshark.issueshark.IssueSHARK`
    """
    setup_logging()
    logger = logging.getLogger("main")
    logger.info("Starting issueSHARK...")

    try:
        backend_choices = BaseBackend.get_all_possible_backend_options()
    except Exception as e:
        logger.exception("Failed to instantiate backend.")
        sys.exit(1)

    logger.debug("Found the following backends: %s" % ", ".join(backend_choices))

    parser = get_base_argparser("Collects information from different issue tracking systems.", "1.0.0")
    parser.add_argument("-n", "--project-name", help="Name of the project to analyze.", required=True)
    parser.add_argument("-i", "--issueurl", help="URL to the bugtracking system.", required=True)
    parser.add_argument(
        "-b", "--backend", help="Backend to use for the issue parsing", default="github", choices=backend_choices
    )
    parser.add_argument("-PH", "--proxy-host", help="Proxy hostname or IP address.", default=None)
    parser.add_argument("-PP", "--proxy-port", help="Port of the proxy to use.", default=None)
    parser.add_argument("-Pp", "--proxy-password", help="Password to use the proxy (HTTP Basic Auth)", default=None)
    parser.add_argument("-PU", "--proxy-user", help="Username to use the proxy (HTTP Basic Auth)", default=None)
    parser.add_argument("-iU", "--issue-user", help="Username to use the issue tracking system", default=None)
    parser.add_argument("-iP", "--issue-password", help="Password to use the issue tracking system", default=None)
    parser.add_argument(
        "-ll",
        "--debug",
        help="Sets the debug level.",
        default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    parser.add_argument("-t", "--token", help="Token for accessing.", default=None)

    try:
        args = parser.parse_args()
        cfg = Config(args)
    except ConfigValidationException as e:
        logger.error(e)
        sys.exit(1)

    issueshark = IssueSHARK()
    issueshark.start(cfg)


if __name__ == "__main__":
    start()
