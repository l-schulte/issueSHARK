import logging


class ConfigValidationException(Exception):
    """
    Exception that is thrown if the config of class :class:`~issueshark.config.Config` could not be validated
    """

    pass


class Config(object):
    """
    Config object, that holds all configuration parameters
    """

    def __init__(self, args):
        """
        Initialization

        :param args: argumentparser of the class :class:`argparse.ArgumentParser`
        """
        self.backend = args.backend
        self.tracking_url = args.issueurl.rstrip("/")
        self.identifier = args.backend
        self.token = args.token.split(",") if args.token is not None else None
        self.project_name = args.project_name
        self.host = args.db_hostname
        self.port = args.db_port
        self.user = args.db_user
        self.password = args.db_password
        self.database = args.db_database
        self.authentication_db = args.db_authentication
        self.issue_user = args.issue_user
        self.issue_password = args.issue_password
        self.debug = args.debug

        if args.proxy_host and args.proxy_host.startswith("http://"):
            self.proxy_host = args.proxy_host[7:]
        else:
            self.proxy_host = args.proxy_host

        self.proxy_port = args.proxy_port
        self.proxy_username = args.proxy_user
        self.proxy_password = args.proxy_password
        self.ssl_enabled = args.ssl
        self._validate_config()

    def _validate_config(self):
        """
        Validates the config

        1. Either token or issue-user must be set, but not both

        2. Issue user and password must be set if either of them is set

        3. Proxy user and password must be set if either of them is set

        4. Proxy host and port must be set if either of them is set
        """
        # allow public issue trackers without authentication
        # if self.token is None and self.issue_user is None:
        #    raise ConfigValidationException('Token or issue user and issue password must be set.')

        # if self.token is not None and self.issue_user is not None:
        #    raise ConfigValidationException('Either token or issue user/password combination is used!')

        if (self.issue_user is not None and self.issue_password is None) or (
            self.issue_password is not None and self.issue_user is None
        ):
            raise ConfigValidationException("Issue user and password must be set if either of them are not None.")

        if (self.proxy_username is not None and self.proxy_password is None) or (
            self.proxy_password is not None and self.proxy_username is None
        ):
            raise ConfigValidationException("Proxy user and password must be set if either of them are not None.")

        if (self.proxy_host is not None and self.proxy_port is None) or (
            self.proxy_port is not None and self.proxy_host is None
        ):
            raise ConfigValidationException("Proxy host and port must be set if either of them are not None.")

    def get_debug_level(self):
        """
        Gets the correct debug level, based on :mod:`logging`
        """
        choices = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }

        return choices[self.debug]

    def _get_proxy_string(self):
        """
        Gets the proxy string to do the requests
        """
        if self.proxy_password is None or self.proxy_username is None:
            return "http://" + self.proxy_host + ":" + self.proxy_port
        else:
            return (
                "http://"
                + self.proxy_username
                + ":"
                + self.proxy_password
                + "@"
                + self.proxy_host
                + ":"
                + self.proxy_port
            )

    def get_proxy_dictionary(self):
        """
        Creates the proxy directory for the requests
        """
        if self._use_proxy():
            proxies = {
                "http": self._get_proxy_string(),
                "https": self._get_proxy_string(),
            }

            return proxies

        return None

    def use_token(self):
        """
        Checks if token is set
        """
        if self.token is None:
            return False

        return True

    def _use_proxy(self):
        """
        Checks if a proxy is used
        """
        if self.proxy_host is None:
            return False

        return True

    def __str__(self):
        return (
            "Config: identifier: %s, token: %s, tracking_url: %s, project_name: %s, host: %s, port: %s, user: %s, "
            "password: %s, database: %s, authentication_db: %s, proxy_host: %s, proxy_port: %s, proxy_username: %s"
            "proxy_password: %s, issue_user: %s, issue_password: %s"
            % (
                self.identifier,
                self.token,
                self.tracking_url,
                self.project_name,
                self.host,
                self.port,
                self.user,
                self.password,
                self.database,
                self.authentication_db,
                self.proxy_host,
                self.proxy_port,
                self.proxy_username,
                self.proxy_password,
                self.issue_user,
                self.issue_password,
            )
        )
