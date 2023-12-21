from issueshark.backends.basebackend import BaseBackend

from pycoshark.mongomodels import Issue, IssueComment, Event, People

import logging
import typing
import dateutil
import requests
import time
import datetime
import os

from mongoengine import DoesNotExist
import uuid

logger = logging.getLogger("backend")


def elvis(dict, key, fallback_value=None):
    """
    Returns the value of the key in the dict or the fallback_value if the key is not in the dict.
    :param dict:
    :param key:
    :param fallback_value:
    """
    return (
        dict[key]
        if (dict is not None and key in dict and dict[key] is not None)
        else fallback_value
    )


class LaunchpadBackend(BaseBackend):
    """
    Backend that collects issues from Launchpad.
    """

    oauth_consumer_key = "issueSHARK"
    oauth_token = None
    oauth_signature = None

    @property
    def identifier(self):
        """
        Identifier of the backend (launchpad)
        """
        return "launchpad"

    def __init__(self, cfg, issue_system_id, project_id):
        """
        Initialization
        Initializes the people dictionary see: :func:`~issueshark.backends.launchpad.LaunchpadBackend._get_people`


        :param cfg: holds als configuration. Object of class.
            :class:`~issueshark.config.Config`
        :param issue_system_id: id of the issue system for which data should be collected.
            :class:`bson.objectid.ObjectId`
        :param project_id: id of the project to which the issue system belongs.
            :class:`bson.objectid.ObjectId`
        """
        super().__init__(cfg, issue_system_id, project_id)

        logger.setLevel(self.debug_level)
        self.people = {}

        # From https://help.launchpad.net/API/SigningRequests
        credentials = self._get_credentials()
        self.oauth_token = credentials[0]
        self.oauth_signature = credentials[1]

    def process(self):
        logger.info("Starting the collection process...")

        counter = 0

        # Get all specs
        specs_generator = self.get_specs()
        for spec in specs_generator:
            self.store_issue(spec)
            counter += 1

        # Get all bugs
        bugs_generator = self.get_bugs()
        for bug in bugs_generator:
            self.store_issue(bug)
            counter += 1

        logger.info("Collected %s issues." % counter)

    def get_specs(self) -> typing.Generator:
        """
        Creates a generator that yields specs parsed into the issue format collected from launchpad.

        :return: generator that yields the raw issues.
            :class:`generator`
        """

        # PROJECT = 'nova'
        # API_VERSION = 'devel'  # 1.0; beta;
        # BASE_URL = f'https://api.launchpad.net/{API_VERSION}/{PROJECT}/all_specifications'

        BASE_URL = self.config.tracking_url + "/all_specifications"

        response = {"next_collection_link": BASE_URL}

        while "next_collection_link" in response:
            print(response["next_collection_link"])
            response = self._send_request(response["next_collection_link"])

            for raw_spec in response["entries"]:
                bugs = self._send_request(raw_spec["bugs_collection_link"])
                bug_ids = [{"id": str(bug["id"])} for bug in bugs["entries"]]

                owner = self._get_people(raw_spec["owner_link"])
                drafter = self._get_people(raw_spec["drafter_link"])
                # approver = self._get_people(raw_spec['approver_link'])
                assignee = self._get_people(raw_spec["assignee_link"])

                fallback_date = "01-01-1900T12:00:00.0+00:00"
                updated_at = max(
                    dateutil.parser.parse(
                        elvis(raw_spec, "date_created", fallback_date)
                    ),
                    dateutil.parser.parse(
                        elvis(raw_spec, "date_started", fallback_date)
                    ),
                    dateutil.parser.parse(
                        elvis(raw_spec, "date_completed", fallback_date)
                    ),
                )

                description = raw_spec["summary"]
                if elvis(raw_spec, "whiteboard"):
                    description += (
                        "\n\n--- whiteboard: ---\n\n" + raw_spec["whiteboard"]
                    )

                spec = {
                    "external_id": raw_spec["name"],
                    "reporter_id": owner,
                    "creator_id": drafter,
                    "assignee_id": assignee,
                    "title": raw_spec["title"],
                    "desc": description,
                    "updated_at": updated_at,
                    "created_at": dateutil.parser.parse(raw_spec["date_created"]),
                    "status": raw_spec["lifecycle_status"],
                    "labels": [],
                    "bug_ids": bug_ids,
                }

                yield spec

    def get_bugs(self, start_date: datetime.datetime = None) -> typing.Generator:
        """
        Creates a generator that yields bugs parsed into the issue format collected from launchpad.

        :param start_date: date from which the issues should be collected.
            :class:`datetime.datetime`
        :return: generator that yields the issues.
            :class:`generator`
        """

        # PROJECT = 'nova'
        # API_VERSION = 'devel'  # 1.0; beta;
        # BASE_URL = f'https://api.launchpad.net/{API_VERSION}/{PROJECT}'

        BASE_URL = self.config.tracking_url
        STATUS_VALUES = [
            "New",
            "Incomplete",
            "Opinion",
            "Invalid",
            "Won't Fix",
            "Expired",
            "Confirmed",
            "Triaged",
            "In Progress",
            "Fix Committed",
            "Fix Released",
            "Does Not Exist",
            "Incomplete (with response)",
            "Incomplete (without response)",
        ]

        response = {
            "next_collection_link": BASE_URL
            + "?ws.op=searchTasks"
            + (
                f"&modified_since={(start_date + datetime.timedelta(0,1)).isoformat()}"
                if start_date
                else ""
            )
            + "&order_by=date_last_updated"
            + "".join([f"&status={status}" for status in STATUS_VALUES])
        }

        while "next_collection_link" in response:
            response = self._send_request(response["next_collection_link"])

            for raw_base_bug in response["entries"]:
                raw_bug = self._send_request(raw_base_bug["bug_link"])

                owner = self._get_people(raw_bug["owner_link"])
                assignee = self._get_people(raw_base_bug["assignee_link"])

                bug = {
                    "external_id": str(raw_bug["id"]),
                    "reporter_id": owner,
                    "creator_id": owner,
                    "assignee_id": assignee,
                    "title": raw_bug["title"],
                    "desc": raw_bug["description"],
                    "updated_at": dateutil.parser.parse(raw_bug["date_last_updated"]),
                    "created_at": dateutil.parser.parse(raw_bug["date_created"]),
                    "status": raw_base_bug["status"],
                    "labels": raw_bug["tags"],
                    "messages_collection_link": raw_bug["messages_collection_link"],
                    "activity_collection_link": raw_bug["activity_collection_link"],
                }

                yield bug

    def store_issue(self, raw_issue):
        """
        Transforms the issue from a launchpad issue to our issue model

        1. Transforms the issue to our model

        2. Processes the comments of the issue.
        See: :func:`~issueshark.backends.launchpad.LaunchpadBackend._process_comments`

        3. Processes the events of the issue.
        See: :func:`~issueshark.backends.launchpad.LaunchpadBackend._process_events`.
        During this: set back the issue and store it again.

        :param raw_issue: like we got it from launchpad
        """

        logger.debug("Processing issue %s" % raw_issue)

        external_id = raw_issue["external_id"]

        try:
            issue = Issue.objects(
                issue_system_id=self.issue_system_id, external_id=external_id
            ).get()
        except DoesNotExist:
            issue = Issue(issue_system_id=self.issue_system_id, external_id=external_id)

        issue.reporter_id = raw_issue["reporter_id"]
        issue.creator_id = raw_issue["creator_id"]
        issue.assignee_id = raw_issue["assignee_id"]
        issue.title = raw_issue["title"]
        issue.desc = raw_issue["desc"]
        issue.updated_at = raw_issue["updated_at"]
        issue.created_at = raw_issue["created_at"]
        issue.status = raw_issue["status"]
        issue.labels = raw_issue["labels"]
        # spec specific
        # issue.approver_id = elvis(raw_issue, 'approver_id')
        # issue.whiteboard = elvis(raw_issue, 'whiteboard')
        issue.issue_links = elvis(raw_issue, "bug_ids")

        mongo_issue = issue.save()

        if "messages_collection_link" in raw_issue:
            self._process_comments(
                str(issue["id"]), raw_issue["messages_collection_link"], mongo_issue
            )
        if "activity_collection_link" in raw_issue:
            self._process_events(
                str(issue["id"]), raw_issue["activity_collection_link"], mongo_issue
            )

        return mongo_issue

    def _process_events(self, system_id, target_url, mongo_issue):
        """
        Processes events of an issue.

        Go through all events and store them. If it has a commit_id in it, directly link it to the VCS data. If the
        event affects the stored issue data (e.g., rename) set back the issue to its original state.

        :param system_id: id of the issue like it is given from the launchpad API
        :param mongo_issue: object of our issue model
        """
        # Get all events to the corresponding issue
        events = elvis(self._send_request(target_url), "entries", [])

        # Go through all events and create mongo objects from it
        events_to_store = []
        for raw_event in events:
            created_at = dateutil.parser.parse(raw_event["datechanged"])
            external_id = (
                mongo_issue.external_id + "/activity/#" + raw_event["http_etag"]
            )

            # If the event is already saved, we can just continue, because nothing will change on the event
            try:
                Event.objects(external_id=external_id, issue_id=mongo_issue.id).get()
                continue
            except DoesNotExist:
                event = Event(
                    external_id=external_id,
                    issue_id=mongo_issue.id,
                    created_at=created_at,
                    status=raw_event["whatchanged"],
                    new_value=raw_event["newvalue"],
                    old_value=raw_event["oldvalue"],
                    author_id=self._get_people(raw_event["person_link"]),
                )

            mongo_issue.save()

            events_to_store.append(event)

        # Bulk insert to database
        if events_to_store:
            Event.objects.insert(events_to_store, load_bulk=False)

    def _process_comments(self, system_id, target_url, mongo_issue):
        """
        Processes the comments of an issue

        :param system_id: id of the issue like it is given by the launchpad API
        :param mongo_issue: object of our issue model
        """
        # Get all the comments for the corresponding issue
        comments = elvis(self._send_request(target_url), "entries", [])

        # Go through all comments
        comments_to_insert = []
        for raw_comment in comments:
            created_at = dateutil.parser.parse(raw_comment["date_created"])
            external_id = "/".join(raw_comment["self_link"].split("/")[-3:])
            try:
                IssueComment.objects(
                    external_id=external_id, issue_id=mongo_issue.id
                ).get()
                continue
            except DoesNotExist:
                owner = self._get_people(raw_comment["owner_link"])

                comment = IssueComment(
                    external_id=external_id,
                    issue_id=mongo_issue.id,
                    created_at=created_at,
                    author_id=owner,
                    comment=raw_comment["content"],
                )
                comments_to_insert.append(comment)

        # If comments need to be inserted -> bulk insert
        if comments_to_insert:
            IssueComment.objects.insert(comments_to_insert, load_bulk=False)

    def _get_people(self, url):
        """
        Gets the people id from the launchpad url

        :param url: url to the launchpad person
        :return: id of the person
        """
        if url is None:
            return None

        if url in self.people:
            return self.people[url]

        raw_person = self._send_request(url, use_credentials=True)

        if raw_person is None:
            return None

        fallback_email = f'{raw_person["name"]}@no_email.launchpad.issueSHARK'
        email = elvis(raw_person, "preferred_email_address_link", fallback_email).split(
            "/"
        )[-1]

        if email == "tag:launchpad.net:2008:redacted":
            email = fallback_email

        try:
            # Try to identify the user by their email. If no email is given try the username.
            try:
                people = People.objects.get(
                    email=email, name=raw_person["display_name"]
                )
            except DoesNotExist:
                people = People.objects.get(
                    username=raw_person["name"], name=raw_person["display_name"]
                )
        except DoesNotExist:
            people = People(username=raw_person["name"], email=email)

            people.name = raw_person["display_name"]
            people = people.save()

        self.people[url] = people.id

        return people.id

    def _send_request(self, url, use_credentials=False):
        """
        Sends a request using the requests library to the url specified.
        If the request fails, it is repeated 2 more times.

        :param url: url to which the request should be sent
        """

        headers = None

        # Make the request
        tries = 1
        while tries <= 3:
            params = (
                {
                    "oauth_consumer_key": self.oauth_consumer_key,
                    "oauth_consumer_secret": " ",
                    "oauth_nonce": str(uuid.uuid4()),
                    "oauth_signature": f"&{self.oauth_signature}",
                    "oauth_signature_method": "PLAINTEXT",
                    "oauth_timestamp": str(int(time.time())),
                    "oauth_token": self.oauth_token,
                    "oauth_version": "1.0",
                }
                if use_credentials
                else None
            )

            logger.debug("Sending request to url: %s (Try: %s)" % (url, tries))
            resp = requests.get(
                url,
                params=params,
                headers=headers,
                proxies=self.config.get_proxy_dictionary(),
            )

            if resp.status_code == 410:
                logger.error("The url %s is no longer available." % url)
                return None

            if resp.status_code != 200:
                logger.error(
                    "Problem with getting data via url %s. Error: %s - %s"
                    % (url, resp.status_code, resp.text)
                )
                tries += 1
                time.sleep((tries + 1) ** 2)
            else:
                logger.debug("Got response: %s" % resp.json())

                return resp.json()

        raise requests.RequestException("Problem with getting data via url %s." % url)

    def _get_credentials(self) -> tuple[str, str]:
        """
        Gets the credentials for the launchpad backend.

        The credentials are stored in the config file.

        :return: tuple of the credentials
        """
        # Check if 'launchpad.conf' exists in file system
        if os.path.isfile("launchpad.conf"):
            conf = open("launchpad.conf", "r")
            content = conf.read().split(", ")
            conf.close()
            return content[0], content[1]

        LAUNCHPAD_REQUESTTOKEN_URL = "https://launchpad.net/+request-token"

        resp = requests.post(
            LAUNCHPAD_REQUESTTOKEN_URL,
            data={
                "Content-type": "application/x-www-form-urlencoded",
                "oauth_consumer_key": self.oauth_consumer_key,
                "oauth_signature_method": "PLAINTEXT",
                "oauth_signature": "&",
            },
        )
        content = resp.text.split("&")
        oauth_request_token = content[0].split("=")[1]
        oauth_request_token_secret = content[1].split("=")[1]

        message = f"""
        ###########################################################################
        #                                                                         #
        #     Launchpad requires you to log-in and authorize the access token     #
        #                                                                         #
        #             The required level of access is "Read Anything"             #
        #                                                                         #
        # https://launchpad.net/+authorize-token?oauth_token={oauth_request_token} #
        #                                                                         #
        ###########################################################################
        """

        print(message)
        input("Press Enter once the authorization is complete...")

        LAUNCHPAD_ACCESSTOKEN_URL = "https://launchpad.net/+access-token"

        resp = requests.post(
            LAUNCHPAD_ACCESSTOKEN_URL,
            data={
                "Content-type": "application/x-www-form-urlencoded",
                "oauth_token": oauth_request_token,
                "oauth_consumer_key": self.oauth_consumer_key,
                "oauth_signature_method": "PLAINTEXT",
                "oauth_signature": f"&{oauth_request_token_secret}",
            },
        )

        content = resp.text.split("&")
        oauth_token = content[0].split("=")[1]
        oauth_token_secret = content[1].split("=")[1]

        conf = open("launchpad.conf", "w")
        conf.write(oauth_token + ", " + oauth_token_secret)
        conf.close()

        return oauth_token, oauth_token_secret
