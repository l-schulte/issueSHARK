from issueshark.backends.basebackend import BaseBackend

from pycoshark.mongomodels import Issue, IssueComment, Event, People

import logging
import typing
import dateutil
import requests
import time
import datetime

from mongoengine import DoesNotExist

logger = logging.getLogger('backend')


def elvis(dict, key, fallback_value=None):
    """
    Returns the value of the key in the dict or the fallback_value if the key is not in the dict.
    :param dict:
    :param key:
    :param fallback_value:
    """
    return dict[key] if (dict is not None and key in dict) else fallback_value


class LaunchpadBackend(BaseBackend):
    """
    Backend that collects issues from Launchpad.
    """

    @property
    def identifier(self):
        """
        Identifier of the backend (launchpad)
        """
        return 'launchpad'

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

    def process(self):

        logger.info("Starting the collection process...")

        # Get last modification date (since then, we will collect bugs)
        last_issue = Issue.objects(issue_system_id=self.issue_system_id)\
            .order_by('-updated_at').only('updated_at').first()

        starting_date = None
        if last_issue is not None:
            starting_date = last_issue.updated_at

        # Get all issues
        issues_generator = self.get_issues(starting_date)
        counter = 0
        for base, issue in issues_generator:
            self.store_issue(base, issue)
            counter += 1

        logger.info("Collected %s issues." % counter)

    def get_issues(self, start_date: datetime.datetime = None) -> typing.Generator:
        """
        Creates a generator that yields the raw issues collected from the launchpad API.

        :param start_date: date from which the issues should be collected.
            :class:`datetime.datetime`
        :return: generator that yields the raw issues.
            :class:`generator`
        """

        # PROJECT = 'nova'
        # API_VERSION = 'devel'  # 1.0; beta;
        # BASE_URL = f'https://api.launchpad.net/{API_VERSION}/{PROJECT}'

        BASE_URL = self.config.tracking_url
        STATUS_VALUES = ["New", "Incomplete", "Opinion", "Invalid", "Won't Fix", "Expired", "Confirmed",
                         "Triaged", "In Progress", "Fix Committed", "Fix Released", "Does Not Exist",
                         "Incomplete (with response)", "Incomplete (without response)"]

        response = {
            'next_collection_link': BASE_URL
            + '?ws.op=searchTasks'
            + (f'&modified_since={start_date.isoformat()}' if start_date else '')
            + '&order_by=date_last_updated'
            + ''.join([f"&status={status}" for status in STATUS_VALUES])
        }

        while 'next_collection_link' in response:
            response = self._send_request(response['next_collection_link'])

            for entry in response['entries']:
                yield entry, self._send_request(entry['bug_link'])

    def store_issue(self, raw_base_issue, raw_issue):
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

        logger.debug('Processing issue %s' % raw_issue)

        external_id = str(raw_issue['id'])

        created_at = dateutil.parser.parse(raw_issue['date_created'])
        updated_at = dateutil.parser.parse(raw_issue['date_last_updated'])

        try:
            # We can not return here, as the issue might be updated. This means, that the title could be updated
            # as well as comments and new events
            issue = Issue.objects(issue_system_id=self.issue_system_id, external_id=external_id).get()
        except DoesNotExist:
            issue = Issue(issue_system_id=self.issue_system_id, external_id=external_id)

        owner = self._get_people(raw_issue['owner_link'])
        assignee = self._get_people(raw_base_issue['assignee_link'])

        issue.reporter_id = owner
        issue.creator_id = owner
        issue.assignee_id = assignee
        issue.title = raw_issue['title']
        issue.desc = raw_issue['description']
        issue.updated_at = updated_at
        issue.created_at = created_at
        issue.status = raw_base_issue['status']
        issue.labels = raw_issue['tags']

        mongo_issue = issue.save()

        self._process_comments(str(issue['id']), raw_issue['messages_collection_link'], mongo_issue)
        self._process_events(str(issue['id']), raw_issue['activity_collection_link'], mongo_issue)

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
        events = elvis(self._send_request(target_url), 'entries', [])

        # Go through all events and create mongo objects from it
        events_to_store = []
        for raw_event in events:
            created_at = dateutil.parser.parse(raw_event['datechanged'])
            external_id = mongo_issue.external_id + '/activity/#' + raw_event['http_etag']

            # If the event is already saved, we can just continue, because nothing will change on the event
            try:
                Event.objects(external_id=external_id, issue_id=mongo_issue.id).get()
                continue
            except DoesNotExist:
                event = Event(external_id=external_id,
                              issue_id=mongo_issue.id,
                              created_at=created_at,
                              status=raw_event['whatchanged'],
                              new_value=raw_event['newvalue'],
                              old_value=raw_event['oldvalue'],
                              author_id=self._get_people(raw_event['person_link']))

            # If the event affects the issue data, we need to set back the issue to its original state
            if event.status == 'summary':
                mongo_issue.title = event.old_value
            if event.status == 'description':
                mongo_issue.desc = event.old_value

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
        comments = elvis(self._send_request(target_url), 'entries', [])

        # Go through all comments
        comments_to_insert = []
        for raw_comment in comments:
            created_at = dateutil.parser.parse(raw_comment['date_created'])
            external_id = '/'.join(raw_comment['self_link'].split('/')[-3:])
            try:
                IssueComment.objects(external_id=external_id, issue_id=mongo_issue.id).get()
                continue
            except DoesNotExist:
                owner = self._get_people(raw_comment['owner_link'])

                comment = IssueComment(
                    external_id=external_id,
                    issue_id=mongo_issue.id,
                    created_at=created_at,
                    author_id=owner,
                    comment=raw_comment['content'],
                )
                comments_to_insert.append(comment)

        # If comments need to be inserted -> bulk insert
        if comments_to_insert:
            IssueComment.objects.insert(comments_to_insert, load_bulk=False)

    def _get_people(self, url):
        if url is None:
            return None

        if url in self.people:
            return self.people[url]

        raw_person = self._send_request(url)

        if raw_person is None:
            return None

        people_id = People.objects(
            name=raw_person['display_name']
        ).upsert_one(name=raw_person['display_name'], username=raw_person['name']).id
        self.people[url] = people_id

        return people_id

    def _send_request(self, url):
        """
        Sends arequest using the requests library to the url specified

        :param url: url to which the request should be sent
        """
        auth = None
        headers = None

        # Make the request
        tries = 1
        while tries <= 3:
            logger.debug("Sending request to url: %s (Try: %s)" % (url, tries))
            resp = requests.get(url, headers=headers, proxies=self.config.get_proxy_dictionary(), auth=auth)

            if resp.status_code == 410:
                logger.error("The url %s is no longer available." % url)
                return None

            if resp.status_code != 200:
                logger.error("Problem with getting data via url %s. Error: %s" % (url, resp.text))
                tries += 1
                time.sleep(2)
            else:
                logger.debug('Got response: %s' % resp.json())

                return resp.json()

        raise requests.RequestException("Problem with getting data via url %s." % url)
