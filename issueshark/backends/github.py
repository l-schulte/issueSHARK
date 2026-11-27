import time

import sys
import datetime

from mongoengine import DoesNotExist
from requests import RequestException
from requests.auth import HTTPBasicAuth

from issueshark.backends.basebackend import BaseBackend
import logging
import requests
import dateutil.parser

from pycoshark.mongomodels import *

logger = logging.getLogger('backend')
STATE_ALL = 'all'
STATE_CLOSED = 'closed'
STATE_OPEN = 'open'


class GitHubAPIError(Exception):
    """
    Exception that is thrown if an error with the github API occur.
    """
    pass


class GithubBackend(BaseBackend):
    """
    Backend that collects issue from github
    """
    @property
    def identifier(self):
        """
        Identifier of the backend (github)
        """
        return 'github'

    def __init__(self, cfg, issue_system_id, project_id, last_system_id):
        """
        Initialization
        Initializes the people dictionary see: :func:`~issueshark.backends.github.GithubBackend._get_people`


        :param cfg: holds als configuration. Object of class :class:`~issueshark.config.Config`
        :param issue_system_id: id of the issue system for which data should be collected. :class:`bson.objectid.ObjectId`
        :param project_id: id of the project to which the issue system belongs. :class:`bson.objectid.ObjectId`
        """
        super().__init__(cfg, issue_system_id, project_id, last_system_id)

        logger.setLevel(self.debug_level)
        self.people = {}

    def process(self):
        """
        Processes the issues from github

        1. Gets the updated_at value of the last issue that was stored (newest updated issue)

        2. Gets issues since this date

        3. Calls for each issue :func:`~issueshark.backends.github.GithubBackend.store_issue`

        4. Raises the page counter
        """
        logger.info("Starting the collection process...")

        # Get all issues
        issues = self.get_issues()

        # If no new bugs found, return
        if len(issues) == 0:
            logger.info('No new issues found. Exiting...')
            sys.exit(0)

        # Otherwise, go through all issues (and all pages)
        page_number = 1
        while len(issues) > 0:
            for issue in issues:
                mongo_issue = self.store_issue(issue)
                self._process_comments(mongo_issue)
                self._process_events(mongo_issue)
            page_number += 1
            issues = self.get_issues(pagecount=page_number)

        self.save_issues()

    def store_issue(self, raw_issue):
        """
        Transforms the issue from a github issue to our issue model

        1. Transforms the issue to our model

        2. Processes the comments of the issue. See: :func:`~issueshark.backends.github.GithubBackend._process_comments`

        3. Processes the events of the issue. See: :func:`~issueshark.backends.github.GithubBackend._process_events`.
        During this: set back the issue and store it again.

        :param raw_issue: like we got it from github
        """
        # logger.debug('Processing issue %s' % raw_issue)
        updated_at = dateutil.parser.parse(raw_issue['updated_at']).replace(tzinfo=None)
        created_at = dateutil.parser.parse(raw_issue['created_at']).replace(tzinfo=None)
        self.issue_id = str(raw_issue['number'])
        try:
            # We can not return here, as the issue might be updated. This means, that the title could be updated
            # as well as comments and new events
            mongo_issue = Issue.objects(issue_system_ids=self.last_system_id, external_id=self.issue_id).get()
            self.old_issues['issues'][self.issue_id] = mongo_issue
        except DoesNotExist:
            mongo_issue = None

        new_issue = Issue(issue_system_ids=[self.issue_system_id], external_id=self.issue_id)

        labels = []
        for label in raw_issue['labels']:
            labels.append(label['name'])

        new_issue.reporter_id = self._get_people(raw_issue['user']['url'])
        new_issue.creator_id = new_issue.reporter_id
        new_issue.title = raw_issue['title']
        new_issue.desc = raw_issue['body']
        new_issue.updated_at = updated_at
        new_issue.created_at = created_at
        new_issue.status = raw_issue['state']
        new_issue.labels = labels

        native_type = raw_issue.get('type')
        if isinstance(native_type, dict) and native_type.get('name'):
            new_issue.issue_type = native_type['name']
            
        # github issues can be pull requests too (gitea is probably the same)
        if 'pull_request' in raw_issue.keys():
            new_issue.is_pull_request = True

        if raw_issue['assignee'] is not None:
            new_issue.assignee_id = self._get_people(raw_issue['assignee']['url'])

        self.parsed_issues['issues'][self.issue_id] = new_issue
        self.check_diff_issue(mongo_issue, new_issue)
        return mongo_issue

    def _process_events(self, mongo_issue):
        """
        Processes events of an issue.

        Go through all events and store them. If it has a commit_id in it, directly link it to the VCS data. If the
        event affects the stored issue data (e.g., rename) set back the issue to its original state.

        :param system_id: id of the issue like it is given from the github API
        :param mongo_issue: object of our issue model
        """
        # Get all events to the corresponding issue
        target_url = '%s/%s/events' % (self.config.tracking_url, self.issue_id)
        events = self._send_request(target_url)

        # Go through all events and create mongo objects from it
        events_to_store = []
        for raw_event in events:
            created_at = dateutil.parser.parse(raw_event['created_at']).replace(tzinfo=None)

            # If the event is already saved, we can just continue, because nothing will change on the event
            mongo_event = None
            if mongo_issue:
                try:
                    mongo_event = IssueEvent.objects(external_id=str(raw_event['id']), issue_id=mongo_issue.id).get()
                except DoesNotExist:
                    mongo_event = None

            new_event = IssueEvent(external_id=str(raw_event['id']), created_at=created_at, status=raw_event['event'])

            if raw_event['commit_id'] is not None:
                # It can happen that a commit from another repository references this issue. Therefore, we can not
                # find the commit, as it is not part of THIS repository
                new_event.commit_sha = raw_event['commit_id']
            if 'actor' in raw_event and raw_event['actor'] is not None:
                new_event.author_id = self._get_people(raw_event['actor']['url'])

            new_event = self._set_old_and_new_value_for_event(new_event, raw_event)
            if self.issue_id not in self.parsed_issues['events']:
                self.parsed_issues['events'][self.issue_id] = {}
            self.parsed_issues['events'][self.issue_id][str(raw_event['id'])] = new_event
            self.check_diff_comment_event(mongo_event, new_event)

    def _set_old_and_new_value_for_event(self, event, raw_event):
        """
        Sets the old and new value for an event to be stored

        :param event: event conforming to our model
        :param raw_event: raw event like it is acquired from the github api
        """

        if raw_event['event'] == 'assigned':
            if 'assignee' in raw_event and raw_event['assignee'] is not None:
                event.new_value = self._get_people(raw_event['assignee']['url'])


            #if 'assigner' in raw_event and raw_event['assigner'] is not None:
            #    event.assigner_id = self._get_people(raw_event['assigner']['url'])

        if raw_event['event'] == 'unassigned':
            if 'assignee' in raw_event and raw_event['assignee'] is not None:
                event.old_value = self._get_people(raw_event['assignee']['url'])

            #if 'assigner' in raw_event and raw_event['assigner'] is not None:
            #    event.assigner_id = self._get_people(raw_event['assigner']['url'])

        if raw_event['event'] == 'labeled' and 'label' in raw_event:
            event.new_value = raw_event['label']['name']

        if raw_event['event'] == 'unlabeled' and 'label' in raw_event:
            event.old_value = raw_event['label']['name']

        if raw_event['event'] == 'milestoned' and 'milestone' in raw_event:
            event.new_value = raw_event['milestone']['title']

        if raw_event['event'] == 'demilestoned' and 'milestone' in raw_event:
            event.old_value = raw_event['milestone']['title']

        if raw_event['event'] == 'renamed' and 'rename' in raw_event:
            event.old_value = raw_event['rename']['from']
            event.new_value = raw_event['rename']['to']
        return event

    def _process_comments(self, mongo_issue):
        """
        Processes the comments of an issue

        :param system_id: id of the issue like it is given by the github API
        :param mongo_issue: object of our issue model
        """
        # Get all the comments for the corresponding issue
        target_url = '%s/%s/comments' % (self.config.tracking_url, self.issue_id)
        comments = self._send_request(target_url)

        # Go through all comments
        for raw_comment in comments:
            created_at = dateutil.parser.parse(raw_comment['created_at']).replace(tzinfo=None)
            mongo_comment = None
            if mongo_issue:
                try:
                    mongo_comment = IssueComment.objects(external_id=str(raw_comment['id']), issue_id=mongo_issue.id).get()
                except DoesNotExist:
                    mongo_comment = None

            new_comment = IssueComment(
                external_id=str(raw_comment['id']),
                created_at=created_at,
                author_id=self._get_people(raw_comment['user']['url']),
                comment=raw_comment['body'],
            )
            if self.issue_id not in self.parsed_issues['comments']:
                self.parsed_issues['comments'][self.issue_id] = {}
            self.parsed_issues['comments'][self.issue_id][str(raw_comment['id'])] = new_comment
            self.check_diff_comment_event(mongo_comment, new_comment)

    def get_issues(self, search_state='all', sorting='asc', pagecount=1):
        """
        Gets issues from the github API

        :param search_state: state to be searched (e.g., all)
        :param start_date: date from which issues should be collected
        :param sorting: sorting of the issues
        :param pagecount: page number
        """
        # Creates the target url for getting the issues
        target_url = self.config.tracking_url + "?state=" + search_state + "&page=" + str(pagecount) \
            + "&per_page=100&sort=updated&direction=" + sorting


        issues = self._send_request(target_url)
        return issues

    def _get_people(self, user_url):
        """
        Gets the person via the user url

        :param user_url: url to the github API to get information of the user
        """
        # Check if user was accessed before. This reduces the amount of API requests to github
        if user_url in self.people:
            return self.people[user_url]

        raw_user = self._send_request(user_url)
        name = raw_user['name']

        if name is None:
            name = raw_user['login']

        email = raw_user['email']
        if email is None:
            email = 'null'

        people_id = People.objects(
            name=name,
            email=email
        ).upsert_one(name=name, email=email, username=raw_user['login']).id
        self.people[user_url] = people_id
        return people_id

    def _send_request(self, url):
        """
        Sends arequest using the requests library to the url specified

        :param url: url to which the request should be sent
        """
        auth = None
        headers = None

        # If tokens are used, set the header, if not use basic authentication
        if self.config.use_token():
            headers = {'Authorization': 'token %s' % self.config.token}
        else:
            auth = HTTPBasicAuth(self.config.issue_user, self.config.issue_password)

        # Make the request
        tries = 1
        while tries <= 3:
            logger.debug("Sending request to url: %s (Try: %s)" % (url, tries))
            resp = requests.get(url, headers=headers, proxies=self.config.get_proxy_dictionary(), auth=auth)

            if resp.status_code != 200:
                logger.error("Problem with getting data via url %s. Error: %s" % (url, resp.text))
                tries += 1
                time.sleep(2)
            else:
                # It can happen that we exceed the github api limit. If we have only 1 request left we will wait
                if 'X-RateLimit-Remaining' in resp.headers and int(resp.headers['X-RateLimit-Remaining']) <= 1:

                    # We get the reset time (UTC Epoch seconds)
                    time_when_reset = datetime.datetime.fromtimestamp(float(resp.headers['X-RateLimit-Reset']))
                    now = datetime.datetime.now()

                    # Then we substract and add 10 seconds to it (so that we do not request directly at the threshold
                    waiting_time = ((time_when_reset-now).total_seconds())+10

                    logger.info("Github API limit exceeded. Waiting for %0.5f seconds..." % waiting_time)
                    time.sleep(waiting_time)

                    resp = requests.get(url, headers=headers, proxies=self.config.get_proxy_dictionary(), auth=auth)

                # logger.debug('Got response: %s' % resp.json())

                return resp.json()

        raise RequestException("Problem with getting data via url %s." % url)

