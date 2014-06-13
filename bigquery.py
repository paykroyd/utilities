"""
Helper functions for interacting with BigQuery via python.
"""
import sys
import uuid
from httplib import BadStatusLine
import json
import time

import httplib2

import boto
import gslib
boto.UserAgent += ' gsutil/%s (%s)' % (gslib.VERSION, sys.platform)
import logging

import apiclient.discovery
import boto.exception
from gslib import util
from gslib import GSUTIL_DIR
from gslib import wildcard_iterator
from gslib.command_runner import CommandRunner
from gslib.util import GetBotoConfigFileList
from gslib.util import GetConfigFilePath
from gslib.util import HasConfiguredCredentials
from gslib.util import IsRunningInteractively
import gslib.exception
import httplib2
import oauth2client

# We don't use the oauth2 authentication plugin directly; importing it here
# ensures that it's loaded and available by default when an operation requiring
# authentication is performed.
try:
  from gslib.third_party.oauth2_plugin import oauth2_plugin
except ImportError:
  pass


from oauth2client.client import SignedJwtAssertionCredentials, AccessTokenRefreshError
from apiclient.discovery import build

import gslib.command
import gslib.util
from gslib.third_party.oauth2_plugin import oauth2_client
try:
    util.InitializeMultiprocessingVariables()
except:
    pass

oauth2_client.InitializeMultiprocessingVariables()


PROJECT_ID = '558172898018'
DATASET_ID = 'Activity'


class BigQuery(object):
    """
    Use BigQuery from python like a sane person.
    """

    def __init__(self, project=PROJECT_ID, dataset=DATASET_ID):
        super(BigQuery, self).__init__()
        self.project = project
        self.dataset = dataset

        key = None
        with open('key.p12') as f:
            key = f.read()

        credentials = SignedJwtAssertionCredentials(
            service_account_name='558172898018@developer.gserviceaccount.com',
            private_key=key,
            scope='https://www.googleapis.com/auth/bigquery')

        http = httplib2.Http()
        http = credentials.authorize(http)
        self.http = http
        self.service = build('bigquery', 'v2', http=http)
        self.jobs = self.service.jobs()

    def query(self, query, destination_table=None, timeout=10000, write_disposition='WRITE_EMPTY'):
        """
        Runs a synchronous query and returns the results.

        :param query: SQL statement
        :param timeout: timeout between polling attempts to get the results in ms
        :param write_disposition: supports: WRITE_EMPTY, WRITE_TRUNCATE, WRITE_APPEND
        """
        try:

            if not destination_table:
                data = {'query': query, 'timeoutMs': timeout}
            else:
                data = {'configuration': {
                    'query': {
                        'query': query,
                        'destinationTable': {'projectId': self.project,
                                             'datasetId': self.dataset,
                                             'tableId': destination_table},
                        'allowLargeResults': True,
                        'createDisposition': 'CREATE_IF_NEEDED',
                        'writeDisposition': write_disposition
                    }
                }}

            if not destination_table:
                reply = None
                error = None
                for attempt in range(0,5):
                    try:
                        reply = self.jobs.query(projectId=self.project, body=data).execute()
                        break
                    except BadStatusLine as e:
                        print 'received a bad status line error'
                        error = e
                if not reply:
                    raise error
            else:
                reply = self.jobs.insert(projectId=self.project, body=data).execute()

            jobReference = reply['jobReference']

            if not destination_table:
                # Timeout exceeded: keep polling until the job is complete.
                while not reply['jobComplete']:
                    reply = self.jobs.getQueryResults(projectId=jobReference['projectId'],
                                                      jobId=jobReference['jobId'],
                                                      timeoutMs=timeout).execute()
            else:
                return reply['jobReference']['jobId']

            results = []
            # If the result has rows, print the rows in the reply.
            if 'rows' in reply:
                results.extend([[field['v'] for field in row['f']] for row in reply['rows']])
                currentRow = len(reply['rows'])
                # Loop through each page of data
                while 'rows' in reply and currentRow < reply['totalRows']:
                    reply = self.jobs.getQueryResults(projectId=jobReference['projectId'],
                                                      jobId=jobReference['jobId'],
                                                      startIndex=currentRow).execute()
                    if 'rows' in reply:
                        results.extend([[field['v'] for field in row['f']] for row in reply['rows']])
                        currentRow += len(reply['rows'])
            return results

        except AccessTokenRefreshError:
            print ("The credentials have been revoked or expired, please re-run"
                   "the application to re-authorize")
            raise

    def export_table(self, table, destination_file, print_header=True):
        """
        Exports a table to a file in the google storage facts bucket and returns the job id.

        :param print_header: if True, each file will have a header row with the column names.
        """
        temp_file = '%s_%s' % (table, uuid.uuid4())
        data = {
            'projectId': self.project,
            'configuration': {
              'extract': {
                'printHeader': print_header,
                'sourceTable': {
                   'projectId': self.project,
                   'datasetId': self.dataset,
                   'tableId': table
                 },
                'destinationUri': 'gs://facts/%s.*.csv' % temp_file,
                'destinationFormat': 'CSV'
               }
             }
           }

        job = self.jobs.insert(projectId=self.project, body=data).execute()
        jobid = job['jobReference']['jobId']

        # wait for the export to complete
        self.wait_for_job(jobid, verbose=True)
        # download the file*
        print 'downloading table export to %s' % destination_file
        with open(destination_file, 'w') as f:
            uri = boto.storage_uri('facts/', 'gs')
            for obj in uri.get_bucket():
                if obj.name.startswith(temp_file):
                    print 'getting contents of file from %s' % obj.name
                    try:
                        obj.get_contents_to_file(f,
                                                 headers={'x-goog-api-version': '2',
                                                 'x-goog-project-id': self.project})
                    finally:
                        obj.delete()

    def delete_table(self, tablename):
        self.service.tables().delete(projectId=self.project, datasetId=self.dataset, tableId=tablename).execute()

    def get_query_results(self, jobid, filename):
        reply = self.jobs.getQueryResults(projectId=self.project,
                                  jobId=jobid,
                                  startIndex=0).execute()
        with open(filename, 'w') as f:
            if 'rows' in reply:
                results = [[field['v'] for field in row['f']] for row in reply['rows']]
                for row in results:
                    f.write(','.join(row))
                    f.write('\n')

                currentRow = len(reply['rows'])
                # Loop through each page of data
                while 'rows' in reply and currentRow < reply['totalRows']:
                    reply = self.jobs.getQueryResults(projectId=self.project,
                                                      jobId=jobid,
                                                      startIndex=currentRow).execute()
                    if 'rows' in reply:
                        results = [[field['v'] for field in row['f']] for row in reply['rows']]
                        for row in results:
                            f.write(','.join(row))
                            f.write('\n')
                        currentRow += len(reply['rows'])
                        print 'fetched %d of %s rows' % (currentRow, reply['totalRows'])

        return filename

    def update_table(self, table, rows, schema, max_bad_records=0):
        """
        Creates or updates a table with the rows passed in.

        :param table: name of the table
        :param rows: rows of data to update the table with
        :param schema:
        :param max_bad_records:
        """
        url = "https://www.googleapis.com/upload/bigquery/v2/projects/" + self.project + "/jobs"

        # Create the body of the request, separated by a boundary of xxx
        # reference on setting up the job with this header:
        # https://developers.google.com/bigquery/docs/reference/v2/jobs#resource
        newresource = ('--xxx\n' +
                       'Content-Type: application/json; charset=UTF-8\n' + '\n' +
                       '{\n' +
                       '   "configuration": {\n' +
                       '     "load": {\n' +
                       '       "schema": {\n'
                       '         "fields": ' + schema + '\n' +
                       '      },\n' +
                       '      "destinationTable": {\n' +
                       '        "projectId": "' + self.project + '",\n' +
                       '        "datasetId": "' + self.dataset + '",\n' +
                       '        "tableId": "' + table + '"\n' +
                       '      },\n' +
                       '      "maxBadRecords": ' + str(max_bad_records) + '\n'
                       '    }\n' +
                       '  }\n' +
                       '}\n' +
                       '--xxx\n' +
                       'Content-Type: application/octet-stream\n' +
                       '\n')

        for row in rows:
            newresource += row + '\n'

        # Signify the end of the body
        newresource += '\n--xxx--\n'
        headers = {'Content-Type': 'multipart/related; boundary=xxx'}

        # print 'sending data to BigQuery'
        for attempt in range(5):
            resp, content = self.http.request(url, method="POST", body=newresource, headers=headers)

            if resp.status == 200:
                jsonResponse = json.loads(content)
                jobid = jsonResponse['jobReference']['jobId']
                return jobid
            elif attempt == 4:
                print resp.status, content
                raise


    def wait_for_job(self, jobid, verbose=False):
        """
        Waits until the job completes and then returns the status.

        Raises an exception if the job is still running after 100 checks.

        :param jobid: BigQuery job id
        :param verbose: if True, it will print updates as it pings the service
        """
        errors = 0
        job = None
        for n in range(100):
            try:
                status, job = self.check_job(jobid)
            except BadStatusLine:
                errors += 1
                if errors > 10:
                    raise
                time.sleep(5)
            if status in ('RUNNING', 'PENDING'):
                print '%s is %s, waiting 10 seconds' % (jobid, status.lower())
                logging.info('%s is %s, waiting 10 seconds' % (jobid, status.lower()))
                time.sleep(10)
            elif status == 'FAILED':
                for reason in job.get('status', {}).get('errors', []):
                    if verbose:
                        print reason['message']
                    logging.error(reason['message'])
                return status
            else:
                for reason in job.get('status', {}).get('errors', []):
                    if verbose:
                        print reason['message']
                    logging.error(reason['message'])
                if status == 'UNKNOWN':
                    print job
                    logging.error(job)
                return status
        raise 'After 100 checks %s has still not finished running on BigQuery' % jobid

    def check_job(self, jobid):
        """
        Checks the status of a job.

        :param jobid: the BigQuery job id
        :return: the status of the job
        """
        job = self.jobs.get(projectId=self.project, jobId=jobid).execute()
        if job['status']['state'] == 'RUNNING':
            return 'RUNNING', job
        elif job['status']['state'] == 'PENDING':
            return 'PENDING', job
        elif job['status']['state'] == 'DONE' and 'errorResult' in job['status']:
            return 'FAILED', job
        elif job['status']['state'] == 'DONE':
            return 'SUCCESS', job
        else:
            return 'UNKNOWN', job




def update_recent_snapshots():
    """
    Uses the user_snapshots table taking the most recent entry for each user storing them in current_user_snapshots.
    """
    tablename = 'current_user_snapshots'
    latest_query = """SELECT uuid,
                           STRFTIME_UTC_USEC(snapshot_date, '%Y-%m-%d') as snapshot_date,
                           STRFTIME_UTC_USEC(lastLoggedIn, '%Y-%m-%d') as lastLoggedIn
                      FROM
                        (SELECT uuid, MAX(timestamp(date)) AS snapshot_date, MAX(timestamp(lastLoggedIn)) AS lastLoggedIn
                        FROM [Activity.user_snapshots]
                        GROUP EACH BY uuid)"""

    snapshot_query = """SELECT t1.uuid as uuid, t1.username as username, t1.date as snapshot_date, t1.source as source, t1.receiveNewsletter as receiveNewsletter,
            t1.numNotebooksFollowed as numNotebooksFollowed, t1.numNote as numNote, t1.numFile as numFile, t1.lastLoggedIn as lastLoggedIn,
            t1.numUsersFollowed as numUsersFollowed, t1.numCollabNotebooks as numCollabNotebooks, t1.numRestaurant as numRestaurant,
            t1.numAppointment as numAppointment, t1.numMovie as numMovie, t1.numPublicRecipe as numPublicRecipe, t1.numPlace as numPlace,
            t1.numAlbum as numAlbum, t1.numPublicBookmark as numPublicBookmark, t1.numFollowers as numFollowers, t1.numPublicGeneralList as numPublicGeneralList,
            t1.numPublicWine as numPublicWine, t1.numTVShow AS numTVShow, t1.numPublicVideo AS numPublicVideo, t1.numRecipe AS numRecipe, t1.numPublicAlbum AS numPublicAlbum, t1.numPublicBlocks AS numPublicBlocks, t1.numMusicalArtist AS numMusicalArtist, t1.numCheckList AS numCheckList, t1.numNotebooks AS numNotebooks, t1.numPublicFile AS numPublicFile, t1.numPublicPhoto AS numPublicPhoto, t1.numVideo AS numVideo, t1.uniqueCollaborators AS uniqueCollaborators, t1.numWine AS numWine, t1.numGeneralList AS numGeneralList, t1.numPerson AS numPerson, t1.numBlocks AS numBlocks, t1.numPublicTask AS numPublicTask, t1.numBook AS numBook, t1.numTask AS numTask, t1.numPublicCheckList AS numPublicCheckList, t1.numPublicMusicalArtist AS numPublicMusicalArtist, t1.numPublicTVShow AS numPublicTVShow, t1.numAudio AS numAudio, t1.created AS created, t1.numPublicAudio AS numPublicAudio, t1.numPublicRestaurant AS numPublicRestaurant, t1.numPublicPlace AS numPublicPlace, t1.numPublicMovie AS numPublicMovie, t1.numPublicReminder AS numPublicReminder, t1.numPublicBook AS numPublicBook, t1.numPublicPerson AS numPublicPerson, t1.numPublicNote AS numPublicNote, t1.numPublicAppointment AS numPublicAppointment, t1.numReminder AS numReminder, t1.numPhoto AS numPhoto, t1.numBookmark AS numBookmark, t1.numPublicNotebooks AS numPublicNotebooks

              FROM [Activity.user_snapshots] as t1
              JOIN EACH [Activity._latest] as t2
              ON t1.uuid = t2.uuid AND t1.date = t2.snapshot_date AND t1.lastLoggedIn = t2.lastLoggedIn;
            """
    service = BigQuery()

    jobid = service.query(latest_query, '_latest', write_disposition='WRITE_TRUNCATE')
    logging.info('starting _latest table creation job: %s' % jobid)
    if service.wait_for_job(jobid) != 'SUCCESS':
        raise ValueError('_latest job failed')
    jobid = service.query(snapshot_query, tablename, write_disposition='WRITE_TRUNCATE')
    logging.info('starting snapshot table creation job: %s' % jobid)
    if service.wait_for_job(jobid) != 'SUCCESS':
        raise ValueError('snapshot job failed')
    service.delete_table('_latest')
