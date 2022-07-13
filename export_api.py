
import pandas as pd
import numpy as np
from  itertools import combinations
import gc
from datetime import datetime
import itertools
import sys
import argparse
from contextlib import closing
from datetime import datetime
from datetime import timedelta
import os
from six.moves.urllib.request import urlopen
from oauth2client.service_account import ServiceAccountCredentials
import pytz
import util
import time

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="client_secrets.json"
#
# Campaign='All'+'-Significant-weightedPI-Rej-'+str(rejPI)
queryID=sys.argv[2]
reID=queryID
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="client_secrets.json"
#os.environ[environment_vars.CREDENTIALS]="client_secrets.json"
#Don't touch the parser as it is not required to make changes here.
parser = argparse.ArgumentParser(
    add_help=False,
    description='Downloads a report if it has been created in '
    'the given timeframe.')
parser.add_argument(
    '--output_directory',
    default=(os.path.dirname(os.path.realpath(__file__))),
    help=('Path to the directory you want to '
          'save the report to.'))
parser.add_argument(
    '--query_id',
    default=queryID,
    type=int,
    help=('The id of a query used to generate a report.'))
parser.add_argument(
    '--report_window',
    default=24*7,
    type=int,
    help=('The age a report must be in hours at a maximum to '
          'be considered fresh.'))


def main(doubleclick_bid_manager, output_dir, query_id, report_window):
  #print(report_window)
  if query_id:
    # Call the API, getting the latest status for the passed queryId.
    query = (
        doubleclick_bid_manager.queries().getquery(queryId=query_id).execute())
    try:
      # If it is recent enough...
      if (1):
        if not os.path.isabs(output_dir):
          output_dir = os.path.expanduser(output_dir)

        # Grab the report and write contents to a file.
        report_url = query['metadata']['googleCloudStoragePathForLatestReport']
        output_file = '%s/%s_%s.csv' % (output_dir, query['queryId'], time.strftime("%d%m%Y"))
        with open(output_file, 'wb') as output:
          with closing(urlopen(report_url)) as url:
            output.write(url.read())
        #print('Download complete.')
        return(query['queryId'])
      else:
        print('No reports for queryId "%s" in the last %s hours.' %
              (query['queryId'], report_window))
    except KeyError:
      print('No report found for queryId "%s".' % query_id)
  else:
    # Call the API, getting a list of queries.
    response = doubleclick_bid_manager.queries().listqueries().execute()
    # Print queries out.
    print('Id\t\tName')
    if 'queries' in response:
      # Starting with the first page.
      print_queries(response)
      # Then everything else
      while 'nextPageToken' in response and response['nextPageToken']:
        response = doubleclick_bid_manager.queries().listqueries(
            pageToken=response['nextPageToken']).execute()
        print_queries(response)
    else:
      print('No queries exist.')
def print_queries(response):
  for q in response['queries']:
    print('%s\t%s' % (q['queryId'], q['metadata']['title']))


def is_in_report_window(run_time_ms, report_window):
  report_time = datetime.fromtimestamp(int((run_time_ms)) / 1000)
  earliest_time_in_range = datetime.now() - timedelta(hours=report_window)
  return report_time > earliest_time_in_range
def DownloadReportbyQueryID(query_id):
    args = util.get_arguments(sys.argv, __doc__, parents=[parser])
    QUERY_ID = query_id
    if not QUERY_ID:
        try:
          QUERY_ID = int(
              input('Enter the query id or press enter to '
                        'list queries: '))
        except ValueError:
          QUERY_ID = 0

    #print(args.output_directory)
    filename=main(util.setup(args), args.output_directory, QUERY_ID, args.report_window)
    return(filename)
queryName=DownloadReportbyQueryID(queryID)
print("Download success!")



#REPORT DOWNLOADER-
stringFormat = queryName + time.strftime("%d%m%Y") +".csv"
print("Saving with the name: "+ stringFormat)

with open("readfilename.txt", "w+") as f:
     f.write(stringFormat)

