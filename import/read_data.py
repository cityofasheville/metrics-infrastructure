from os import link
import sys
from googleapiclient.discovery import build
import json
from csv import reader
from google.oauth2 import service_account
from os.path import exists
from functools import reduce
import time
import psycopg2

SERVICE_ACCOUNT_FILE = None
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


INPUTS_SPREADSHEET_ID = None

sheetService = None

#########################################################

def setUpServices():
  global sheetService
  creds = service_account.Credentials.from_service_account_file( SERVICE_ACCOUNT_FILE, scopes=SCOPES )
  sheetService = build('sheets', 'v4', credentials=creds)

def createColumnMap(row):
  columnMap = dict()
  print('Column of value is ', row.index('value'))
  columnMap['period_start'] = row.index('period_start')
  columnMap['period_end'] = row.index('period_end')
  columnMap['value'] = row.index('value')
  columnMap['metric_id'] = row.index('metric_id')
  if 'note' in row:
    columnMap['note'] = row.index('note')
  if 'disaggregation_type' in row:
    columnMap['disaggregation_type'] = row.index('disaggregation_type')
  if 'disaggregation_value' in row:
    columnMap['disaggregation_value'] = row.index('disaggregation_value')
  return columnMap

def read_data(inputSpreadsheetId):
    sheet = sheetService.spreadsheets()
    results = sheet.values().get(spreadsheetId=inputSpreadsheetId,range='A1:G3').execute()
    values = results.get('values', [])
    print(values)
    # Figure out where the data starts and map the relevant columns
    i = 0
    hIndex = -1
    print('length of the array is ', len(values))
    while i < len(values):
      row = values[i]
      if set(['period_start', 'period_end', 'value', 'metric_id']).issubset(row):
        print('header row is ', i)
        hIndex = i
        columnMap = createColumnMap(row)
        print(columnMap)
        break
      i += 1
    print('Now we look at an individual value')
    data = {}
    # Steps:
    # 1. convert dates and version to a standard format
    # 2. create a hash from id, period_start/end, disaggregation type/value, and version
    # 3. rewrap into an array with just the relevant values in standard order
    for i in range(hIndex + 1,len(values)):
      row = values[i]
      metric_id = row[columnMap['metric_id']]
      period_start = row[columnMap['period_start']]
      period_end = row[columnMap['period_end']]
      disaggregation_type = row[columnMap['disaggregation_type']]
      disaggregation_value = row[columnMap['disaggregation_value']]
      version = row[columnMap['version']]
      value = row[columnMap['value']]
      hash = metric_id
    print(values[0])
    return(values)

###########################################################################


inputs = None
if exists('./inputs.json'):
    with open('inputs.json', 'r') as file:
        inputs = json.load(file)
else:
    print('You must create an inputs.json file')
    sys.exit()


INPUTS_SPREADSHEET_ID = inputs['INPUTS_SPREADSHEET_ID']
SERVICE_ACCOUNT_FILE = inputs['SERVICE_ACCOUNT_FILE']

print('Set up services')
setUpServices()
sheet = sheetService.spreadsheets()

print('Read data')
data = read_data(INPUTS_SPREADSHEET_ID)

print('Now read the database')
# Set up the database connection (note etl_task_file_copy has python verson of getconnection)
conn = psycopg2.connect(database="cn2",
                        host="localhost",
                        user="ejaxon",
                        password="kegli85",
                        port="5432")
cursor = conn.cursor()
cursor.execute("SELECT * FROM metric.coa_metrics")
print(cursor.fetchall())

print('Finished')

