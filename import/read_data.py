from os import link
import sys
from googleapiclient.discovery import build
import json
from csv import reader
from google.oauth2 import service_account
from os.path import exists
from functools import reduce
import time

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

    print(values[1])
    # del values[13]
    # del values[6]

    # weight_df = pd.DataFrame(values, columns=['weight_in_cat', 'global_weight'])
    # weight_df['weight_in_cat'] = weight_df['weight_in_cat'].astype(float)
    # weight_df['global_weight'] = weight_df['global_weight'].astype(float)

    # # Gets project links from the evaluation assignment sheet
    # sheet = sheetService.spreadsheets()
    # results = sheet.values().get(spreadsheetId=inputSpreadsheetId,range='Eligible Proposals and Assignments!A2:C').execute()
    # values = results.get('values', [])
    # links_df = pd.DataFrame(values, columns=['project_number', 'project_name', 'project_link'])

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

print('Finished')

