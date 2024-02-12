import os
import sys
from googleapiclient.discovery import build
import json
from csv import reader
from google.oauth2 import service_account
from os.path import exists
from functools import reduce
import time
import psycopg2
import dateutil.parser as dateParser

SERVICE_ACCOUNT_FILE = None
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


SPREADSHEET_ID = None
SPREADSHEET_RANGE = None

sheetService = None

errorState = False
errorMessage = ''
hasError = False

columnMap = {}

#########################################################

def setUpServices():
  global sheetService
  creds = service_account.Credentials.from_service_account_file( SERVICE_ACCOUNT_FILE, scopes=SCOPES )
  sheetService = build('sheets', 'v4', credentials=creds)

def getConnection():
  # For now just using environment variables, but we can set up along
  # the same lines as getConnection in Bedrock. See handler.py in 
  # Bedrock's etl_task_file_copy for a python verson of getConnection
  conn = psycopg2.connect(database=os.getenv('METRIC_DB_NAME'),
                          host=os.getenv('METRIC_DB_HOST'),
                          user=os.getenv('METRIC_DB_USER'),
                          password=os.getenv('METRIC_DB_PASSWORD'),
                          port="5432")
  return conn

nCols = -1
def createColumnMap(row):
  global columnMap
  global nCols
  nCols = len(row)

  columnMap['period_start'] = row.index('period_start')
  columnMap['period_end'] = row.index('period_end')
  columnMap['value'] = row.index('value')
  columnMap['metric_id'] = row.index('metric_id')
  columnMap['action'] = row.index('action')
  columnMap['result'] = row.index('result')
  if 'note' in row:
    columnMap['note'] = row.index('note')
  if 'disaggregation_type' in row:
    columnMap['disaggregation_type'] = row.index('disaggregation_type')
  if 'disaggregation_value' in row:
    columnMap['disaggregation_value'] = row.index('disaggregation_value')

  return

def computeHash(metricId, periodStart, periodEnd, disaggregationType, disaggregationValue):
  hash = f'{metricId}-{periodStart}-{periodEnd}-{disaggregationType}-{disaggregationValue}'
  return hash

def readInputSheet(inputSpreadsheetId, inputSpreadsheetRange):
    global nCols
    global columnMap
    global SPREADSHEET_ID
    global SPREADSHEET_RANGE
    global sheetService

    sheet = sheetService.spreadsheets()
    results = sheet.values().get(spreadsheetId=inputSpreadsheetId,range=inputSpreadsheetRange).execute()
    values = results.get('values', [])

    # Figure out where the data starts and map the relevant columns
    i = 0
    hIndex = -1
    while i < len(values):
      row = values[i]
      if set(['period_start', 'period_end', 'value', 'metric_id']).issubset(row):
        hIndex = i
        createColumnMap(row)
        break
      i += 1

    if (hIndex < 0):
       errorState = True
       errorMessage = 'Unable to find column map row'
       return None

    data = {}
    for i in range(hIndex + 1,len(values)):
      # Note: individual rows may be short if they have empty values at end
      row = [''] * nCols
      for j in range(len(values[i])):
         row[j] = values[i][j]

      metricId = row[columnMap['metric_id']]
      if metricId not in data: # Add the metric to the map
        data[metricId] = []

      disaggregation_type = row[columnMap['disaggregation_type']]
      if (len(disaggregation_type.strip()) == 0):
        disaggregation_type = 'none'
      disaggregation_value = row[columnMap['disaggregation_value']]
      if (len(disaggregation_value.strip()) == 0):
        disaggregation_value = 'none'
      value = row[columnMap['value']]

      if (len(value.strip()) == 0):
         value = None
      else:
         value = float(value)
      observation = {
        'hash': None,
        'row': i,
        'metric_id': row[columnMap['metric_id']],
        'period_start': dateParser.parse(row[columnMap['period_start']]).strftime('%Y/%m/%d'),
        'period_end': dateParser.parse(row[columnMap['period_end']]).strftime('%Y/%m/%d'),
        'disaggregation_type': disaggregation_type,
        'disaggregation_value': disaggregation_value,
        'value': value,
        'note': row[columnMap['note']],
        'version': None,
        'action': row[columnMap['action']],
        'result': None
      }
      observation['hash'] = computeHash(
         observation['metric_id'],
         observation['period_start'],
         observation['period_end'],
         observation['disaggregation_type'],
         observation['disaggregation_value']
      )
      data[metricId].append(observation)
    return(data)

def readTableRecords(metricId):
    map = {}

    sql = (
      f"SELECT * FROM internal.coa_metrics where metric_id = '{metricId}'"
      'order by metric_id, period_start, period_end, disaggregation_type, disaggregation_value, version ASC'
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    for i in range(len(rows)):
        row = rows[i]
        hash = computeHash(row[0], row[1].strftime('%Y/%m/%d'), row[2].strftime('%Y/%m/%d'), row[4], row[5])

        map[hash] = {
           'metric_id': row[0],
           'period_start': row[1],
           'period_end': row[2],
           'value': row[3],
           'note': row[6],
           'disaggregation_type': row[4],
           'disaggregation_value': row[5],
           'version': row[7]
        }
    return map

def doInsert(cursor, itm):
    sql = (
       'insert into internal.coa_metrics '
       '(metric_id, period_start, period_end, value, note, disaggregation_type, disaggregation_value) values ('
       f"'{itm['metric_id']}', '{itm['period_start']}',"
       f"'{itm['period_end']}','{itm['value']}',"
       f"'{itm['note']}','{itm['disaggregation_type']}',"
       f"'{itm['disaggregation_value']}'"
       ') returning version'
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    newVersion = rows[0][0]

    # Update the spreadsheet
    row = itm['row'] + 1

    col = chr(ord('A') + columnMap['action'])
    sheetService.spreadsheets().values().update(
      spreadsheetId=SPREADSHEET_ID,
      range=f'{col}{row}:{col}{row}',
      valueInputOption='USER_ENTERED',
      body={'values': [[f"x-{itm['action']}"]]}
    ).execute()

    col = chr(ord('A') + columnMap['result'])
    sheetService.spreadsheets().values().update(
      spreadsheetId=SPREADSHEET_ID,
      range=f'{col}{row}:{col}{row}',
      valueInputOption='USER_ENTERED',
      body={'values': [[itm['result'] + f' (version: {newVersion})']]}
    ).execute()

def doSkip(cursor, itm):
    # Update the spreadsheet
    row = itm['row'] + 1
    # No need to update the action column

    col = chr(ord('A') + columnMap['result'])
    sheetService.spreadsheets().values().update(
      spreadsheetId=SPREADSHEET_ID,
      range=f'{col}{row}:{col}{row}',
      valueInputOption='USER_ENTERED',
      body={'values': [[itm['result']]]}
    ).execute()

def doReplace(cursor, itm):
    sql = (
       'update internal.coa_metrics '
       f"set value = '{itm['value']}', note = '{itm['note']}' "
       f"where metric_id='{itm['metric_id']}' "
       f"and period_start='{itm['period_start']}' "
       f"and period_end='{itm['period_end']}' "
       f"and disaggregation_type='{itm['disaggregation_type']}' "
       f"and disaggregation_value='{itm['disaggregation_value']}'"
    )
    cursor.execute(sql)

    # Update the spreadsheet
    row = itm['row'] + 1

    col = chr(ord('A') + columnMap['action'])
    sheetService.spreadsheets().values().update(
      spreadsheetId=SPREADSHEET_ID,
      range=f'{col}{row}:{col}{row}',
      valueInputOption='USER_ENTERED',
      body={'values': [[f"x-{itm['action']}"]]}
    ).execute()

    col = chr(ord('A') + columnMap['result'])
    sheetService.spreadsheets().values().update(
      spreadsheetId=SPREADSHEET_ID,
      range=f'{col}{row}:{col}{row}',
      valueInputOption='USER_ENTERED',
      body={'values': [[itm['result']]]}
    ).execute()

def doUpdate(cursor, itm, newVersion):
    sql = (
       'insert into internal.coa_metrics '
       '(metric_id, period_start, period_end, value, note, disaggregation_type, disaggregation_value, version) values ('
       f"'{itm['metric_id']}', '{itm['period_start']}',"
       f"'{itm['period_end']}','{itm['value']}',"
       f"'{itm['note']}','{itm['disaggregation_type']}',"
       f"'{itm['disaggregation_value']}',"
       f'{newVersion}'
       ') returning version'
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    newVersion = rows[0][0]
    # Update the spreadsheet
    row = itm['row'] + 1

    col = chr(ord('A') + columnMap['action'])
    sheetService.spreadsheets().values().update(
      spreadsheetId=SPREADSHEET_ID,
      range=f'{col}{row}:{col}{row}',
      valueInputOption='USER_ENTERED',
      body={'values': [[f"x-{itm['action']}"]]}
    ).execute()

    col = chr(ord('A') + columnMap['result'])
    sheetService.spreadsheets().values().update(
      spreadsheetId=SPREADSHEET_ID,
      range=f'{col}{row}:{col}{row}',
      valueInputOption='USER_ENTERED',
      body={'values': [[itm['result'] + f' (version: {newVersion})']]}
    ).execute()

def validateAndUpdate(cursor, metricData, tableData):
   # Loop over rows in the input for this metric ID   
   for itm in metricData:
      version = 0
      hash = itm['hash']
      msg = 'New observation inserted'
      result = 'insert' # Default is to insert new metric data
      if hash in tableData:
        version = tableData[hash]['version']
        if itm['value'] != tableData[hash]['value'] or itm['note'] != tableData[hash]['note']:
           result = 'error'
           action = itm['action']
           if action.startswith('x'):
              result = 'skip'
              msg = 'No action - already done'
           if action == 'update':
               result = 'update'
               msg = 'Updated by creating a new version'
           elif action == 'replace':
               result = 'replace'
               msg = 'Existing version updated'
           else:
               result = 'error'
               msg = 'Error - skipped - unanticipated collision'
        else:
           result = 'skip'
           msg = 'No action - matches existing record'

      itm['result'] = msg
      if result == 'insert':
        doInsert(cursor, itm)
      elif result == 'update':
         doUpdate(cursor, itm, version+1)
      elif result == 'replace':
         doReplace(cursor, itm)
      elif result == 'skip':
         doSkip(cursor, itm)
      else:
         doSkip(cursor, itm)


###########################################################################
##  Main Program
###########################################################################

inputs = None
if exists('./inputs.json'):
    with open('inputs.json', 'r') as file:
        inputs = json.load(file)
else:
    print('You must create an inputs.json file')
    sys.exit()

SPREADSHEET_ID = inputs['INPUT_SPREADSHEET_ID']
SPREADSHEET_RANGE = inputs['INPUT_SPREADSHEET_RANGE']
SERVICE_ACCOUNT_FILE = inputs['SERVICE_ACCOUNT_FILE']

setUpServices()
sheet = sheetService.spreadsheets()

print('Read input data sheet')
inputData = readInputSheet(SPREADSHEET_ID, SPREADSHEET_RANGE)
if (errorState):
   print('Error: ', errorMessage)
   sys.exit(1)

# Set up the database
conn = getConnection()
cursor = conn.cursor()

print('Number of metric IDs to process: ', len(inputData))
tableData = {}
for metricId in inputData:
  print('Processing metric ID ', metricId)
  metricData = inputData[metricId]
  tableData = readTableRecords(metricId)
  validateAndUpdate(cursor, metricData, tableData)

conn.commit()


