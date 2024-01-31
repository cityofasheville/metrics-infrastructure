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

def read_data(inputSpreadsheetId):
    # Gets score weights from the evaluation sheet, and project links, and puts these things into 2
    # dfs to merge with the main summary df later
    sheet = sheetService.spreadsheets()
    results = sheet.values().get(spreadsheetId=inputSpreadsheetId,range='A1:G3').execute()
    values = results.get('values', [])
    print(values)
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

