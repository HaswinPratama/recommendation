
from google.cloud.bigtable import Client
from google.oauth2 import service_account
from google.cloud.bigtable import column_family
import os
import google.cloud.bigtable.row_filters as row_filters
import datetime
from google.cloud.bigtable.row_set import RowSet
import csv




config = os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "haswin.json"
pid  = "ddb-bribrain-brimo-recom" 
iid = "ddb-bribrain-brimo-recom"
tid    = "recommendation-brimo"
cfi = "recommendation-value"

credentials = service_account.Credentials.from_service_account_file('haswin.json',)

client   = Client(project=pid, credentials=credentials, admin=True)
instance = client.instance(instance_id=iid)
# table    = instance.table(tid)



def getData(id):
    table = instance.table(tid)
    row_key = id
    row = table.read_row(row_key)
    recommedation = ''
    print("Reading data for {}:".format(row.row_key.decode("utf-8")))
    for cf, cols in sorted(row.cells.items()):
        for col, cells in sorted(cols.items()):
            recommedation = cells[0].value.decode("utf-8")
    return recommedation.split(' ')

def deleteTable(tid):
    print("Table deleted")
    table = instance.table(tid)
    table.delete()

def createNewTable(tid):
    print("Creating the {} table.".format(tid))
    table = instance.table(tid)
    print("Creating column family recommendation-value with Max Version GC rule...")
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    cf = {cfi: max_versions_rule}
    if not table.exists():
        table.create(column_families=cf)
    else:
        print("Table {} already exists.".format(tid))


def insertDataTable(tid, data):
    table = instance.table(tid)
    print("Writing data recommedation to the table.")
    rows = []
    for i, value in enumerate(data):
        print(i, value)
        row_key = value['id'].encode()
        row = table.direct_row(row_key)

        row.set_cell(
            cfi, value['id'],value = ' '.join([value['1'],value['2'],value['3'],value['4'],value['5'],value['6'],value['7'],value['8'],value['9'],value['10']]), timestamp=datetime.datetime.utcnow()
        )
        rows.append(row)
    table.mutate_rows(rows)

def formatCsvData():
    with open('data.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        li_return = []
        for row in csv_reader:
            # temp_dict = {'id':row['id'], 'data':[row['1'],row['2'],row['3'],row['4'],row['5'],row['6'],row['7'],row['8'],row['9'],row['10'],]}
            li_return.append(row)
        return li_return

def insertCsvToBigTable():
    total_data = formatCsvData()
    createNewTable(tid)
    insertDataTable(tid, total_data)


    
    