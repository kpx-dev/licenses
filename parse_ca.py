from dotenv import load_dotenv
load_dotenv()
import os
import csv
import boto3

cwd = os.getcwd()

ddb_resource = boto3.resource('dynamodb')
ddb_client = boto3.client('dynamodb')

ddb_table_name = os.getenv("DDB_TABLE_NAME")
# table = ddb_resource.Table(ddb_table_name)
partition_key = "state-agency"
sort_key = "license-number"
ddb_max_batch = 25
ddb_queue = []

def parse(file_path):
    # headers
    # ['Agency Name', 'License Type', 'Speciality Code', 'License Number', 'Indiv/Org', 'Org/Last Name', 'First Name', 'Middle Name', 'Suffix', 'Address Line 1', 'Address Line 2', 'City', 'County', 'State', 'Zip', 'Country', 'Original Issue Date', 'Expiration Date', 'School', 'Year Graduated', 'Degree', 'License Status']

    with open(file_path) as fd:
        rd = csv.DictReader(fd, delimiter="\t", quotechar='"')
        next(rd) # skip header
        item = {}
        for row in rd:
            item = {
                "agency_name": row["Agency Name"].strip(),
                "license_type": row["License Type"].strip(),
                "speciality_code": row["Speciality Code"].strip(),
                "license_number": row["License Number"].strip(),
                "address1": row["Address Line 1"].strip(),
                "address2": row["Address Line 2"].strip(),
                "city": row["City"].strip(),
                "county": row["County"].strip(),
                "state": row["State"].strip(),
                "zip": row["Zip"].strip(),
                "country": row["Country"].strip(),
                "license_issue_date": row["Original Issue Date"].strip(),
                "license_expiration_date": row["Expiration Date"].strip(),
                "license_status": row["License Status"].strip()
            }

            if row["Indiv/Org"] == "I":
                item["is_individual"] = True
                item["first_name"] = row["First Name"].strip()
                item["middle_name"] = row["Middle Name"].strip()
                item["last_name"] = row["Org/Last Name"].strip()
                item["name_suffix"] = row["Suffix"].strip()
            elif row["Indiv/Org"] == "O":
                item["is_org"] = True
                item["org_name"] = row["Org/Last Name"].strip()

            queue_ddb(item)


def flush_ddb(ddb_queue):
    res = ddb_resource.batch_write_item(
        RequestItems={
            ddb_table_name: ddb_queue
        }
    )
    # print('batch res ')
    ddb_queue = []


def queue_ddb(item):
    if len(ddb_queue) == ddb_max_batch:
        flush_ddb(ddb_queue)

    agency_slug = '-'.join(item["agency_name"].lower().split())
    item[partition_key] = "ca-{}".format(agency_slug)
    item[sort_key] = item["license_number"]
    ddb_queue.append({ 'PutRequest': { 'Item': item } })

if __name__ == "__main__":
    # file_path = cwd + "/data/ca/Accountancy_Data00.xls"
    file_path = cwd + "/data/ca/Acupuncture_Data00.xls"

    parse(file_path)
    flush_ddb(ddb_queue)
