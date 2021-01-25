import os
import csv
import json
from db import DDBQueue
from os import walk

cwd = os.getcwd()
partition_key = 'state-agency'
sort_key = 'license-number'
queue = DDBQueue()

with open(cwd + '/parsers/fakedata.json') as f:
  fakedata = json.load(f)

def parse_without_header(file_path):
    agency_name = None

    with open(file_path) as fd:
        try:
            rd = csv.reader(fd, delimiter="\t", quotechar='"')
            for row in rd:
                agency_name = row[1].strip()
                item = {
                    'agency_name': agency_name,
                    'license_number': row[4].strip(),
                    'first_name': row[7].strip(),
                    'address1': row[10].strip(),
                    'state': row[14].strip(),
                    'country': row[16].strip(),
                    "license_issue_date": row[17].strip(),
                    "license_expiration_date": row[18].strip(),
                    "license_status": row[19].strip().lower()
                }

                last_name = row[6].strip()
                city = row[12].strip()
                zip_code = row[15].strip()
                county = row[13].strip()

                if last_name and len(last_name) > 0:
                    item['last_name'] = last_name

                if city and len(city) > 0:
                    item['city'] = city

                if zip_code and len(zip_code) > 0:
                    item['zip'] = zip_code

                if county and len(county) > 0:
                    item['county'] = county

                save(item)
        except Exception as e:
            print('error parsing without header ', file_path)

    return agency_name


def parse(file_path):
    # headers
    # ['Agency Name', 'License Type', 'Speciality Code', 'License Number', 'Indiv/Org', 'Org/Last Name', 'First Name', 'Middle Name', 'Suffix', 'Address Line 1', 'Address Line 2', 'City', 'County', 'State', 'Zip', 'Country', 'Original Issue Date', 'Expiration Date', 'School', 'Year Graduated', 'Degree', 'License Status']
    agency_name = None
    last_known_agency_name = None

    with open(file_path) as fd:
        rd = csv.DictReader(fd, delimiter="\t", quotechar='"')
        next(rd) # skip header
        item = {}
        try:
            for row in rd:
                agency_name = row["Agency Name"].strip()

                if len(agency_name) == 0:
                    print('invalid agency name, use last known: ', last_known_agency_name)
                    agency_name = last_known_agency_name

                item = {
                    "agency_name": agency_name,
                    "license_type": row["License Type"].strip(),
                    "speciality_code": row["Speciality Code"].strip(),
                    "license_number": row["License Number"].strip(),
                    "address1": row["Address Line 1"].strip(),
                    "address2": row["Address Line 2"].strip(),
                    "state": row["State"].strip(),
                    "country": row["Country"].strip(),
                    "license_issue_date": row["Original Issue Date"].strip(),
                    "license_expiration_date": row["Expiration Date"].strip(),
                    "license_status": row["License Status"].strip().lower()
                }

                # indexed keys cannot be empty
                zip_code = row["Zip"].strip()
                city = row["City"].strip()
                county = row["County"].strip()

                if len(zip_code) > 0:
                    item['zip'] = zip_code

                if len(city) > 0:
                    item['city'] = city

                if len(county) > 0:
                    item['county'] = county

                if row["Indiv/Org"] == "I":
                    # indexed keys cannot be empty
                    first_name = row["First Name"].strip()
                    last_name = row["Org/Last Name"].strip()

                    if len(first_name) > 0:
                        item["first_name"] = first_name

                    if len(last_name) > 0:
                        item["last_name"] = last_name

                    item["is_individual"] = True
                    item["middle_name"] = row["Middle Name"].strip()
                    item["name_suffix"] = row["Suffix"].strip()
                elif row["Indiv/Org"] == "O":
                    item["is_org"] = True

                    org_name = row["Org/Last Name"].strip()
                    if len(org_name) > 0:
                        item["org_name"] = org_name

                save(item)
                last_known_agency_name = agency_name
                # break
        except Exception as e:
            print('error parsing with header, skipping: ', file_path)

    return last_known_agency_name

def save(item):
    agency_slug = '-'.join(item["agency_name"].replace(',', '').lower().split())

    item[partition_key] = "ca-{}".format(agency_slug)
    item[sort_key] = item["license_number"]

    queue_item = { 'PutRequest': { 'Item': item } }
    queue.enqueue(queue_item)

if __name__ == "__main__":
    file_path = cwd + "/data/ca"

    _, _, filenames = next(walk(file_path))
    for file_name in filenames:
        if not file_name.endswith('.xls'):
            continue

        full_path = "{}/{}".format(file_path, file_name)
        print('processing file ', full_path)

        # parse with header
        agency_name = parse(full_path)

        # parse without header
        if agency_name is None:
            print('processing file without header ', full_path)
            agency_name = parse_without_header(full_path)

        if agency_name and len(agency_name) > 0:
            # insert 1 fake data for this agency
            fake_item = fakedata
            fake_item['agency_name'] = agency_name
            save(fake_item)

        if queue.size() > 0:
            queue.flush()
