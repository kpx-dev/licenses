import os
import csv
import json
from db import DDBQueue
from os import walk

cwd = os.getcwd()
partition_key = "state-agency"
sort_key = "license-number"
queue = DDBQueue()

def parse_tow_companies(file_path):
    with open(file_path) as fd:
        rd = csv.DictReader(fd)
        next(rd) # skip header
        item = {}
        for row in rd:
            # print(row)
            # exit()
            item = {
                "agency_name": row[' "LICENSE TYPE"'].strip(),
                "license_type": row[' "LICENSE TYPE"'].strip(),
                "customer_name": row['CUSTOMER_NAME'].strip(),
                "customer_dba_name": row['CUSTOMER_DBA_NAME'].strip(),
                "license_number": row["CERTIFICATE NUMBER"].strip(),
                'phone': row["PHONE"].strip(),

                "mail_address1": row["MAIL_ADDR_LINE1"].strip(),
                "mail_address2": row["MAIL_ADDR_LINE2"].strip(),
                "mail_city": row["MAIL_CITY"].strip(),
                "mail_state": row["MAIL_STATE"].strip(),
                "mail_zip": row["MAIL_ZIP_PREFIX"].strip(),

                "site_address1": row[' "SITE_ADDR1"'].strip(),
                "site_address2": row[' "SITE_ADDR2"'].strip(),
                "site_city": row['SITE_CITY'].strip(),
                "site_county": row['SITE_COUNTY'].strip(),
                "site_state": row['SITE_STATE'].strip(),
                "site_zip": row['SITE_ZIP_PREFIX'].strip(),

                "company_tdi": row['TDI_COMPANY_NBR'].strip(),
                "insurer": row[' "INSURER"'].strip(),
                "policy_type": row['POLICY_TYPE'].strip(),
                "coverage_amount": row['COVERAGE_AMOUNT'].strip(),
                "effective_date": row['EFFECTIVE_DATE'].strip(),
                "cancel_effective_date": row['CANCEL_EFF_DATE'].strip(),
                "vehicle_count": row['VEH_COUNT'],
            }
            save(item)
        # except Exception as e:
        #     print('error parsing, skipping', e)


def parse_massage_instructor(file_path):
    with open(file_path) as fd:
        rd = csv.DictReader(fd)
        next(rd) # skip header
        item = {}
        try:
            for row in rd:
                # print(row)
                # exit()
                item = {
                    "agency_name": row[' "LICENSE TYPE"'].strip(),
                    "license_type": row[' "LICENSE TYPE"'].strip(),
                    "customer_name": row['CUSTOMER_NAME'].strip(),
                    "customer_dba_name": row['CUSTOMER_DBA_NAME'].strip(),
                    "license_number": row["CERTIFICATE NUMBER"].strip(),
                    'phone': row["PHONE"].strip(),

                    "mail_address1": row["MAIL_ADDR_LINE1"].strip(),
                    "mail_address2": row["MAIL_ADDR_LINE2"].strip(),
                    "mail_city": row["MAIL_CITY"].strip(),
                    "mail_state": row["MAIL_STATE"].strip(),
                    "mail_zip": row["MAIL_ZIP_PREFIX"].strip(),

                    "site_address1": row[' "SITE_ADDR1"'].strip(),
                    "site_address2": row[' "SITE_ADDR2"'].strip(),
                    "site_city": row['SITE_CITY'].strip(),
                    "site_county": row['SITE_COUNTY'].strip(),
                    "site_state": row['SITE_STATE'].strip(),
                    "site_zip": row['SITE_ZIP_PREFIX'].strip(),

                    "company_tdi": row['TDI_COMPANY_NBR'].strip(),
                    "insurer": row[' "INSURER"'].strip(),
                    "policy_type": row['POLICY_TYPE'].strip(),
                    "coverage_amount": row['COVERAGE_AMOUNT'].strip(),
                    "effective_date": row['EFFECTIVE_DATE'].strip(),
                    "cancel_effective_date": row['CANCEL_EFF_DATE'].strip(),
                    "vehicle_count": row['VEH_COUNT'].strip(),
                }
                save(item)
        except Exception as e:
            print('error parsing, skipping', file_path)


def parse(file_path):
    pass

def save(item):
    agency_slug = '-'.join(item["agency_name"].replace(',', '').lower().split())

    item[partition_key] = "tx-{}".format(agency_slug)
    item[sort_key] = item["license_number"]

    queue_item = { 'PutRequest': { 'Item': item } }
    queue.enqueue(queue_item)

if __name__ == "__main__":
    file_path = cwd + "/data/tx"

    _, _, filenames = next(walk(file_path))
    for file_name in filenames:
        if not file_name.endswith('.csv'):
            continue

        full_path = "{}/{}".format(file_path, file_name)
        print('processing file ', full_path)

        if file_name in ['TowCompanies.csv', 'VSFs.csv']:
            parse_tow_companies(full_path)
        # elif file_name == 'vsMassageInstructor.csv':
        #     parse_massage_instructor(full_path)

        if queue.size() > 0:
            queue.flush()
