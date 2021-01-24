import boto3

ddb = boto3.resource('dynamodb')

class DDB:
    def __init__(self, table_name="pro-licenses", partition_key="state-agency", sort_key="license-number"):
        self.table = ddb.Table(table_name)
        self.partition_key = partition_key
        self.sort_key = sort_key

    def get_state_agency_license(self, state, agency, license_number):
        partition_key = "{}-{}".format(state, agency)
        key = {
            self.partition_key: partition_key,
            self.sort_key: license_number
        }
        res = self.table.get_item(Key=key)

        if "Item" in res:
            return res["Item"]

        return {}


    def get_state_agency(self, state, agency):
        partition_key = "{}-{}".format(state, agency)

        res = self.table.get_item(Key={
            self.partition_key: partition_key,
        })

        return res
