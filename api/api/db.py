import boto3
from boto3.dynamodb.conditions import Key

ddb = boto3.resource('dynamodb')


class DDB:
    def __init__(self, table_name="pro-licenses", partition_key="state-agency", sort_key="license-number"):
        self.table = ddb.Table(table_name)
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.query_limit = 50

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

        res = self.table.query(
            Limit=self.query_limit,
            KeyConditionExpression=Key(self.partition_key).eq(partition_key)
        )

        return res['Items']
