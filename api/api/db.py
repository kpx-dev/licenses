import boto3
from boto3.dynamodb.conditions import Key, Attr

ddb = boto3.resource('dynamodb')


class DDB:
    def __init__(self, table_name="us-professional-licenses", partition_key="state-agency", sort_key="license-number"):
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

    def get_license_by_status(self, state, agency, status):
        partition_key = "{}-{}".format(state, agency)

        res = self.table.query(
            Limit=self.query_limit,
            KeyConditionExpression=Key(self.partition_key).eq(partition_key),
            FilterExpression=Attr('license_status').eq(status)
        )

        return res["Items"]

    def search_by_name(self, state, agency, query):
        partition_key = "{}-{}".format(state, agency)
        filters = False

        if "first_name" in query and query['first_name']:
            filters = Attr('first_name').begins_with(query['first_name'].strip())

        if "last_name" in query and query['last_name']:
            curr_filter = Attr('last_name').begins_with(query['last_name'].strip())
            if filters:
                filters = filters & curr_filter
            else:
                filters = curr_filter

        print('filters ', partition_key, query['first_name'])
        if filters:
            res = self.table.query(
                KeyConditionExpression=Key(self.partition_key).eq(partition_key),
                FilterExpression=filters
            )
        else:
            res = self.table.query(
                KeyConditionExpression=Key(self.partition_key).eq(partition_key)
            )

        return res["Items"]
