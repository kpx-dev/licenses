import boto3

ddb_resource = boto3.resource('dynamodb')
# ddb_client = boto3.client('dynamodb')

# Dynamo DB Queue
class DDBQueue:
    def __init__(self, table_name, partition_key, sort_key=None):
        self.table_name = table_name
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.max_queue_size = 25
        self.queue = []

    def size(self):
        return len(self.queue)

    def flush(self):
        try:
            items = { self.table_name: self.queue }
            res = ddb_resource.batch_write_item(RequestItems=items)
            self.queue = []
            # print('request items ', json.dumps(items))
            # TODO: empty queue only after all items are flushed ok
            # print(res['ResponseMetadata']['HTTPStatusCode'] == 200)
            # exit()
        except Exception as e:
            print("batch write error", e)
            # exit()

    def enqueue(self, item):
        if self.size() == self.max_queue_size:
            self.flush()

        self.queue.append(item)

