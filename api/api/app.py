# from dotenv import load_dotenv
# load_dotenv()
import json
from db import DDB

db = DDB()

def resp(message):
    return {
        "statusCode": 200,
        "body": json.dumps(message),
    }


# ex: /ca
def handle_state(state):
    message = "/{} collection is not supported yet".format(state)
    return resp(message)

# ex: GET /tx/accountants
def handle_state_agency(state, agency):
    res = db.get_state_agency(state, agency)
    # message = "/{} collection is not supported yet".format(state)
    return resp(res)

# ex: GET /tx/accountants/123
def handle_state_agency_license(state, agency, license_number):
    res = db.get_state_agency_license(state, agency, license_number)

    return resp(res)

# ex: GET /search?q=”cpa license in california”
def handle_search():
    pass

def lambda_handler(event, context):
    # print('got event ', json.dumps(event))
    path = event['pathParameters']['proxy'].strip().split('/')
    if len(path) == 1:
        return handle_state(path[0])
    elif len(path) == 2:
        return handle_state_agency(path[0], path[1])
    elif len(path) == 3:
        return handle_state_agency_license(path[0], path[1], path[2])

    return resp("This request is not supported right now.")
