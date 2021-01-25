# from dotenv import load_dotenv
# load_dotenv()
import json
from db import DDB
db = DDB()

with open('agencies.json') as f:
  agencies = json.load(f)

def agency_slug(name):
    return '-'.join(name.replace(',', '').lower().split())

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

# ex: GET /ca/agencies
def handle_state_agencies(state):
    state = agencies[state]
    state.sort()

    payload = []
    for name in state:
        payload.append({
            "name": name,
            "collection": agency_slug(name)
        })

    return resp(payload)

# ex: GET /tx/accountants/123
def handle_state_agency_license(state, agency, license_number):
    res = db.get_state_agency_license(state, agency, license_number)

    return resp(res)

# ex: GET /tx/accountants?status=active
def handle_license_status(state, agency, status):
    res = db.get_license_by_status(state, agency, status)

    return resp(res)

# ex: GET /tx/accountants/search?first_name=A&last_name=B
def handle_search(state, agency, query):
    res = db.search(state, agency, query)

    return resp(res)

def lambda_handler(event, context):
    # print('got event ', json.dumps(event))
    path = event['pathParameters']['proxy'].strip().split('/')
    if len(path) == 1:
        return handle_state(path[0])
    elif len(path) == 2:
        state, agency = path

        if agency == 'agencies':
            return handle_state_agencies(state)

        if "queryStringParameters" in event and event["queryStringParameters"]:
            if "status" in event["queryStringParameters"]:
                status = event["queryStringParameters"]["status"].lower()

                return handle_license_status(state, agency, status)

        return handle_state_agency(state, agency)
    elif len(path) == 3:
        state, agency, extra = path

        if extra == 'search' and 'queryStringParameters' in event and event['queryStringParameters']:
            return handle_search(state, agency, event['queryStringParameters'])

        return handle_state_agency_license(state, agency, extra)

    return resp("This request is not supported right now.")
