import oci
import io
import json
import requests
import os
import sys
import datetime
import collections
from fdk import response
import logging
logging.basicConfig(level=logging.INFO)

def handler(ctx, data: io.BytesIO=None):
    signer = oci.auth.signers.get_resource_principals_signer()
    resp = do(signer)
    return response.Response(ctx,response_data=json.dumps(resp),headers={"Content-Type": "application/json"})

def get_charges(idcs, start_time, end_time):
    # returns a bill for the given compartment.
    username = os.environ['username']
    password = os.environ['password']
    idcs_guid = idcs
    cloud_acct = os.environ["domain"]

    compartmentbill = collections.defaultdict(dict)

    url_params = {
        'startTime': start_time,
        'endTime': end_time,
        'usageType': 'TOTAL',
        'computeTypeEnabled': 'Y'
    }
    resp = requests.get(
        'https://itra.oraclecloud.com/metering/api/v1/usagecost/'+cloud_acct,
        auth=(username, password),
        headers={'X-ID-TENANT-NAME': idcs_guid},
        params=url_params
    )
    #print(resp.content)

    if resp.status_code != 200:
        print('Error in GET: {}'.format(resp.status_code), file=sys.stderr)
        raise Exception

    for item in resp.json()['items']:
        itemcost = 0
        service = item['serviceName']
        resource = item['resourceName']
        for cost in item['costs']:
            itemcost += cost['computedAmount']
        try:
            compartmentbill[service][resource] += itemcost
        except KeyError:
            compartmentbill[service][resource] = itemcost
    currency = item['currency']
    return compartmentbill, currency

def CostPerService(tenancyName, daily_usage,startTime,endTime,currency,ELK_index_name):
    total = 0
    ELK_format = ''
    for service in daily_usage:
        for resource in daily_usage[service]:
            total += daily_usage[service][resource]
            #print(service, resource, daily_usage[service][resource])
            elk_data = {"serviceName": service, "resourceName": resource, "computedAmount": daily_usage[service][resource],"tenancy":tenancyName, "startTime":startTime,"endTime":endTime, "currency":currency}
            ELK_format += '\n' + '{"index": {"_index": ' + '\"' + ELK_index_name + '\"}}' + '\n' + str(elk_data).replace("'",'"') + '\n'
            #print(ELK_format)
    SendToELK(ELK_format,ELK_index_name)
    return total

def SendToELK(ELK_format,ELK_index_name):
    # push to ELK
    headers = {'Content-type': 'application/json'}
    #IP will be changed to DNS later
    ELK_url = 'http://My_ElasticSearch_IP:9200/'+ ELK_index_name +'/cost/_bulk'
    response = requests.post(url=ELK_url, data=ELK_format, headers=headers)
    print("ELK Bulk is completed and response code: {}".format(response.status_code))
    #print(response.content)

def do(signer):
    try:
        tenancy_id = os.environ['tenancy']
        idcs = os.environ['idcs_guid']
        ELK_index_name = os.environ["elk_index_name"]

        ociidentity = oci.identity.IdentityClient(config={}, signer=signer)
        tenancy = ociidentity.get_tenancy(tenancy_id)
        tenancyName = tenancy.data
        print("Tenancy Name: {}".format(tenancyName.name))

        # Set start/end time example: 2020-01-01T00:00:00.000
        end_time = datetime.datetime.utcnow().date()
        start_time = end_time + datetime.timedelta(days=-1)
        startTime = str(start_time) + 'T00:00:00.000Z'
        endTime = str(end_time) + 'T00:00:00.000Z'

        daily_usage = get_charges(idcs, startTime, endTime)
        currency = daily_usage[1]

    except Exception as e:
        print(logging.info(e))

    try:
        CostPerService(tenancyName.name, daily_usage[0], startTime,endTime, currency,ELK_index_name)
    except Exception as e:
        print("----------------- Error while Sending cost details to ELK-------------------",
              file=sys.stderr)
        logging.info(e)
        print("-------------------End----------------------", file=sys.stderr)

