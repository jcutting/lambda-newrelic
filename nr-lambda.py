import json
import base64
import boto3
import requests
from string import Template
import datetime


def lambda_handler(event, context):

    # New Relix API key
    nr_api_key = "12345..."

    #Metric interval in seconds
    metric_interval = 300

    #Summary or raw metrics
    summarize = True

    # Per query timeout
    timeout = 5



    api_url = "https://api.newrelic.com/v2/"

    list_servers_endpoint = "servers.json"
    list_metrics_endpoint = Template("servers/$server_id/metrics.json")
    list_data_endpoint = Template("servers/$server_id/metrics/data.json")

    stream = "new_relic_stream"



    # Calculate the from and to timeranges
    current_time = datetime.datetime.now()
    from_time = (current_time - datetime.timedelta(seconds=metric_interval)).strftime("%Y-%m-%dT%H:%M:00")
    to_time = current_time.strftime("%Y-%m-%dT%H:%M:00")


    # Create header
    headers = { 'X-Api-Key': nr_api_key, 'content-type': 'application/json' }

    session = requests.Session()

    server_done = False
    server_page = 1

    data_blob = []

    while not(server_done):
        url = api_url + list_servers_endpoint
        params = { 'page': server_page }
        r_servers = session.get(url=url, headers=headers, timeout=timeout, params=params)
        servers = r_servers.json()
        if servers['servers'] == []:
            server_done = True
        server_page = server_page + 1

        for server in servers['servers']:
            metrics_done = False
            metrics_page = 1

            while not(metrics_done):
                server_id = server['id']
                params = { 'page': metrics_page }
                url = api_url + list_metrics_endpoint.substitute(server_id=server_id)
                r_metrics = session.get(url=url, headers=headers, timeout=timeout, params=params)
                metrics = r_metrics.json()
                metric_names = []
                params = { 'names[]': [ metric['name'] for metric in metrics['metrics']], 
                           'from': from_time, 
                           'to': to_time, 
                           'summarize': summarize,
                        }
                url = api_url + list_data_endpoint.substitute(server_id=server_id)
                r_data = session.get(url=url, headers=headers, params=params, timeout=timeout)
                if metrics['metrics'] == []:
                    metrics_done = True
                metrics_page = metrics_page + 1
                if r_data.status_code == 200:
                    metric_data = r_data.json()
                    for metric in metric_data['metric_data']['metrics']:
                        data = { 
                        'timestamp': to_time,
                        'server': { 'name': server['name'],
                                    'host': server['host'],
                                    'id': server['id'],
                                  },
                        'account_id': server['account_id'],
                        'metric': { 'name': metric['name'],
                                'data': metric['timeslices'][0]['values'],
                                  },
                        }

                        record = {
                        'Data': json.dumps(data),
                        'PartitionKey': str(hash(data['server']['id']))
                         }

                        data_blob.append(record)

    kinesis_stream = boto3.client('kinesis')
    kinesis_response = kinesis_stream.put_records(Records=data_blob, StreamName=stream)
    return kinesis_response

