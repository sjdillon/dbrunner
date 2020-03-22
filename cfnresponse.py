#!python
# ============================================================================#
# title          :cfnresponse.py
# description    :communicates CF custom resources lambda functions with CF
# author         :sjdillon
# date           :12/13/2018
# python_version :2.7.12
# notes          :https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-lambda-function-code.html
# ==========================================================================
import json
import logging

from botocore.vendored import requests

logging.basicConfig(format="%(asctime)s - %(thread)s - %(levelname)s - %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

SUCCESS = "SUCCESS"
FAILED = "FAILED"


def send(event, context, responseStatus, responseData, noEcho=False):
    responseUrl = event['ResponseURL']
    logger.info('responseUrl: {}'.format(responseUrl))

    responseBody = {}
    responseBody['Status'] = responseStatus
    responseBody['Reason'] = 'See the details in CloudWatch Log Stream: ' + context.log_stream_name
    responseBody['PhysicalResourceId'] = context.log_stream_name
    responseBody['StackId'] = event['StackId']
    responseBody['RequestId'] = event['RequestId']
    responseBody['LogicalResourceId'] = event['LogicalResourceId']
    responseBody['NoEcho'] = noEcho
    responseBody['Data'] = responseData

    json_responseBody = json.dumps(responseBody)

    logger.info("Response body: {}".format(json_responseBody))

    headers = {'content-type': '', 'content-length': str(len(json_responseBody))}

    try:
        response = requests.put(responseUrl,
                                data=json_responseBody,
                                headers=headers)
        logger.info("Status code: {}".format(response.reason))
    except Exception as e:
        logger.warn("send(..) failed executing requests.put(..): {}".format(str(e)))
