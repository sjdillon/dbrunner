#!python
# ==========================================================#
# title          :lambda_function.py
# description    :runs sql scripts
# author         :sjdillon
# date           :10/25/2018
# python_version :2.7.12
# {
#   "prefix1": "sjd",
#   "prefix2": "culdee",
#   "env": "ops",
#   "schema": "culdee",
#   "debug": "True",
#   "actions": ["info"]
# }
# ==========================================================================
import json
import logging
import os
import uuid
import cfnresponse
import dbrunner as dbr

logging.basicConfig(format="%(asctime)s - %(thread)s - %(levelname)s - %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def db_run(event):
  """ invoke dbrunner """
  db = dbr.Runner(event)
  if db.debug:
    dbr.check_port(db.endpoint, db.port)
  return_event = db.run()
  logger.info('return_event: {}'.format(json.dumps(return_event)))
  return return_event

def lambda_handler(event, context):
    logger.info('event: {}'.format(json.dumps(event)))
    status = None

    # check if invocation is coming from cloudformation
    if 'ResourceProperties' in event:
      try:
          payload=event['ResourceProperties']
          if 'RequestType' in event and (str(event['RequestType']) in ['Create', 'Update']):
              logger.debug('request_type: {}'.format(event['RequestType']))

              # call dbrunner
              return_event=db_run(payload)
              status = return_event['success']
              # end call

              # prepare the response event
              if 'PhysicalResourceId' not in event:
                logger.debug('PhysicalResourceId not found')
                event['PhysicalResourceId'] = str(uuid.uuid4())
                logger.debug('PhysicalResourceId: {}'.format(event['PhysicalResourceId']))

              return_data = {'success': status, 'return_event': return_event}
              logger.debug('return_data: {}'.format(return_data))
          else:
            status = True
            return_data = {'success': status, 'return_event': 'no action required on this RequestType: {}'.format(event['RequestType'])}

          if status:
              if 'StackId' in event:
                logger.debug('cfnresponse_send event: {}'.format(json.dumps(event)))
                cfnresponse.send(event, context, responseStatus='SUCCESS', responseData=return_data)
                logger.debug('success')
              return return_data
          else:
              if 'StackId' in event:
                  cfnresponse.send(event, context, responseStatus='FAILED', responseData={})
              logger.warn('failed')
              return False
      except Exception as ex:
          if 'StackId' in event:
              cfnresponse.send(event, context, responseStatus='FAILED', responseData={})
          logger.error('failed with exception', exc_info=True)
          raise


    else:
      return db_run(event)