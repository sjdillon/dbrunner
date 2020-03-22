#!python
# ==========================================================#
# title          :db_runner.py
# description    :runs sql scripts
# author         :sjdillon
# date           :10/25/2018
# python_version :2.7.12
# ==========================================================================
import json
import re

import boto3

import config
from utils import *

logging.basicConfig(format="%(asctime)s - %(thread)s - %(levelname)s - %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_client(service, region):
    client = boto3.client(service, region_name=region)
    return client


class Runner:
    """ Runs database scripts """

    def __init__(self, event):
        if 'debug' in event and event['debug'] == True:
            self._set_logger(logging.DEBUG)
            self.debug = True
        else:
            self.debug = False

        self.region = os.environ['AWS_REGION']
        self.prefix1 = event['prefix1']
        self.prefix2 = event['prefix2']
        self.env = event['env']

        self.event = event
        self.event['service'] = 's3'
        self.bucket = event["s3bucket"]
        self.s3key = event["s3key"]
        self.schema = event['schema']
        self.actions = event['actions']

        # get boto clients
        self.client = get_client('s3', self.region)
        self.ssm = get_client('ssm', self.region)
        # get configs from config file
        self.load_config()
        self.migrations_filename = os.path.basename(self.s3key)
        self.local_migration_tar = self.local_migration_tar_format.format(working_dir=self.working_dir,
                                                                          migrations_filename=self.migrations_filename)
        self.sql_migrations_dir = self.sql_migrations_dir.format(working_dir=self.working_dir,
                                                                 flyway_dir=self.flyway_dir)

        # get connection info from SSM
        self.pw_key = self.pw_key_format.format(self.prefix1, self.prefix2, self.env)
        self.endpoint_key = self.endpoint_key_format.format(self.prefix1, self.prefix2, self.env, self.region)
        self.pw = self.get_param(self.pw_key)
        self.endpoint = self.get_param(self.endpoint_key)
        self.url = self.url_format.format(endpoint=self.endpoint, port=self.port)

    def load_config(self):
        for k, v in config.RUNNER_CONFIG.items():
            setattr(self, k, v)
        return self

    def _set_logger(self, level=logging.INFO):
        logger.setLevel(level)

    def format_response(self, in_response):
        """ create a dict with kv pairs from the tuple"""
        response = {}
        response['status'] = in_response[0]
        response['return_code'] = in_response[1]
        response['data'] = in_response[2]
        response['errors'] = self.redact(in_response[3])
        logger.info('flyway_output: {}'.format(json.dumps(response)))
        # remove large elements from return results to avoid CF problems
        del response['data']
        del response['errors']
        return response

    def get_param(self, key):
        """ get param from ssm """
        logger.debug('get_param: {}'.format(key))
        response = self.ssm.get_parameter(Name=key, WithDecryption=True)
        return response['Parameter']['Value']

    ## FLYWAY ##

    def get_flyway(self):
        """ copy flyway software to tmp and confirm """
        shutil.copytree('/opt/flyway-5.0.7/', self.flyway_dir)
        os.chmod("{}/flyway".format(self.flyway_dir), 0775)
        logger.debug('flyway_dir: {}'.format(self.flyway_dir))
        logger.debug(os.listdir(self.flyway_dir))
        flyway_version = run_cmd('{flyway_dir}/flyway -v'.format(flyway_dir=self.flyway_dir))[2]
        logger.debug('flyway_version: {}'.format(flyway_version))
        status = 'Flyway' in str(flyway_version)
        logger.debug('flyway installed: {}'.format(status))
        return status

    def flyway_cmd(self, cmd):
        """ generic flyway command creator """
        conf = self.get_config()
        out = run_cmd('{flyway_dir}/flyway {cmd} {conf}'.format(flyway_dir=self.flyway_dir, cmd=cmd, conf=conf))
        response = self.format_response(out)
        response['flyway_command'] = cmd
        logger.debug('migrate output: {}'.format(response))
        return response

    def migrate(self):
        """ Migrates the schema to the latest version """
        return self.flyway_cmd(cmd='migrate')

    def info(self):
        """ Returns the details and status information about all the migrations """
        return self.flyway_cmd(cmd='info')

    def run_flyway_commands(self, cmds=[]):
        allowed_cmds = {'migrate', 'info', 'validate', 'repair'}
        status = True
        if set(cmds).issubset(allowed_cmds):
            results = []
            for cmd in cmds:
                result = self.flyway_cmd(cmd=cmd)
                logger.debug('_status: {}'.format(result['status']))
                # if any of the commands report failure, set the overall status to failure
                if not result['status']:
                    status = False
                results.append(result)
            return status, results
        else:
            raise Exception(
                'Check event actions list {}, one of the flyway commands is not in supported list {}'.format(cmds,
                                                                                                             allowed_cmds))
            return False

    def get_config(self):
        """ get database connection details """
        conf = "-url={url} -user={user} -password='{password}' -schemas={schemas}".format(url=self.url, user=self.user,
                                                                                          password=self.pw,
                                                                                          schemas=self.schema)
        return conf

    def get_file(self, bucket, key, region, filename=None):
        logger.debug('get_file: {} {}'.format(bucket, key))
        if filename is None:
            filename = '{}{}'.format(self.working_dir, key.split('/')[-1])
        response = self.client.download_file(Bucket=bucket, Key=key, Filename=filename)
        return response

    def get_migrations(self):
        """ get migration file from s3 """
        return self.get_file(self.bucket, self.s3key, self.region)

    def get_sql(self):
        """ get the sql scripts and move to execution folder """
        self.get_migrations()
        response = extract_file(self.local_migration_tar, self.sql_migrations_dir)
        return response

    def run(self):
        """ install flyway and run the sql scripts """
        import shutil
        if os.path.exists(self.working_dir):
            logger.debug('working dir exists, deleting')
            shutil.rmtree(self.working_dir)
        os.mkdir(self.working_dir)
        os.chdir(self.working_dir)
        logger.debug('listdir: {}'.format(os.listdir('.')))

        if self.debug:
            check_java()
        self.get_flyway()
        self.get_sql()

        # prepare return_response
        status, cmd_results = self.run_flyway_commands(self.actions)
        response = {}
        response['success'] = status
        response['results'] = cmd_results
        response['bucket'] = self.bucket

        logger.debug(json.dumps(response))
        return response

    def redact(self, line):
        """ remove password from error output """
        return re.sub(r"(?i)password='(\w+)'", "password='XXXXXXX", line)
