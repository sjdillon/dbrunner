#!python
# ==========================================================#
# title          :utils.py
# description    :helper functions
# author         :sjdillon
# date           :10/25/2018
# python_version :2.7.12
# ==========================================================================
import logging
import os
import shutil
import subprocess
import tarfile

logging.basicConfig(format="%(asctime)s - %(thread)s - %(levelname)s - %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_file(filepath, outpath, ext=".sql"):
    logger.debug(filepath)
    logger.debug(outpath)

    tar = tarfile.open(filepath)
    tar.extractall()
    tar.close()
    for file in os.listdir("."):
        if file.endswith(ext):
            shutil.move(file, outpath)
    return check_extract(outpath)


def check_extract(filepath):
    files = os.listdir(filepath)
    file_cnt = len(files)
    logger.debug('check_extract - extracted files count: %i', file_cnt)
    logger.debug('check_extract extracted files: %s', files)
    if file_cnt == 0:
        logger.warn('no files extracted')


def run_cmd(cmd, throwOnError=False, secret=False):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    data = p.stdout.readlines()
    comm = p.communicate()
    check = comm[0]
    err = comm[1]
    rc = p.returncode
    if secret:
        cmd = redact(cmd)
    if rc > 0:
        logger.error('FAILURE: (%s) - return_code: %i; OUTPUT: %s; ERROR: %s', cmd, rc, data, err)
        if throwOnError == True:
            raise Exception('FAILURE: (%s) - return_code: %i; OUTPUT: %s; ERROR: %s', cmd, rc, data, err)
        return False, rc, data, err
    else:
        logger.debug('SUCCESSFUL: (%s)', cmd)
        return True, rc, data, err


def check_java():
    get_java_version = "java -version 2>&1 | head -n 1 "
    java_version = run_cmd(get_java_version)[2]
    logger.info('java_version: {}'.format(java_version))
    version_string = 'openjdk version'
    status = version_string in str(java_version)
    logger.info('java installed: {}'.format(status))
    return status


def check_port(address, port):
    import socket
    s = socket.socket()
    s.settimeout(3)
    try:
        s.connect((address, port))
        logger.info("Connected SUCCESSFULLY to resource %s on port %s" % (address, port))
        return True
    except socket.error, e:
        logger.info("Connection *** FAILED *** to resource %s on port %s (%s)" % (address, port, e))
        return False
