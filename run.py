#!/usr/bin/env python3
"""Aggregate Freesurfer Longitudinal Pipeline results.

Run at the project level, search through analyses for the given gear (and 
optional gear version) to find all csv files.  Read and combine them into
individual files for each listed in CSV_WHITELIST (defined below).
"""


import os
import sys
import tempfile
import json
import logging
import re
import pandas as pd

import flywheel


CSV_WHITELIST = ['_aparc_thick_left.csv', 
                 '_aparc_thick_right.csv',
                 '_aparc_area_left.csv', 
                 '_aparc_area_right.csv', 
                 '_aparc_vol_left.csv', 
                 '_aparc_vol_right.csv', 
                 '_aseg_vol.csv']

RETURN_VALUE = 0

with open('config.json') as cf:
    config_json = json.load(cf)
config = config_json['config']

fmt = '%(asctime)s.%(msecs)03d %(levelname)-8s [%(name)s %(funcName)s()]: '+\
      '%(message)s'
log_level = config['gear-log-level']
log_name = 'FreeSurfer/grp-14b'
logging.basicConfig(level=log_level, format=fmt, datefmt='%Y-%m-%d %H:%M:%S')
LOG = logging.getLogger(log_name)
LOG.critical(f'{log_name} starting with log level at {log_level}')

context = flywheel.GearContext()

try:
    destination_id = config_json['destination']['id']
    destination_type = config_json['destination']['type']
    LOG.info(f'Destination: type = {destination_type}, id = {destination_id}')

    fw = context.client

    dest_container = fw.get(destination_id)

    LOG.info(f"Destination's parent's id is {dest_container.parent.id}")
    LOG.info(f'Running at {dest_container.parent.type} level')

    # If this is None (not running at project level), there will be an exception
    project_id = dest_container.parents.project

    project = fw.get(project_id)
    LOG.info(f'Project is "{project.label}"')

except Exception as e:
    log.critical(e, exc_info=True)
    RETURN_VALUE = 1
    LOG.info(f'This gear must be run at the project level.')
    LOG.info(f'{log_name} returning {RETURN_VALUE}')
    sys.exit(RETURN_VALUE)

GEAR_NAME = config['name-of-gear']

GEAR_VERSION = ''
if 'version-of-gear' in config:
    GEAR_VERSION = config['version-of-gear']

STATE = 'complete'

TOTAL_COMPLETED_ANALYSES = 0
COMPLETED_SUBJECT_ANALYSES = 0
SUMMARY_MESSAGES = []

CSV_WHITELIST = [ project.label + name for name in CSV_WHITELIST ]

# intermediate data structure: lists of data frames for each CSV file
DF_DICT = { csv_name:[] for csv_name in CSV_WHITELIST }


def load_csv(analysis):
    """Load csv files into data frames"""

    global LOG, COMPLETED_SUBJECT_ANALYSES

    LOG.info('Info:')
    for kk,vv in analysis.info.items():
        LOG.info(f'  {kk:>30} : {vv.rstrip()}')

    csvs = [ x for x in analysis.files 
             if x.type == 'tabular data' and x.name in CSV_WHITELIST ]

    if len(csvs) > 0:

        COMPLETED_SUBJECT_ANALYSES += 1

        LOG.info('Yay! CSV files:')

        with tempfile.TemporaryDirectory() as tmpdirname:

            for cc in csvs:

                LOG.info(f'  Reading {cc.name}')
                path = tmpdirname + '/' + cc.name
                analysis.download_file(cc.name, path)
                DF_DICT[cc.name].append(pd.read_csv(path))

            LOG.info('')

    else:
        LOG.error(f'PROBLEM No CSV files, I am so sorry. So close.')


LOG.info(f'Gear name "{GEAR_NAME}"')

for s in project.subjects():

    COMPLETED_SUBJECT_ANALYSES = 0

    subject = fw.get_subject(s.id)
    LOG.info(f'Subject {subject.label} has {len(subject.analyses)} analyses' +
             f' to check for csv files')

    for analysis in subject.analyses:

        if analysis.gear_info.name == GEAR_NAME:
            LOG.info(f'FOUND job id {analysis.job.id}  ' +
                     f'analysis id {analysis.id}  ' +
                     f'gear version {analysis.gear_info.version}')

            if GEAR_VERSION == '' or analysis.gear_info.version == GEAR_VERSION:

                if analysis.job.state == STATE: 

                    if analysis.info:
                        if ('longitudinal-step' in analysis.info and 
                            'completed' in analysis.info['longitudinal-step']):

                            if 'analysis-regex' in config:
                                if not re.search(config['analysis-regex'],
                                    analysis.label):
                                    LOG.warning('analysis-regex "' +
                                        config['analysis-regex'] + 
                                        '" mismatch with analysis.label "'+
                                        analysis.label + '"')
                                    continue

                                else:
                                    LOG.info('analysis-regex "' +
                                        config['analysis-regex'] + 
                                        '" match with analysis.label "'+
                                        analysis.label + '"')

                            # here's the meat!
                            load_csv(analysis)

                        else:
                            LOG.warning('PROBLEM longitudinal-step not found')

                    else:
                        LOG.warning(f'PROBLEM analysis.info = {analysis.info}')

                else:
                    LOG.warning(f'PROBLEM job state = {analysis.job.state}')

            else:
                LOG.warning(f'IGNORING {GEAR_NAME} version ' + 
                            f'{analysis.gear_info.version}')

        else:
            LOG.warning(f'IGNORING {analysis.gear_info.name} gear analysis')

    msg = f'Subject {subject.label} had {COMPLETED_SUBJECT_ANALYSES} ' + \
          f'{GEAR_NAME} successful analyses'
    SUMMARY_MESSAGES.append(msg)

    TOTAL_COMPLETED_ANALYSES += COMPLETED_SUBJECT_ANALYSES

for msg in SUMMARY_MESSAGES:
    LOG.info(msg)
msg = f'Project {project.label} had {TOTAL_COMPLETED_ANALYSES} ' + \
      f'{GEAR_NAME} successful analyses'
LOG.info(msg)

if TOTAL_COMPLETED_ANALYSES > 0:

    DF_LIST = [pd.concat(DF_DICT[csv_name],ignore_index=True) 
               for csv_name in CSV_WHITELIST]

    for df,name in zip(DF_LIST,CSV_WHITELIST):
        LOG.info(f'Writing {name}')
        df.to_csv('output/' + name, index=False)

else:
    RETURN_VALUE = 1
    LOG.critical('No analyses found.')

if RETURN_VALUE == 0:
    LOG.info(f'{log_name} successfully completed!')
else:
    LOG.info(f'{log_name} failed')

LOG.info(f'{log_name} returning {RETURN_VALUE}')
sys.exit(RETURN_VALUE)


# vi:set autoindent ts=4 sw=4 expandtab : See Vim, :help 'modeline'
