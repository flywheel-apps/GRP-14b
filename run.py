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
from copy import deepcopy

import flywheel
from utils.fly.make_file_name_safe import make_file_name_safe


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
    os._exit(RETURN_VALUE)

GEAR_NAME = config['name-of-gear']

GEAR_VERSION = ''
if 'version-of-gear' in config:
    GEAR_VERSION = config['version-of-gear']

TOTAL_COMPLETED_ANALYSES = 0
COMPLETED_SUBJECT_ANALYSES = 0
SUMMARY_MESSAGES = []

# a dict of the subjects/sessions found and the number of times each was
# found (in the job's info):
#   {"subject.label": {"session.label": N, ...}, ...}
# n.b. Both subject label and session label were converted to "safe" directory
# names using e.g. make_file_name_safe(session.label, '_')
SUBJECTS_SESSIONS = {}

CSV_WHITELIST = [ project.label + name for name in CSV_WHITELIST ]

# intermediate data structure: lists of data frames for each CSV file
DF_DICT = { csv_name:[] for csv_name in CSV_WHITELIST }


def add_blank_cvs(msg, subject_label, analysis_job_id):
    """Add a blank frame becuase there was an error"""

    for key in DF_DICT.keys():
        # add an eror mesage instead of a spreadsheet to the list so that
        # a blank spreadsheet can be added later.
        DF_DICT[key].append([subject_label, analysis_job_id, msg])


def load_csv(analysis, job, subject_label):
    """Load csv files into data frames"""

    global LOG, COMPLETED_SUBJECT_ANALYSES

    LOG.info('Info:')
    for kk,vv in analysis.info.items():
        LOG.info(f'  {kk:>30} : {job.profile.total_time_ms} ms : {vv.rstrip()}')

        # keep count of all subjects and sessions that were seen
        if kk != 'longitudinal-step':

            if kk == 'BASE':
                subj = make_file_name_safe(subject_label, '_')
                sess = 'BASE'

            else:
                # The Freesuefer subject directory is made from the subject code
                # and the session label (after unsafe characters were removed)
                subj, sess = kk.split('-')

            if subj not in SUBJECTS_SESSIONS:
                SUBJECTS_SESSIONS[subj] = {}
                SUBJECTS_SESSIONS[subj][sess] = 1
            else:
                if sess not in SUBJECTS_SESSIONS[subj]:
                    SUBJECTS_SESSIONS[subj][sess] = 1
                else:
                    SUBJECTS_SESSIONS[subj][sess] += 1

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
        return ''

    else:
        msg = 'PROBLEM No CSV files'
        LOG.error(f'{msg}, I am so sorry. So close.')
        return msg


LOG.info(f'Gear name "{GEAR_NAME}"')
if GEAR_VERSION != '':
    LOG.info(f'Gear version "{GEAR_VERSION}"')

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

                if analysis.job.state == 'complete':

                    if analysis.info:
                        if 'longitudinal-step' in analysis.info:

                            if 'completed' in \
                                analysis.info['longitudinal-step']:

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

                                job = fw.get_job(analysis.job.id)

                                # here's the meat!
                                msg = load_csv(analysis, job, subject.label)
                                if 'PROBLEM' in msg:
                                    add_blank_cvs(msg, subject.label, 
                                                  analysis.job.id)

                            else:
                                msg = 'PROBLEM longitudinal-step is ' + \
                                    analysis.info['longitudinal-step']
                                LOG.warning(msg)
                                add_blank_cvs(msg, subject.label, 
                                              analysis.job.id)

                        else:
                            msg = 'PROBLEM longitudinal-step not found'
                            LOG.warning(msg)
                            add_blank_cvs(msg, subject.label, analysis.job.id)

                    else:
                        msg = f'PROBLEM analysis.info = {analysis.info}'
                        LOG.warning(msg)
                        add_blank_cvs(msg, subject.label, analysis.job.id)

                else:
                    msg = f'PROBLEM job state = {analysis.job.state}'
                    LOG.warning(msg)
                    add_blank_cvs(msg, subject.label, analysis.job.id)

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

    # get headers for each spreadsheet to create blank entries for failed runs
    # by finding a valid one in the liest
    blank_df = {}
    for key in DF_DICT.keys():  # for each spreadsheet
        for df in DF_DICT[key]:
            if isinstance(df, pd.core.frame.DataFrame):
                blank_df[key] = pd.DataFrame(columns = df.columns)
                blank_df[key].loc[0] = '-'   # set all values to '-'
                blank_df[key].loc[0][0] = project.label  # (study)

    # find all failed runs and insert blank spreadsheet entries (in place)
    for key in DF_DICT.keys():  # for each spreadsheet
        for ii, df in enumerate(DF_DICT[key]):
            if isinstance(df, list):
                DF_DICT[key][ii] = deepcopy(blank_df[key])
                DF_DICT[key][ii].loc[0][1] = df[0]  # subject.label (scrnum)
                DF_DICT[key][ii].loc[0][2] = f'job.id={df[1]} {df[2]}'
                # df[1] is analysis.job.id end df[2] is the rror message

    # Find all sessions for all subjects and add a blank if either is missing
    sessions = project.sessions()
    for session in sessions:
        session_label = make_file_name_safe(session.label, '_')
        subject_label = make_file_name_safe(session.subject.label, '_')
        if subject_label not in SUBJECTS_SESSIONS:
            LOG.error(f'Subject "{subject_label}", Session "{session_label}" '
                       'was not processed')
            # add blank to spreadsheets
            for key in DF_DICT.keys():  # for each spreadsheet
                DF_DICT[key].append((blank_df[key]))
                DF_DICT[key][-1].loc[0][1] = subject_label  # (scrnum)
                DF_DICT[key][-1].loc[0][2] = session_label # (visit)
        else:
            if session_label not in SUBJECTS_SESSIONS[subject_label]:
                LOG.error(f'Session "{session_label}" for Subject '
                          f'"{subject_label}" was not processed')
                # add blank to spreadsheets
                for key in DF_DICT.keys():  # for each spreadsheet
                    DF_DICT[key].append((blank_df[key]))
                    DF_DICT[key][-1].loc[0][1] = subject_label  # (scrnum)
                    DF_DICT[key][-1].loc[0][2] = session_label # (visit)
            else:
                num = SUBJECTS_SESSIONS[subject_label][session_label]
                if num > 1:
                    LOG.error(f'Session "{session_label}" for Subject '
                        f'"{subject_label}" was processed {num} times'
                        f", what's up with that?")

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
os.sys.exit(RETURN_VALUE)
