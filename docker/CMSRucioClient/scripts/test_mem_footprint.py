#! /bin/env python
"""
Service synchronizing the sites
"""

from __future__ import absolute_import, division, print_function

import string
import functools
import time
import argparse
import sys
import os
import traceback
import copy
import yaml

from mp_custom import multiprocessing
from multiprocessing_logging import install_mp_handler
from cmsdatareplica import _replica_update
from custom_logging import logging
from syncaccounts import SYNC_ACCOUNT_FMT
from rucio.client.client import Client
from rucio.common.exception import RucioException
from instrument import timer, get_timing
from phedex import PhEDEx

def get_blocks_at_pnn(pnn, pcli, multi_das_calls=True):
    """
    Get the list of completed replicas of closed blocks at a site
    :pnn:  the phedex node name
    :pcli: phedex client instance

    returns a dictionnary with <block name>: <number of files>
    """

    # This is not optimal in terms of calls and time but
    # prevents dasgoclient to explode memory footprint
    if multi_das_calls:
        blocks_at_pnn = {}
        logging.notice('Getting blocks with multiple das calls. %s', list(string.letters + string.digits))
        for item in list(string.letters + string.digits):
            for block in pcli.list_data_items(pnn=pnn,  pditem='/' + item + '*/*/*'):
                 if block['block'][0]['is_open'] == 'n' and block['block'][0]['replica'][0]['complete'] == 'y':
                     blocks_at_pnn[block['block'][0]['name']] = block['block'][0]['files']
            logging.notice('Got blocks for %s', item)
        return blocks_at_pnn
    else:
        blocks_at_pnn = pcli.list_data_items(pnn=pnn)

    # list(string.letters + string.digits)
    return {
        item['block'][0]['name']: item['block'][0]['files']
        for item in blocks_at_pnn
        if item['block'][0]['is_open'] == 'n' and\
            item['block'][0]['replica'][0]['complete'] == 'y'
    }

if __name__ == '__main__':
     logging.my_lvl('DEBUG')

     print(get_blocks_at_pnn(
         pnn = 'T1_FR_CCIN2P3_Buffer',
         pcli = PhEDEx(),
         multi_das_calls=True
     ))

