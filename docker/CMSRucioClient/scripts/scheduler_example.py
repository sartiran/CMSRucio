#! /bin/env python
"""
Just a simple model for the multiprocess setup of the sync application
"""

from __future__ import absolute_import, division, print_function

import argparse
from random import randint
from time import sleep

from mp_custom import multiprocessing
from multiprocessing_logging import install_mp_handler

from custom_logging import logging

def getConfig(filepath):

def grandchild_proc(name):
    """
    The grandchild process
    """
    logging.my_fmt(label=name)
    logging.my_lvl('VVERBOSE')

    logging.vverbose('Starting Grandchild %s', name)
    delay = randint(1, 10)
    sleep(delay)
    logging.vverbose('Finished Grandchild %s, slept for %s secs',
                     name, delay)
    #logging.getLogger().setLevel(logging.DEBUG)
    logging.debug('This is a Grandchild debug message: %s', name)
    return delay

def child_proc(name, poolsize):
    """
    The child process: this spakwns several grandchilds.
    """
    logging.my_fmt(label=name)
    logging.my_lvl('DEBUG')
    logging.verbose('Starting Child %s', name)
    delay = randint(1, 10)
    sleep(delay)
    logging.verbose('Child %s slept %s secs', name, delay)

    logging.debug('This is a Child debug message: %s', name)

    pool = multiprocessing.Pool(poolsize)
    how_many = randint(1, 10)
    logging.verbose('Child %s ready to spawn %s grand children with poolsize %d',
                    name, how_many, poolsize)
    grandchildren = []

    for grandchild in range(1, how_many + 1):
        gc_name = '%s-grandchild_%d' % (name, grandchild)
        logging.verbose('Spawning grandchild %s', gc_name)
        grandchildren.append({
            'name': gc_name,
            'proc': pool.apply_async(grandchild_proc, (gc_name,))
        })

    logging.verbose('Child %s waiting for grandchildren to finish',
                    name)
    for grandchild in grandchildren:
        grandchild['ret'] = grandchild['proc'].get()
        logging.vverbose('Child %s recovered grandchild %s with ret %s',
                         name, grandchild['name'], grandchild['ret'])

    logging.vverbose('Finished child %s, slept %d sec, spawned %d grandchildren',
                     name, delay, how_many)

    return (delay, how_many)

def parent(how_many, poolsize, subpoolsize):
    """
    The parent process
    """

    logging.vverbose('Starting Parent Process for %d children with poolsize %d',
                     how_many, poolsize)

    install_mp_handler()
    pool = multiprocessing.NDPool(poolsize)

    children = []
    for child in range(1, how_many + 1):
        name = 'child-%s' % child
        logging.vverbose('Parent spawning child %s', name)
        children.append({
            'name': name,
            'proc': pool.apply_async(child_proc, (name, subpoolsize))
        })

    logging.vverbose('Parent waiting for children to finish')

    while children:
        for child in children:
            if child['proc'].ready():
                child['ret'] = child['proc'].get()
                children.remove(child)
                logging.vverbose('Parent recovered child %s with ret %s',
                                 child['name'], child['ret'])
        sleep(3)

    logging.vverbose('Finished Parent')

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='''CLI for updating a CMS RSE''',
    )
    PARSER.add_argument('-v', '--verbose', dest='debug', action='store_true',
                        help='Define verbosity level')
    PARSER.add_argument('--pool', dest='pool', default=1,
                        help='number of parallel threads. Default 1.')
    PARSER.add_argument('--subpool', dest='subpool', default=1,
                        help='number of child parallel threads. Default 1.')
    PARSER.add_argument('--children', dest='children', default=1,
                        help='number of parallel threads. Default 1.')

    OPTIONS = PARSER.parse_args()

    logging.my_fmt(label='scheduler')
    logging.my_lvl('VVERBOSE')
    #logging.getLogger().setLevel(logging.VVERBOSE)

    parent(int(OPTIONS.children), int(OPTIONS.pool), int(OPTIONS.subpool))
