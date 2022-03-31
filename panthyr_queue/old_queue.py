#! /usr/bin/python3
# -*- coding: utf-8 -*-
"""Panthyr queue management

Project: Hypermaq/Panthyr
Dieter Vansteenwegen, VLIZ Belgium

Provides access to the queue (and settings) database table.
Allows one to add items to the queue, or return what is still queued.
Options:
-a: add to queue, possible values: measure/set_clock_gnss/set_station_params
-l: display a list of all (undone) items in the queue
-c: used by cron to queue measurements.
    First checks if currently between configured start/stop time of day, then adds meas to queue
-b: system has booted and needs to be set up
"""
import datetime  # to get the current time (to check if measurements should be made)
import getopt  # tool to parse arguments
import sys  # access to arguments

from p_connections import DbConnection  # access to the db itself
# Define constants."""
POSSIBLE_MANUAL_TASKS = (
    'backup_ftp', 'measure', 'set_clock_gnss', 'set_station_params', 'vacuum_',
)  # tasks that can be added manually using the option '-a'


def setup_logging(email=False):

    import logging
    import logging.handlers
    fmt = '%(asctime)s |%(levelname)-7s |%(module)-10.10s|%(lineno)-03s |%(funcName)s |%(message)s'
    datefmt = '%d/%m/%Y %H:%M:%S'  # defines format for timestamp

    log = logging.getLogger(__name__)  # creates root logger with name of script/module
    log.setLevel(logging.DEBUG)
    log.logThreads = 0  # tell log that it shouldn't gather thread data
    log.logProcesses = 0  # tell log that it shouldn't gather process data

    # create, configure and add streamhandler(s)
    h1 = logging.StreamHandler()  # handler to stdout for less important messages
    h1.setLevel(logging.DEBUG)
    h1.setFormatter(logging.Formatter(fmt, datefmt))
    log.addHandler(h1)  # add both handlers to the logger
    with connection() as db:
        if email and db.get_setting('email_enabled')[1]:
            from worker_libs.log_handlers import buffered_SMTP_Handler

            email_conf = {'recipient': '', 'server_port': '', 'user': '', 'password': ''}
            for i in email_conf:
                email_conf[i] = db.get_setting('email_' + i)[1]
            station_id = db.get_setting('station_id')[1]
            h2 = buffered_SMTP_Handler(
                host=email_conf['server_port'],
                fromaddress=email_conf['user'],
                toaddress=email_conf['recipient'],
                password=email_conf['password'],
                id=station_id,
            )  # handler for messages that should be emailed
            # # secure = ())
            h2.setLevel(logging.WARNING)
            h2.setFormatter(logging.Formatter(fmt, datefmt))
            log.addHandler(h2)

    return log


def print_tasks(tasks):
    if len(tasks) > 0:
        msg_task_len = 'There are {} tasks in the queue:'.format(len(tasks))
        line_of_dashes = '-' * 78

        # header
        print('\n{: ^78}'.format(msg_task_len))  # align center, line length 78
        print(line_of_dashes)  # padding
        print(
            ' {: ^8s} | {: ^5s} | {: ^20s} | {: ^35s}'.format(
            'PRIORITY', 'ID', 'ACTION', 'OPTIONS',
            ),
        )  # print headers for the columns
        print(line_of_dashes)  # padding

        # list of tasks
        for task in tasks:
            print(
                ' {0[0]: ^8} | {0[1]: ^5} | {0[2]: ^20} | {0[3]: ^35}'.format(
                task,
                ),
            )  # print PRIORITY, ID, ACTION, OPTIONS


"""Main"""
try:
    opts, arg = getopt.getopt(
        sys.argv[1:],
        'cbla:',
    )  # returns a list with each option,argument combination
    if len(opts) == 0:  # no valid options have been provided
        raise getopt.GetoptError('No valid options have been provided.')

    with DbConnection() as db:
        for option, argument in opts:
            """Next we check what option was chosen and execute it."""

            if option == '-l':  # list tasks in queue (will be expanded so that user can choose between all/done/not done tasks)
                reply = db.get_all_tasks()[1]
                print_tasks(reply)

            if option == '-b':  # mark in the db that we've restarted and not yet set up system time and location
                db.set_setting('system_set_up', 0)

            elif option == '-a':  # add an item to the queue
                """"option -a adds a task to the queue.
                The argument at least consists of the task and can be any member of POSSIBLE_MANUAL_TASKS.

                Priority (1 or 2) and (a comma separated list of) options can be added as well, in that order and separated by commas.
                If no priority or options are given, priority is set to 2, and options as a blank string.

                Example: queue.py -a measure,1
                This adds a measure task with priority 1
                """
                # log = setup_logging(email=False)
                lst_split_argument = argument.split(
                    ',',
                )  # returns a list of all comma-separated fields of the argument: [0] = task, [1] = priority, [2:] are options

                if lst_split_argument[
                        0
                ] not in POSSIBLE_MANUAL_TASKS:  # only these task are possible
                    print('No valid task for option -a was provided.')
                    exit()

                if len(lst_split_argument) > 1 and lst_split_argument[1] in (
                        '1', '2',
                ):  # a priority is provided
                    priority = lst_split_argument[1]
                else:
                    priority = 2

                if len(lst_split_argument) > 2:  # options are provided as well
                    options = ','.join(
                        lst_split_argument[2:],
                    )  # join al other fields into a comma-separated string
                else:
                    options = ''

                if db.add_to_queue(lst_split_argument[0], priority, options)[0]:
                    exit()
                else:
                    print('Error while adding task to queue.')

            elif option == '-c':  # triggered by cron
                """First we want to check if:
                - the setting 'manual' isn't set to 1 (running in manual mode)
                - the current time of day is between the 'measurements_start_hour' (included) and 'measurements_stop_hour' settings.
                After that we add a new measurement to the queue.
                """
                log = setup_logging(email=True)
                try:  # get settings for 'manual' and 'measurements_(start)/(stop)_hour'
                    manual = int(db.get_setting('manual')[1])
                    start = int(db.get_setting('measurements_start_hour')[1])
                    stop = int(db.get_setting('measurements_stop_hour')[1])
                except ValueError:  # one of the settings was not in the correct format (these should all be integers)
                    log.error('Invalid value for "manual", "measurements_start_hour" or \
                        "measurements_stop_hour" setting in the database')
                    exit()

                if not manual and (
                    start <= datetime.datetime.now().hour <
                    stop
                ):  # not in manual mode, and in the correct timeframe
                    db.add_to_queue('measure', 2, '')
                else:
                    exit()

except getopt.GetoptError:  # invalid arguments have been provided at the command line.
    print("""
    Provides access to the queue database table. Allows one to add items to the queue, or return what is still queued.

    Options:
    -a: add 1 task to the queue, possible values: measure/set_clock_gnss/set_station_params/vacuum_db
    -l: display a list of all (undone) items in the queue
    -c: used by cron to add measurements
    -b: used after sytem restart (when clock and location are not yet set up), sets system_setup to 0

    To add options and priority to a task (-a), use the argument task,priority,option
    Example: 'queue.py -a measure,1,option'
    """)
except KeyboardInterrupt:
    print('(QUEUE) Keyboard interrupt (CTRL+C) detected, now exiting...')
    exit()
