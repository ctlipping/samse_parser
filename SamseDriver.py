#!/usr/bin/env python

import sys
import subprocess
import argparse
from datetime import *
import time

from SamseEvent import *

def process_samse_output(samse_output):
    """ Processes a line of output from subprocess 'sacctmgr  show event'
        list[str] --> list[str]"""

    output_lines = samse_output.decode("utf-8").split("\n")
    return list(filter(lambda x: x != '', output_lines))

def pick_sort(events):
    """ Chooses how to sort the list of events.
        str, list[str], --> list[str]"""
    global cli_args
    sort_arg = cli_args["sort"]

    #we use lambda expressions to define what the list should be sorted based on
    if (sort_arg == "duration"):
        return sorted(events, key=lambda x: x.duration_td)
    elif (sort_arg == "name"):
        #this expression sorts the events first by number, then by name
        #because sorted() is a stable sort, this works.
        return sorted(sorted(events, key=lambda x: x.node_name), key=lambda x: x.node_name.partition)
    elif (sort_arg == "down_date"):
        return sorted(events, key=lambda x: x.down_date)
    elif (sort_arg is None or sort_arg == "reason"):
        return sorted(events, key=lambda x: x.reason)
    else:
        print("Unknown reason '{0}'".format(sort_arg))
        print("Valid reasons: {0}, {1}, {2}, {3} (default)".format("duration", "name", "down_date", "reason"))
        exit(1)

def parse_dicts(start_date,nodes_lost, time_lost):
    """ Parses the two dictionaries defined in main for whole-partition data
        Dict{int}, Dict{timedelta} --> void"""

    global cli_args

    if start_date is None:
        first_of_month = datetime(date.today().year, date.today().month, 1, 0,0,0)
    else:
        #if we provided a start date, process that and turn it into a datetime()
        sd_split = map(lambda x: int(x), start_date.split("=")[1].split("-"))
        first_of_month = datetime(sd_split[0], sd_split[1], sd_split[2], 0,0,0)

    for partition,val in time_lost.iteritems():
        if (cli_args["p"] is not None and cli_args["p"] not in partition):
            continue
        #time lost is calculated by:
        #1) taking the total amount of lost time (add up all timedeltas by partition)
        #2) multiply the number of nodes in the partition by the amount of time since
        #   the measurement period started (first of the month)
        #3) divide #1 by #2 and multiply by 100 to get a percent

        partition_size = subprocess.Popen(["sinfo", "-h","-o", "%D","-p", partition],
                                          stdout=subprocess.PIPE).communicate()[0].replace("\n", "")
        possible_time = int(partition_size) * total_seconds((datetime.now() - first_of_month))
        pct_lost = (float(total_seconds(time_lost[partition]) / float(possible_time))) * 100.0
        print("{0}:\t{3}/{4}\t{1}\t({2}% lost)".format(partition, time_lost[partition], round(pct_lost, 3),
                                                       nodes_lost[partition], partition_size))
        
def total_seconds(time_delta):
    """ Returns the total seconds held in a timedelta object because apparently
        they lacked the technology to do so in python 2.6.6
        timedelta --> int"""
    return (time_delta.days * 86400) + (time_delta.seconds) + int(round(time_delta.microseconds / 100000))

def args_handler(argv):
    """ Parses argv using the argparse library, returning a namespace containing the args.
        str --> vars() """
    #sets up argument parsing to allow start/end times and sort style
    parser = argparse.ArgumentParser()
    parser.add_argument("start", help="Start time for sacctmgr", nargs='?')
    parser.add_argument("end", help="End time for sacctmgr", nargs='?')
    parser.add_argument("--sort", help="Sort output by (reason,node_name,down_date)", nargs='?')
    parser.add_argument("--stats", help="Show statistics at the end", action="store_true")
    parser.add_argument("-p", help="Show only specified partition", nargs='?')
    return vars(parser.parse_args(argv))

def gen_samse_args(cli_args):
    """ Based on given command line arguments, convert these into arguments for 'sacctmgr'.
        vars() --> list(str)"""
    samse_args = ["sacctmgr", "show", "event", "-n"]
    try:
        if cli_args["start"] is not None:
            samse_args.append(cli_args["start"])
        if cli_args["end"] is not None:
            samse_args.append(cli_args["end"])
    except KeyError:
        pass
    return samse_args
    
def process_data(processed):
    """ Given a list of properly handled strings representing SamseEvents, convert those to SamseEvents
        while handling a number of possible fail cases.
        list(str) --> list(SamseEvent) """
    invalid_partitions = ["cf"]
    events_list = []
    print("processing data...")
    for event_str in processed:
        #there are a handful of monthly events that can be discarded.  this is a way to do that.
        if (event_str[11:27].strip() == ''):
            continue
        event = SamseEvent(event_str)
        #some partitions no longer exist because they have either been decomissioned or were temporary
        #situations.  we exclude those.
        if event.partition in invalid_partitions:
            continue
        events_list.append(SamseEvent(event_str))
    return events_list

def print_data(sorted_events_list):
    """ After sorting the event list, we print it out. While the first part resembles standard sacctmgr output,
        it is actually much more useful because all of the lines represent their own object.  In addition, this
        method generates the more useful stats.
        list(SamseEvent) --> void """
    global cli_args
    #dict to show how many nodes have gone down in the last reporting period per partition
    nodes_lost_per_partition = {
        'alice' : 0 ,
        'alsacc' : 0 ,
        'baldur1' : 0 ,
        'catamount' : 0 ,
        'cf1' : 0 ,
        'dirac1' : 0 ,
        'etna' : 0 ,
        'explorer' : 0 ,
        'hbar1' : 0 ,
        'jbei1' : 0 ,
        'lr2' : 0 ,
        'lr3' : 0 ,
        'lr4' : 0 ,
        'lr5' : 0 ,
        'mako' : 0 ,
        'mhg' : 0 ,
        'musigny' : 0 ,
        'nano1' : 0 ,
        'voltaire' : 0 ,
        'vulcan' : 0 ,
        'xmas' : 0 }
    #dict to show how much time has been lost due to down nodes per partition
    time_lost_per_partition = {
        'alice' : timedelta(0) ,
        'alsacc' : timedelta(0) ,
        'baldur1' : timedelta(0) ,
        'catamount' : timedelta(0) ,
        'cf1' : timedelta(0) ,
        'dirac1' : timedelta(0) ,
        'etna' : timedelta(0) ,
        'explorer' : timedelta(0) ,
        'hbar1' : timedelta(0) ,
        'jbei1' : timedelta(0) ,
        'lr2' : timedelta(0) ,
        'lr3' : timedelta(0) ,
        'lr4' : timedelta(0) ,
        'lr5' : timedelta(0) ,
        'mako' : timedelta(0) ,
        'mhg' : timedelta(0) ,
        'musigny' : timedelta(0) ,
        'nano1' : timedelta(0) ,
        'voltaire' : timedelta(0) ,
        'vulcan' : timedelta(0) ,
        'xmas' : timedelta(0) }
    print("NAME\t\tDownTime\t\tUpTime\t\t\tReason\t\t\t\tDuration")
    sel = sorted_events_list
    if (cli_args["p"] is not None):
        sel = list(filter(lambda x: x.partition == cli_args["p"], sorted_events_list))
    for event in sel:
        #if --stats not specified, prints normal parsed sacctmgr events
        if (cli_args["stats"] != True):
            print("{0}\t{1}\t{2}\t{3}\t{4}".format(event.node_name, event.down_date, event.up_date, 
                                                   event.reason, event.duration))
        if (event.state == "MAINT" or event.state == "MAINT*"):
            continue
        nodes_lost_per_partition[event.partition] += 1
        time_lost_per_partition[event.partition] += event.duration_td
    #if stats specified, prints stats but not the normal list
    if (cli_args["stats"] == True):
        parse_dicts(cli_args["start"],nodes_lost_per_partition, time_lost_per_partition)


#global variables are bad, but it's useful in this case to have the arguments
#defined globally. Only main should ever write to this variable, and only once.
cli_args = vars()

def export_event_list():
    """ Export a list of samse events.  Import SamseDriver then run this to get a list of 
        Samse Events.
        void --> List(SamseEvent) """
    print("fetching 'sacctmgr show event' output...")
    samse_output = subprocess.Popen(gen_samse_args(cli_args), stdout=subprocess.PIPE).communicate()[0]
    print("done")

    return process_data(process_samse_output(samse_output))
    

def main():

    #processes arguments
    global cli_args
    cli_args = args_handler(sys.argv[1:])

    #grabs output from 'sacctmgr show event [options]' and stores it
    events_list = export_event_list()

    print_data(pick_sort(events_list))

if __name__ == "__main__":
    main()
