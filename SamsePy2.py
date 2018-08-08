#!/usr/bin/env python

import sys
import subprocess
import argparse
from datetime import *
import time

class SamseEvent:
    def __init__(self, line):
        """ Processes a line of output from 'sacctmgr show event' into a parsed Event object
            str --> SamseEvent"""

        self.node_name = line[11:27].strip()
        self.partition = self.node_name.split(".")[1].replace("0", "").replace("+", "t")
        self.down_date = line[27:47].strip()
        self.up_date = line[47:67]
        self.state = line[67:74].strip()
        self.reason = line[74:105]
        self.duration_td = self.difference(self.down_date, self.up_date)
        has_star_ongoing = "*" if (self.up_date.strip() == "Unknown") else ""
        self.duration = has_star_ongoing + str(self.duration_td)


    def difference(self, down_date, up_date):
        """ Returns the time difference between two given dates.
            datetime, datetime --> timedelta"""
        
        ddt = self.to_dtime(down_date)
        #use a ternary statement to change "Unknown" to the current time
        udt = datetime.now() if (up_date.strip() == "Unknown") else self.to_dtime(up_date)
        return udt - ddt


    def to_dtime(self, dtime):
        """ Processes a string of format 'YYYY:MM:DDTHH:MM:SS' into a datetime object
            str --> datetime"""
        
        tmp = dtime.split("T")

        #use lambdas to stream process the strings into ints
        idate = list(map(lambda x: int(x), tmp[0].split("-")))
        itime = list(map(lambda x: int(x), tmp[1].split(":")))
        
        return datetime(idate[0], idate[1], idate[2], itime[0], itime[1], itime[2])

def process_samse_output(samse_output):
    """ Processes a line of output from subprocess 'sacctmgr  show event'
        list[str] --> list[str]"""

    output_lines = samse_output.decode("utf-8").split("\n")[2:]
    return list(filter(lambda x: x != '', output_lines))

def pick_sort(sort_arg, events):
    """ Chooses how to sort the list of events.
        str, list[str], --> list[str]"""
    if (sort_arg == "duration"):
        return sorted(events, key=lambda x: x.duration)
    elif (sort_arg == "name"):
        return sorted(sorted(events, key=lambda x: x.node_name.split(".")[0]), key=lambda x: x.node_name.split(".")[1])
    elif (sort_arg == "down_date"):
        return sorted(events, key=lambda x: x.down_date)
    elif (sort_arg is None or sort_arg == "reason"):
        return sorted(events, key=lambda x: x.reason)
    else:
        print("Unknown reason '{0}'".format(sort_arg))
        exit(1)

def parse_dicts(start_date,nodes_lost, time_lost):
    """ Parses the two dictionaries defined in main for whole-partition data
        Dict{int}, Dict{timedelta} --> void"""

    if start_date is None:
        first_of_month = datetime(date.today().year, date.today().month, 1, 0,0,0)
    else:
        sd_split = map(lambda x: int(x), start_date.split("=")[1].split("-"))
        first_of_month = datetime(sd_split[0], sd_split[1], sd_split[2], 0,0,0)

    for partition in time_lost:
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

def main():
    #sets up argument parsing to allow start/end times and sort style
    parser = argparse.ArgumentParser()
    parser.add_argument("start", help="Start time for sacctmgr", nargs='?')
    parser.add_argument("end", help="End time for sacctmgr", nargs='?')
    parser.add_argument("--sort", help="Sort output by (reason,node_name,down_date)", nargs='?')
    parser.add_argument("--stats", help="Show statistics at the end", action="store_true")

    #processes arguments
    cli_args = vars(parser.parse_args(sys.argv[1:]))
    samse_args = ["sacctmgr", "show", "event"]
    if cli_args["start"] is not None:
        samse_args.append(cli_args["start"])
    if cli_args["end"] is not None:
        samse_args.append(cli_args["end"])
    sort_arg = cli_args["sort"]

    #grabs output from 'sacctmgr show event [options]' and stores it
    print("fetching 'sacctmgr show event' output...")
    samse_output = subprocess.Popen(samse_args, stdout=subprocess.PIPE).communicate()[0]
    print("done")

    #processes the output of sacctmgr and converts every event into a SamseEvent object
    events_list = []
    print("processing data...")
    for event_str in process_samse_output(samse_output):
        if (event_str[11:27].strip() == ''):
            continue
        if ("cf0" in event_str):
            continue
        events_list.append(SamseEvent(event_str))

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

    #print results:
    print("NAME\t\tDownTime\t\tUpTime\t\t\tReason\t\t\t\tDuration")
    for event in pick_sort(sort_arg, events_list):
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

main()
