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
        self.down_date = line[27:47].strip()
        self.up_date = line[47:67]
        self.state = line[67:74].strip()
        self.reason = line[74:105]
        self.duration_td = self.difference(self.down_date, self.up_date)
        has_star_ongoing = "*" if (self.up_date.strip() == "Unknown") else ""
        self.duration = has_star_ongoing + str(self.duration_td)


    def difference(self, down_date, up_date):
        """ Returns the time difference between two given dates.
            datetime, datetime --> TimeDelta"""
        
        ddt = self.to_dtime(down_date)
        #use a ternary statement to change "Unknown" to the current time
        udt = datetime.now() if (up_date.strip() == "Unknown") else self.to_dtime(up_date)
        return udt - ddt


    def to_dtime(self, dtime):
        """ Processes a string of format YYYY:MM:DDTHH:MM:SS into a datetime object
            str --> datetime"""
        
        tmp = dtime.split("T")

        #use lambdas to stream process the strings into ints
        idate = list(map(lambda x: int(x), tmp[0].split("-")))
        itime = list(map(lambda x: int(x), tmp[1].split(":")))
        
        return datetime(idate[0], idate[1], idate[2], itime[0], itime[1], itime[2])

def process_samse_output(samse_output):
    """ Processes a line of output from subprocess 'sacctmgr  show event'
        list[str] --> list[str]"""

    output_lines = samse_output.stdout.decode("utf-8").split("\n")[3:]
    return list(filter(lambda x: x != '', output_lines))

def pick_sort(sort_arg, events):
    """ Chooses how to sort the list of events.
        str, list[str], --> list[str]"""
    if (sort_arg == "name"):
        return sorted(sorted(events, key=lambda x: x.node_name.split(".")[0]), key=lambda x: x.node_name.split(".")[1])
    elif (sort_arg == "down_date"):
        return sorted(events, key=lambda x: x.down_date)
    elif (sort_arg is None or sort_arg == "reason"):
        return sorted(events, key=lambda x: x.reason)
    else:
        print("Unknown reason '{}'".format(sort_arg))
        exit(1)


def main():
    #sets up argument parsing to allow start/end times and sort style
    parser = argparse.ArgumentParser()
    parser.add_argument("start", help="Start time for sacctmgr", nargs='?')
    parser.add_argument("end", help="End time for sacctmgr", nargs='?')
    parser.add_argument("--sort", help="Sort output by (reason,node_name,down_date)", nargs='?')

    #processes arguments
    cli_args = vars(parser.parse_args(sys.argv[1:]))
    samse_args = ["sacctmgr", "show", "event"]
    if cli_args["start"] is not None:
        samse_args.append(cli_args["start"])
    if cli_args["end"] is not None:
        samse_args.append(cli_args["end"])
    sort_arg = cli_args["sort"]

    print("fetching 'sacctmgr show event' output...")
    samse_output = subprocess.run(samse_args, stdout=subprocess.PIPE)
    print("done")

    events_list = []
    print("processing data...")
    for event_str in process_samse_output(samse_output):
        events_list.append(SamseEvent(event_str))

    print("NAME\tDownTime\tUpTime\tReason\tDuration")
    for event in pick_sort(sort_arg, events_list):
        print("{}\t{}\t{}\t{}\t{}".format(event.node_name, event.down_date, event.up_date, event.reason, event.duration))


main()
