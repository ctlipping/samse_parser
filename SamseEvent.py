#!/usr/bin/env python

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
    
    def __repr__(self):
        """ Returns a more internally useful string representation of a SamseEvent
            void --> str """
        return "SamseEvent({0},{1},{2},{3},{4},{5},{6},{7})".format(self.node_name, self.partition,
                                                                   self.down_date, self.up_date.strip(),
                                                                   self.state, self.reason.strip(),
                                                                   self.duration_td, self.duration)


    def __str__(self):
        """ Returns an easier to understand string representation of a SamseEvent
            void --> str """
        return "{0} {1}-{2} {3} {4} {5}".format(self.node_name, self.down_date, self.up_date.strip(),
                                                self.state, self.reason.strip(), self.duration)

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
