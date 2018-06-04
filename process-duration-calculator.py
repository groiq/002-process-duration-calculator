#!/usr/bin/env python3
"""retrieves a logfile via email and prints process durations to a text file.

This script was written in order to track performance of a certain program.
That program auto-runs every night and sends a log to a gmail address.

This script retrieves the email, parses the log, calculates durations of all
subprocesses an prints the results to a text file.
It omits processes that take less than 0.1 seconds. It also marks the five
longest processes that have no subprocesses.

Note that this program is tailored to a specific log file with a specific
structure. Adapting it for your own purposes will require several changes
to the code.

TODO which part of the code might be interesting for someone else?
(get to know the code before running ... bla bla bla ...)

You can read arguments from file by typing ...

"""

import imaplib  # TODO which ones?
import email    # TODO which ones?
# from imaplib import IMAP4_SSL
# from email import message_from_bytes

from datetime import datetime, timedelta, time

import re   # TODO which ones?

from argparse import ArgumentParser
# TODO either do something useful with argparse or scrap it altogether

__author__ = "Hannes Alkin"
__copyright__ = "Copyright 2018 by Hannes Alkin"
__credits__ = "Hannes Alkin, Franz Eder"

__license__ = "GPL"
__version__ = "???" # which license?
__maintainer__ = "Hannes Alkin"
__email__ = "hannes@alkin.at"
__status__ = "Prototype"

# Parameters
# ----------

parser = ArgumentParser(fromfile_prefix_chars='@', 
                        description="calculate durations from a process log")
parser.add_argument("-v", "--verbose", action="store_true", 
                    help="increase output verbosity")
# add argument: flag for mail vs file
# add arguments for mail
# add argument for file
# TODO what arguments else?
args = parser.parse_args()

debug_msgs = []

if args.verbose:
    def verbose(line):
        debug_msgs.append(line)
else:
    def verbose(line):
        pass

date = datetime.now()   # assuming it's today's log I want

mintime = timedelta(0, 0, 100000)

# Mail parameters
im_server = "imap.gmail.com"
im_port = 993
im_uname = "name@email.com"
im_pwd = "password"

# The log is the *only* message that matches these criteria.
subject_contains = "erfolgreich"
query = "(Subject '{}' on {})".format(subject_contains, 
                                      date.strftime("%d-%b-%Y"))
one_of_these_folders = ("[Gmail]/AUTO","INBOX")

def read_from_mail():
    """Get data via E-Mail"""
    im = imaplib.IMAP4_SSL(im_server, im_port)
    im.login(im_uname, im_pwd)
    mail_found = False
    for folder in one_of_these_folders:
        im.select(folder)
        dummy, search_results = im.search(None, query)
        if not search_results == [b'']:   # if list of matches not empty
            mail_found = True
            dummy, data = im.fetch(search_results[0], "(RFC822)")
            break
    im.logout()

    if not mail_found:
        # Todo: if read_from_mail() doesn't work, try read_from_file()?
        output = open(outfile, "w", encoding="utf-8")
        output.write("ERROR: Mail not found.")
        output.close()
        exit()

    data = email.message_from_bytes(data[0][1])
    data = data.get_payload()[0].get_payload(decode=True).decode()
    data = data.split("<br>")
    # Input was an HTML mail with the relevant lines basically plain text,
    # but with <br>-Tags instead of newline characters. So I broke it down
    # by tag.
    return data


def read_from_file(filename):
    """Get data from a file

    From an earlier version, before I had implemented mail reading.
    Usage: data = read_from_file(filename)
    """
    # split filename into name and extension
    filename_exp = r""" (.+) (\.[^\.]+) """
    filename_eval = re.match(filename_exp, filename, re.X)
    if filename_eval:
        #print(filename_eval.groups())
        filename = filename_eval.group(1)
        extension = filename_eval.group(2)
    else:
        # filename should already have the right value
        extension = ""
    print("filename: {}; extension: {}".format(filename,extension))

    # open the file (handling extensions)
    try:
        data = open("{}{}".format(filename,extension), "r")
    except:
        extension = ".txt"
        # todo: try more extensions
        data = open("{}{}".format(filename,extension), "r")
    # If X is given as filename, first try to open X, then X.txt.
    # Todo: Handle the opposite case (file is X, but user types X.txt). 
    # What other cases are likely?
    return data

# data = read_from_mail()
# if data is None:
    # data = read_from_file("log.txt")

data = read_from_file("log.txt")

"""loop through data

Loop through input data and extract relevant information.
Make a dictionary of all processes.
Make a list of master processes. 
"""

for line in data:
    # get date from logfile
    if line.startswith("Message date:"):
        line = line.split()
        date = datetime.strptime(line[2], "%Y/%m/%d")
        print(date)
        break

date_string = date.strftime("%Y-%m-%d")
outfile_name = "performance-{}.txt".format(date_string)

# regexp for analyzing the file
# sample line:
#   Masterjob :  : RDS_LOAD_TABLES : Followed unconditional link : 
#   Start of job execution (2017/08/24 03:05:03.510)
regexp = r""" \s* 
        (\w+)           # 1: Parent process
         \s+ : \s+ : \s+    
        (?:             # optional part
            ([^:]+)     # 2: Process name (optional)
        \s+ : \s+ )?    
        ([^:]+)         # 3: comment
        \s : \s
        ([^:]+)         # 4: status
        \s+
        ( [(] \d{4} / \d{2} / \d{2} \s+         # 5: timestamp (date part)
        \d{2} : \d{2} : \d{2} [.] \d{3} [)] )   # 5: timestamp (time part)
        \s* """
zeitformat = "(%Y/%m/%d %H:%M:%S.%f)"

# variables for working through the file
proclist = dict()
master_procs = []
line_num = 0

for line in data:

    line_num += 1
    # line = line.rstrip()    # necessary if opening from file
    # verbose(line)
    
    eval = re.match(regexp, line, re.X)
    if not eval:
        continue

    # backup: how to check on the eval
    #if line_num < 30: # modify number to catch first few relevant lines
    #   print(line_num,line)
    #   for i in range(len(eval.groups())):
    #       print(i+1, eval.group(i+1))
    #sample eval:
    #1 Masterjob
    #2 Check Db connections
    #3 [nr=0, errors=0, exit_status=0, result=true]
    #4 Job execution finished
    #5 (2017/08/24 03:05:03.096)    
    
    if eval.group(2) is None: # handle entry for the master process
        parent = ""
        procname = eval.group(1)
        master_procs.append(procname)
        # note that this captures process name, not process id.
        # Might run into problems if we have duplicate *master* processes.
    else:
        parent = eval.group(1)
        procname = eval.group(2)
    status = eval.group(4)
    timestamp = datetime.strptime(eval.group(5), zeitformat)
    
    procid = procname

    # if line logs a process start, create dict entry
    if "Start of job execution" in status:
        
        # check if entry already there
        i = 1
        while procid in proclist:
            i += 1
            procid = procname + "_" + str(i)

        # create new entry
        proclist[procid] = dict()
        # ...and fill it
        proclist[procid]["parent"] = parent # do I need that field?
        proclist[procid]["procname"] = procname
        proclist[procid]["start.timestamp"] = timestamp
        proclist[procid]["subprocs"] = list()
        
        # Add the new entry to the list of child processes 
        # in the entry for the parent process
        if not parent == "": # to deal with entry for master process
            parentid = parent
            if not parentid in proclist:
                proclist[parentid] = dict()
                proclist[parentid]["procname"] = parent
                proclist[parentid]["subprocs"] = []
                master_procs.append(parentid)
            i = 1
            while "end.timestamp" in proclist[parentid]:
                i += 1
                parentid = parent + "_" + str(i)
            proclist[parentid]["subprocs"].append(procid)

    # if line logs a process finish: 
    # add to the dict entry that should already be there
    elif "Job execution finished" in status:
    
        # check if entry already there
        if not procid in proclist:
            debug_msgs.append("{}: {}".format(line_num,line))
            debug_msgs.append("Error: Process entry not found.")
            
        # check if the entry is already finished
        i = 1
        while "end.timestamp" in proclist[procid]:
            i += 1
            procid = procname + "_" + str(i)

        # fill new values
        proclist[procid]["end.timestamp"] = timestamp
            
    else:
        debug_msgs.append("{}: {}".format(line_num,line))
        debug_msgs.append("Error: neither start nor finish of a process")
        
    # test output during loop
    #verbose("{}: {}".format(procid, proclist[procid]))
    #for item in proclist:
    #   verbose("    {}: {}".format(item, proclist[item]))
    #verbose("List of top-level processes: {}".format(masterprocs))
        
# test output after loop
verbose("proclist after reading through file: ")
for item in proclist:
    verbose("{}: {}".format(item, proclist[item]))
verbose("List of top-level processes: {}".format(master_procs))

"""calculate durations

Loop through the dictionary.
Handle cases where the timestamps are incomplete.
Calculate durations.
Find the longest processes.
"""

durationlist = list()

for procid in proclist:

    proclist[procid]["duration"] = timedelta()

    # if times are not filled in, insert dummy times
    times_filled_in = True
    if "start.timestamp" in proclist[procid]:
        starttime = proclist[procid]["start.timestamp"]
    else:
        times_filled_in = False
        starttime = datetime.combine(date,time(23,59,59,0))
    if "end.timestamp" in proclist[procid]:
        endtime = proclist[procid]["end.timestamp"]
    else:
        times_filled_in = False
        endtime = datetime.combine(date,time(0,0,0,0))

    # if times are not filled in, try to get them from the subprocesses
    if not times_filled_in:
        for item in proclist[procid]["subprocs"]:
            if ("end.timestamp" in proclist[item]
                    and proclist[item]["end.timestamp"] > endtime):
                endtime = proclist[item]["end.timestamp"]
            if ("start.timestamp" in proclist[item]
                    and proclist[item]["start.timestamp"] < starttime):
                starttime = proclist[item]["start.timestamp"]

    # calculate duration for all processes
    if not starttime > endtime:
        proclist[procid]["duration"] = endtime - starttime
        # this skips Success Mail because that has no end and no subprocs,
        # so endtime is still at 00.00.
        # But since duration is created with default zero and never changed,
        # Success Mail is just logged as durationless and ignored.
        
    # To find the longest processes, we will collect the candidates 
    # in a list and sort them.
    # A process is excluded from the candidates
    # - if it has subprocesses (we want the longes *single* procs)
    # - if its parent has "Master" in its name (in that case it is a parent
    #   process "in disguise")
    # - We assume that only processes longer than a minute are candidates,
    #   so we can keep the list short for performance reasons
    if (proclist[procid]["duration"] > timedelta(0,60,0)
            and not proclist[procid]["subprocs"]
            and not "MASTER" in proclist[procid]["parent"]):
        durationlist.append((proclist[procid]["duration"],procid))
    
durationlist.sort()
durationlist.reverse()
    
for i in range(5):
    # mark the five longest processes
    procid = durationlist[i][1]
    proclist[procid]["rank"] = i
        
"""write output"""

def output_entry(procid, tree_level):
    if proclist[procid]["duration"] < mintime:
        return
        
    outstrings = []
    tab = " " * 4 * (tree_level-1)
    outstrings.append(tab)
    outstrings.append(proclist[procid]["procname"])
    base_line_length = 65
    separator_count = base_line_length - len(proclist[procid]["procname"])
    separator_count -= len(tab)
    outstrings.append("." * separator_count)
    duration = proclist[procid]["duration"]
    # duration = timedelta(days = duration.days, seconds = duration.seconds)
    duration = timedelta(seconds = duration.seconds)
    duration = format(duration)
    outstrings.append(duration)
    if "rank" in proclist[procid]:
        outstrings.append(" <<<<")
    
    output_line = "".join(outstrings)
    outfile.write(output_line)
    outfile.write("\n")
    # verbose(outstrings)
    
    for entry in proclist[procid]["subprocs"]:
        output_entry(entry, tree_level+1)

outfile = open(outfile_name, "w", encoding="utf-8")

for entry in master_procs:
    output_entry(entry, 1)
    outfile.write("\n")  # insert blank line between master processes
    
if debug_msgs:
    """write debug messages to output"""
    outfile.write("\n".join([("-"*79),"Debug messages",("-"*79),"",""]))
    for entry in debug_msgs:
        try:
            outfile.write(entry)
        except:
            outfile.write(format(entry))
        outfile.write("\n")
        


outfile.close()