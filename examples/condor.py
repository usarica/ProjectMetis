from pprint import pprint

from metis.Utils import condor_q, get_hist

import math, sys, os

"""
Some simple uses of the condor_q API
"""


if __name__ == "__main__":

    # Do condor_q and print out a dict for the first job
    # By default, only shows jobs for $USER
    my_jobs = condor_q()
    pprint(my_jobs[0])
    print

    # Don't specify a user, so get all jobs
    all_jobs = condor_q(user="")
    pprint(all_jobs[0])
    print

    # Get all job statuses and print out a counts of the
    # different statuses
    all_statuses = [job["JobStatus"] for job in all_jobs]
    print get_hist(all_statuses)
