import time
import os

import datetime

from metis.File import MutableFile
from metis.Sample import FilelistSample
from metis.CMSSWTask import CMSSWTask

"""
This script submits CMSSW jobs to different sites. You can switch
between a dummy pset and a cms4 pset.

* Run this script once to submit jobs for the current day
* Wait 15 minutes
* Run it again. Some red should be green now.
"""

def print_summary_string(statuses):
    for site,done in sorted(statuses.items()):
        col = "\033[00;32m"
        if not done:
            col = "\033[00;31m"
        print "{}{}\033[0m  ".format(col,site),
    print

def get_task_fast(daystr,site):
    # dummy pset -- 1-5mins
    return CMSSWTask(
            sample = FilelistSample(
                dataset="/SiteTest/{}/TEST".format(site),
                # copied a single miniaod file to hadoop and put the proper event count here
                filelist=[["/store/user/namin/test/68753E9C-6D5E-E811-BC40-24BE05C4D821.root", 3334]],
                ),
            output_name = "output.root",
            tag = "v0",
            pset = "pset_dummy.py",
            cmssw_version = "CMSSW_9_4_9",
            special_dir = "metis_site_tests/{}/".format(daystr),
            scram_arch = "slc6_amd64_gcc630",
            condor_submit_params = {"sites":site},
    )

def get_task_cms4(daystr,site):
    # cms4 task -- 15-30mins
    return CMSSWTask(
            sample = FilelistSample(
                dataset="/SiteTest/{}/CMS4".format(site),
                # copied a single miniaod file to hadoop and put the proper event count here
                filelist=[["/store/user/namin/test/68753E9C-6D5E-E811-BC40-24BE05C4D821.root", 3334]],
                ),
            output_name = "output.root",
            tag = "v0_{}".format(daystr),
            pset = "psets_cms4/main_pset_V09-04-17_fast.py",
            pset_args = "data=False",
            cmssw_version = "CMSSW_9_4_9",
            special_dir = "metis_site_tests/{}/".format(daystr),
            scram_arch = "slc6_amd64_gcc630",
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V09-04-19_949.tar.gz",
            condor_submit_params = {"sites":site},
            recopy_inputs = True,
    )

if __name__ == "__main__":

    # daystr = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    daystr = datetime.datetime.now().strftime("%Y-%m-%d")

    sites = [

            "T2_US_UCSD",
            "T2_US_Caltech",
            "T2_US_Wisconsin",
            "T2_US_MIT",
            "T2_US_Nebraska",
            "T2_US_Purdue",
            "T2_US_Vanderbilt",
            "T2_US_Florida",
            "T3_US_UCSB",
            "T3_US_OSG",
            "T3_US_Baylor",
            "T3_US_Colorado",
            "T3_US_NotreDame",
            "UAF",
            "T3_US_UCR",

            # "T3_US_ANL",
            # "T3_US_Brown",
            # "T3_US_Cornell",
            # "T3_US_FIT",
            # "T3_US_FIU",
            # "T3_US_FNALLPC",
            # "T3_US_FSU",
            # "T3_US_HEPCloud",
            # "T3_US_MIT",
            # "T3_US_Minnesota",
            # "T3_US_NERSC",
            # "T3_US_NEU",
            # "T3_US_OSU",
            # "T3_US_PSC",
            # "T3_US_Princeton_ARM",
            # "T3_US_PuertoRico",
            # "T3_US_Rice",
            # "T3_US_Rutgers",
            # "T3_US_SDSC",
            # "T3_US_TACC",
            # "T3_US_TAMU",
            # "T3_US_TTU",
            # "T3_US_UB",
            # "T3_US_UCD",
            # "T3_US_UIowa",
            # "T3_US_UMD",
            # "T1_US_FNAL",

            ]

    # time.sleep(60)
    statuses = {}
    for site in sites:
        # task = get_task_fast(daystr,site)
        task = get_task_cms4(daystr,site)
        isdone = task.get_outputs()[0].exists()
        if not isdone:
            task.process()
        statuses[site] = isdone
    print_summary_string(statuses)
