from __future__ import print_function

import time
import itertools
import json
import traceback

from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email


if __name__ == "__main__":

    dataset_names = [
            "/TT_TuneCUETP8M2T4_13TeV-powheg-pythia8/RunIISummer17MiniAOD-92X_upgrade2017_realistic_v7-v1/MINIAODSIM",
            "/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer17MiniAOD-92X_upgrade2017_realistic_v7-v1/MINIAODSIM",
            "/W1JetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer17MiniAOD-92X_upgrade2017_realistic_v8-v2/MINIAODSIM",
            ]

    for i in range(10000):


        total_summary = {}
        for dsname in dataset_names:

            cmsswver = "CMSSW_9_2_8"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-06_928.tar.gz"

            try:

                task = CMSSWTask(
                        sample = DBSSample(
                            dataset=dsname,
                            xsec=1.0,
                            kfact=1.0,
                            efact=1.0,
                            ),
                        events_per_output = 350e3,
                        output_name = "merged_ntuple.root",
                        tag = "CMS4_V00-00-06",
                        global_tag = "", # if global tag blank, one from DBS is used
                        pset = "main_pset.py",
                        pset_args = "data=False",
                        cmssw_version = cmsswver,
                        tarfile = tarfile,
                )
            
                task.process()
            except:
                traceback_string = traceback.format_exc()
                print("Runtime error:\n{0}".format(traceback_string))
                send_email(subject="metis error", body=traceback_string)


            total_summary[dsname] = task.get_task_summary()

        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/").do()

        # time.sleep(1.*3600)
        time.sleep(60.*60)

