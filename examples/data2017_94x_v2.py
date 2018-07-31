import time
import traceback

from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email

if __name__ == "__main__":

    pds = ["MuonEG","SingleElectron","MET","SinglePhoton","SingleMuon","DoubleMuon","JetHT","DoubleEG","HTMHT"]
    letters = list("BCDEF")

    dataset_names = []
    for pd in pds:
        for letter in letters:
            dataset_names.append("/{}/Run2017{}-31Mar2018-v1/MINIAOD".format(pd,letter))


    for i in range(10000):


        total_summary = {}
        total_counts = {}
        for dsname in dataset_names:

            cmsswver = "CMSSW_9_4_6_patch1"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V09-04-16_946p1.tar.gz"
            pset = "psets_cms4/main_pset_V09-04-16.py"
            scramarch = "slc6_amd64_gcc630"

            try:

                task = CMSSWTask(
                        sample = DBSSample(dataset=dsname),
                        open_dataset = False,
                        events_per_output = 350e3,
                        output_name = "merged_ntuple.root",
                        tag = "CMS4_V09-04-16",
                        pset = pset,
                        pset_args = "data=True prompt=False",
                        scram_arch = scramarch,
                        cmssw_version = cmsswver,
                        condor_submit_params = {"use_xrootd":True},
                        tarfile = tarfile,
                        is_data = True,
                        publish_to_dis = True,
                        snt_dir = True,
                        special_dir = "run2_data2017/",
                )

                if not task.complete():
                    task.process()
            except:
                traceback_string = traceback.format_exc()
                print "Runtime error:\n{0}".format(traceback_string)
                send_email(subject="metis error", body=traceback_string)


            total_summary[dsname] = task.get_task_summary()

        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/", make_plots=False).do()

        time.sleep(1.*3600)

