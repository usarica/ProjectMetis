from __future__ import print_function

import logging
import glob
import time
import datetime
import os

import scripts.dis_client as dis

from metis.Constants import Constants
from metis.Utils import setup_logger, cached
from metis.File import FileDBS, EventsFile

DIS_CACHE_SECONDS = 5*60
if os.getenv("NOCACHE"): DIS_CACHE_SECONDS = 0

class Sample(object):
    """
    General sample which stores as much information as we might want
    """

    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here
        self.info = {
            "tier": kwargs.get("tier", "CMS3"),
            "dataset": kwargs.get("dataset", None),
            "gtag": kwargs.get("gtag", None),
            "kfact": kwargs.get("kfact", 1.),
            "xsec": kwargs.get("xsec", 1.),
            "efact": kwargs.get("efact", 1.),
            "filtname": kwargs.get("filtname", None),
            "analysis": kwargs.get("analysis", None),
            "tag": kwargs.get("tag", None),
            "version": kwargs.get("version", None),
            "nevents_in": kwargs.get("nevents_in", None),
            "nevents": kwargs.get("nevents", None),
            "location": kwargs.get("location", None),
            "status": kwargs.get("status", None),
            "twiki": kwargs.get("twiki", None),
            "comments": kwargs.get("comments", None),
            "files": kwargs.get("files", []),
        }

        self.logger = logging.getLogger(setup_logger())

    def __repr__(self):
        return "<{0} dataset={1}>".format(self.__class__.__name__, self.info["dataset"])

    @cached(default_max_age = datetime.timedelta(seconds=DIS_CACHE_SECONDS))
    def do_dis_query(self, ds, typ="files"):

        self.logger.debug("Doing DIS query of type {0} for {1}".format(typ, ds))

        rawresponse = dis.query(ds, typ=typ, detail=True)
        response = rawresponse["response"]["payload"]
        if not len(response):
            self.logger.error("Query failed with response:" + str(rawresponse["response"]))

        return response

    def load_from_dis(self):

        (status, val) = self.check_params_for_dis_query()
        if not status:
            self.logger.error("[Dataset] Failed to load info for dataset %s from DIS because parameter %s is missing." % (self.info["dataset"], val))
            return False

        query_str = "status=%s, dataset_name=%s, sample_type=%s" % (Constants.VALID_STR, self.info["dataset"], self.info["type"])
        if self.info["type"] != "CMS3":
            query_str += ", analysis=%s" % (self.info["analysis"])
        if self.info["tag"]:
            query_str += ", cms3tag=%s" % (self.info["tag"])

        response = {}
        try:
            response = dis.query(query_str, typ='snt', detail=True)
            response = response["response"]["payload"]
            if len(response) == 0:
                self.logger.error(" Query found no matching samples for: status = %s, dataset = %s, type = %s analysis = %s" % (self.info["status"], self.info["dataset"], self.info["type"], self.info["analysis"]))
                return False

            if len(response) > 1:
                response = self.sort_query_by_timestamp(response)


            self.info["gtag"] = response[0]["gtag"]
            self.info["kfact"] = response[0]["kfactor"]
            self.info["xsec"] = response[0]["xsec"]
            self.info["filtname"] = response[0].get("filter_name", "NoFilter")
            self.info["efact"] = response[0]["filter_eff"]
            self.info["analysis"] = response[0].get("analysis", "")
            self.info["tag"] = response[0].get("tag", response[0].get("cms3tag"))
            self.info["version"] = response[0].get("version", "v1.0")
            self.info["nevts_in"] = response[0]["nevents_in"]
            self.info["nevts"] = response[0]["nevents_out"]
            self.info["location"] = response[0]["location"]
            self.info["status"] = response[0].get("status", Constants.VALID_STR)
            self.info["twiki"] = response[0].get("twiki_name", "")
            self.info["files"] = response[0].get("files", [])
            self.info["comments"] = response[0].get("comments", "")
            return True
        except:
            return False

    def do_update_dis(self):
        self.logger.debug("Updating DIS")
        query_str = "dataset_name={},sample_type={},cms3tag={},gtag={},location={},nevents_in={},nevents_out={},xsec={},kfactor={},filter_eff={},timestamp={}".format(
           self.info["dataset"], self.info["tier"], self.info["tag"], self.info["gtag"],
           self.info["location"], self.info["nevents_in"], self.info["nevents"],
           self.info["xsec"], self.info["kfact"], self.info["efact"], int(time.time())
        )

        response = {}
        try:
            succeeded = False
            response = dis.query(query_str, typ='update_snt')
            response = response["response"]["payload"]
            if "updated" in response and str(response["updated"]).lower() == "true":
                succeeded = True
            self.logger.debug("Updated DIS")
        except:
            pass

        if not succeeded:
            self.logger.debug("WARNING: failed to update sample using DIS with query_str: {}".format(query_str))
            self.logger.debug("WARNING: got response: {}".format(response))

        return succeeded

    def check_params_for_dis_query(self):
        if "dataset" not in self.info:
            return (False, "dataset")
        if "type" not in self.info:
            return (False, "type")
        if self.info["type"] != "CMS3" and "analysis" not in self.info:
            return (False, "analysis")
        return (True, None)

    def sort_query_by_timestamp(self, response, descending=True):
        if type(response) is list:
            return sorted(response, key=lambda k: k.get('timestamp', -1), reverse=descending)
        else:
            return response

    def get_datasetname(self):
        return self.info["dataset"]

    def get_nevents(self):
        if self.info.get("nevts", None):
            return self.info["nevts"]
        self.load_from_dis()
        return self.info["nevts"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]
        self.load_from_dis()
        self.info["files"] = [EventsFile(f) for f in glob.glob(self.info["location"])]
        return self.info["files"]

    def get_globaltag(self):
        if self.info.get("gtag", None):
            return self.info["gtag"]
        self.load_from_dis()
        return self.info["gtag"]



class DBSSample(Sample):
    """
    Sample which queries DBS (through DIS)
    for central samples
    """

    def set_selection_function(self, selection):
        """
        Use this to specify a function returning True for files we 
        want to consider only. Input to the selection function is
        the filename
        """
        self.selection = selection

    def load_from_dis(self):

        response = self.do_dis_query(self.info["dataset"], typ="files")
        fileobjs = [
                FileDBS(name=fdict["name"], nevents=fdict["nevents"], filesizeGB=fdict["sizeGB"]) for fdict in response
                if (not hasattr(self,"selection") or self.selection(fdict["name"]))
                ]
        fileobjs = sorted(fileobjs, key=lambda x: x.get_name())

        self.info["files"] = fileobjs
        self.info["nevts"] = sum(fo.get_nevents() for fo in fileobjs)
        # self.info["tier"] = self.info["dataset"].rsplit("/",1)[-1]


    def get_nevents(self):
        if self.info.get("nevts", None):
            return self.info["nevts"]
        self.load_from_dis()
        return self.info["nevts"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]
        self.load_from_dis()
        return self.info["files"]

    def get_globaltag(self):
        if self.info.get("gtag", None):
            return self.info["gtag"]
        response = self.do_dis_query(self.info["dataset"], typ="config")
        self.info["gtag"] = response["global_tag"]
        self.info["native_cmssw"] = response["release_version"]
        return self.info["gtag"]

    def get_native_cmssw(self):
        if self.info.get("native_cmssw", None):
            return self.info["native_cmssw"]
        response = self.do_dis_query(self.info["dataset"], typ="config")
        self.info["gtag"] = response["global_tag"]
        self.info["native_cmssw"] = response["native_cmssw"]
        return self.info["native_cmssw"]

class DirectorySample(Sample):
    """
    Sample which just does a directory listing to get files
    Requires a `location` to do an ls and a `dataset`
    for naming purposes
    :kwarg globber: pattern to select files in `location`
    :kwarg location: where to pick up files using `globber`
    :kwarg use_xrootd: if `True`, transform filenames into `/store/...`
    """

    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here
        needed_params = self.needed_params()
        if any(x not in kwargs for x in needed_params):
            raise Exception("Need parameters: {0}".format(",".join(needed_params)))

        self.globber = kwargs.get("globber", "*.root")
        self.use_xrootd = kwargs.get("use_xrootd", False)

        # Pass all of the kwargs to the parent class
        super(DirectorySample, self).__init__(**kwargs)

    def needed_params(self):
        return ["dataset","location"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]
        filepaths = glob.glob(self.info["location"] + "/" + self.globber)
        if self.use_xrootd:
            filepaths = [fp.replace("/hadoop/cms", "") for fp in filepaths]
        self.info["files"] = list(map(EventsFile, filepaths))

        return self.info["files"]

    def get_nevents(self):
        return self.info.get("nevts", 0)

    def get_globaltag(self):
        return self.info.get("gtag", "dummy_gtag")

    def set_files(self, fnames):
        if self.use_xrootd:
            fnames = [fp.replace("/hadoop/cms", "") for fp in fnames]
        self.info["files"] = list(map(EventsFile, fnames))

class SNTSample(DirectorySample):
    """
    Sample object which queries DIS for SNT samples
    """

    def __init__(self, **kwargs):

        self.typ = kwargs.get("typ", "CMS3")

        # Pass all of the kwargs to the parent class
        super(SNTSample, self).__init__(**kwargs)

        self.info["type"] = self.typ

    def needed_params(self):
        return ["dataset"]

    def get_nevents(self):
        if self.info.get("nevts", None):
            return self.info["nevts"]
        self.load_from_dis()
        return self.info["nevts"]

    def get_location(self):
        if self.info.get("location", None):
            return self.info["location"]
        self.load_from_dis()
        return self.info["location"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]
        filepaths = glob.glob(self.get_location() + "/" + self.globber)
        if self.use_xrootd:
            filepaths = [fp.replace("/hadoop/cms", "") for fp in filepaths]
        self.info["files"] = list(map(EventsFile, filepaths))

        return self.info["files"]

    def get_globaltag(self):
        if self.info.get("gtag", None):
            return self.info["gtag"]
        response = self.do_dis_query(self.info["dataset"], typ="config")
        self.info["gtag"] = response["global_tag"]
        self.info["native_cmssw"] = response["release_version"]
        return self.info["gtag"]


if __name__ == '__main__':

    s1 = DBSSample(dataset="/DoubleMuon/Run2017A-PromptReco-v3/MINIAOD")
    s1.set_selection_function(lambda x: "296/980/" in x)
    print(len(s1.get_files()))
    print(s1.get_nevents())

    s1 = DBSSample(dataset="/MET/Run2017A-PromptReco-v3/MINIAOD")
    print(len(s1.get_files()))

    s1 = DBSSample(dataset="/JetHT/Run2017A-PromptReco-v3/MINIAOD")
    print(len(s1.get_globaltag()))

    # ds = DirectorySample(
    #         dataset="/blah/blah/MINE",
    #         location="/hadoop/cms/store/user/namin/ProjectMetis/JetHT_Run2017A-PromptReco-v3_MINIAOD_CMS4_V00-00-03",
    #         )
    # print ds.get_files()
    # print ds.get_globaltag()
    # print ds.get_datasetname()
