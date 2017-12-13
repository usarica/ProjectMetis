import unittest
import os
import logging

from metis.Sample import Sample, DBSSample, DirectorySample, SNTSample, TextfileSample, DummySample
from metis.Constants import Constants
from metis.File import MutableFile
import metis.Utils as Utils

class SampleTest(unittest.TestCase):

    def test_instantiation(self):
        dsname = "/blah/blah/BLAH/"
        samp = Sample(dataset=dsname)
        self.assertEqual(samp.get_datasetname(), dsname)

    def test_failures(self):
        logging.getLogger("logger_metis").disabled = True
        # should return False since no dataset was provided!
        samp = Sample()
        self.assertEqual(samp.load_from_dis(), False)

    def test_get_nevents(self):
        samp = Sample(dataset="/blah/blah/BLAH/", gtag="mygtag")
        samp.info["nevts"] = 123
        self.assertEqual(samp.get_nevents(), 123)

    def test_get_files(self):
        samp = Sample(dataset="/blah/blah/BLAH/", files=["blah1.txt","blah2.txt"])
        self.assertEqual(samp.get_files(), ["blah1.txt","blah2.txt"])

    def test_get_globaltag(self):
        samp = Sample(dataset="/blah/blah/BLAH/", gtag="mygtag")
        self.assertEqual(samp.get_globaltag(), "mygtag")

    def test_sort_responses(self):
        responses = [
                {"timestamp": 2},
                {"timestamp": 1},
                {"timestamp": 3},
                ]
        def get_values(listofdicts):
            return list(map(lambda x: list(x.values())[0], listofdicts))
        samp = Sample(dataset="/blah/blah/BLAH/")
        self.assertEqual(get_values(samp.sort_query_by_timestamp(responses, descending=True)), [3,2,1])
        self.assertEqual(get_values(samp.sort_query_by_timestamp(responses, descending=False)), [1,2,3])
        self.assertEqual(samp.sort_query_by_timestamp({}), {})


class DBSSampleTest(unittest.TestCase):

    @unittest.skipIf(os.getenv("NOINTERNET"), "Need internet access")
    def test_queries(self):
        dsname = "/ZeroBias6/Run2017A-PromptReco-v2/MINIAOD"
        dbssamp = DBSSample(dataset=dsname)
        # make initial queries
        self.assertEqual(dbssamp.get_nevents(), 2109150)
        dbssamp.info["gtag"] = None # reset so we don't pull from cache
        self.assertEqual(dbssamp.get_globaltag(), "92X_dataRun2_Prompt_v4")
        self.assertEqual(dbssamp.get_native_cmssw(), "CMSSW_9_2_1")
        self.assertEqual(len(dbssamp.get_files()), 10)

        # pull from cache
        self.assertEqual(dbssamp.get_nevents(), 2109150)
        self.assertEqual(dbssamp.get_globaltag(), "92X_dataRun2_Prompt_v4")
        self.assertEqual(dbssamp.get_native_cmssw(), "CMSSW_9_2_1")
        self.assertEqual(len(dbssamp.get_files()), 10)

class DirectorySampleTest(unittest.TestCase):

    def test_instantiation(self):
        dsname = "/blah/blah/BLAH/"
        dirsamp = DirectorySample(dataset=dsname, location="/dummy/dir/")
        self.assertEqual(len(dirsamp.get_files()), 0)

    def test_set_files(self):
        dirsamp = DirectorySample(dataset= "/blah/blah/BLAH/", location="/dummy/dir/")
        fnames = ["/hadoop/cms/store/user/blah/file_1.root","/hadoop/cms/store/user/blah/file_2.root"]
        dirsamp.set_files(fnames)
        self.assertEqual(list(map(lambda x: x.get_name(), dirsamp.get_files())), fnames)

    def test_set_files_xrootd(self):
        dirsamp = DirectorySample(dataset= "/blah/blah/BLAH/", location="/dummy/dir/", use_xrootd=True)
        fnames = ["/hadoop/cms/store/user/blah/file_1.root","/hadoop/cms/store/user/blah/file_2.root"]
        fnames_nocms = ["/store/user/blah/file_1.root","/store/user/blah/file_2.root"]
        dirsamp.set_files(fnames)
        self.assertEqual(list(map(lambda x: x.get_name(), dirsamp.get_files())), fnames_nocms)

    def test_get_globaltag(self):
        dirsamp = DirectorySample(dataset= "/blah/blah/BLAH/", location="/dummy/dir/")
        dirsamp.info["gtag"] = "dummygtag"
        self.assertEqual(dirsamp.get_globaltag(), dirsamp.info["gtag"])

class SNTSampleTest(unittest.TestCase):

    @unittest.skipIf(os.getenv("NOINTERNET"), "Need internet access")
    def test_everything(self):
        nfiles = 5
        tag = "v1"
        dsname = "/DummyDataset/Dummy/TEST"
        basedir = "/tmp/{0}/metis/sntsample_test/".format(os.getenv("USER"))

        # make a directory, touch <nfiles> files
        Utils.do_cmd("mkdir -p {0} ; rm {0}/*.root".format(basedir))
        for i in range(1,nfiles+1):
            Utils.do_cmd("touch {0}/output_{1}.root".format(basedir,i))

        # push a dummy dataset to DIS using the dummy location
        # and make sure we updated the sample without problems
        dummy = SNTSample(
                dataset=dsname,
                tag=tag,
                read_only=True, # note that this is the default!
                )
        dummy.info["location"] = basedir
        dummy.info["nevents"] = 123
        dummy.info["gtag"] = "stupidtag"

        # will fail the first time, since it's read only
        updated = dummy.do_update_dis()
        self.assertEqual(updated, False)

        # flip the bool and updating should succeed
        dummy.read_only = False
        updated = dummy.do_update_dis()
        self.assertEqual(updated, True)

        # make a new sample, retrieve from DIS, and check
        # that the location was written properly
        check = SNTSample(
                dataset=dsname,
                tag=tag,
                )
        self.assertEqual(len(check.get_files()), nfiles)
        self.assertEqual(check.get_globaltag(),dummy.info["gtag"])
        self.assertEqual(check.get_nevents(), dummy.info["nevents"])
        self.assertEqual(check.get_location(), basedir)

class TextfileSampleTest(unittest.TestCase):

    def test_all(self):
        dsname = "/blah/blah/BLAH/"
        fname = "tfsampletest.tmp"

        # make a temporary file putting in some dummy filenames
        # to be picked up by TextfileSample
        mf = MutableFile(fname)
        mf.touch()
        nfiles = 3
        for i in range(1,nfiles+1): mf.append("ntuple{}.root\n".format(i))
        tfsamp = TextfileSample(dataset=dsname, filelist=fname)
        self.assertEqual(len(tfsamp.get_files()), nfiles)

        # clean up
        mf.rm()

class DummySampleTest(unittest.TestCase):

    def test_all(self):
        dsname = "/blah/blah/BLAH/"
        nfiles = 15

        s1 = DummySample(N=nfiles,dataset=dsname)
        self.assertEqual(len(s1.get_files()), nfiles)


if __name__ == "__main__":
    unittest.main()

