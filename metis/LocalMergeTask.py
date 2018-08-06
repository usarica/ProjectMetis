import os

from metis.Task import Task
from metis.File import File, MutableFile
import metis.Utils as Utils

import ROOT as r

class LocalMergeTask(Task):
    def __init__(self, **kwargs):
        """
        Takes a list of input file paths and a single full absolute output filename
        and performs a single merge operation
        """
        self.input_filenames = kwargs.get("input_filenames", [])
        self.output_filename = kwargs.get("output_filename", [])
        self.io_mapping = kwargs.get("io_mapping", [])
        self.update_mapping()
        super(self.__class__, self).__init__(**kwargs)

    def get_inputs(self):
        return sum([x[0] for x in self.io_mapping], [])

    def get_outputs(self):
        return sum([x[1] for x in self.io_mapping], [])

    def update_mapping(self):
        if self.io_mapping: return
        self.io_mapping = [ 
                [
                    map(File,self.input_filenames),
                    [File(self.output_filename),]
                    ] 
                ]

    def process(self):
        done = all(map(lambda x: x.exists(), self.get_outputs()))
        self.logger.info("Begin processing")
        if not done:
            self.merge_function(self.get_inputs(), self.get_outputs()[0])
            pass
        self.logger.info("End processing")

    def merge_function(self, inputs, output):
        # make the directory hosting the output if it doesn't exist
        fdir = output.get_basepath()
        if not os.path.exists(fdir): Utils.do_cmd("mkdir -p {0}".format(fdir))

        # when merging 1 file, TFileMerger defaults to a special case
        # of just copying the file. this screws up because of an issue
        # in TUrl and leaves potentially big files in /tmp/ without cleaning
        # them up later, so do it nonlocally, sigh :(
        local = True
        if len(inputs) == 1: local = False
        fm = r.TFileMerger(local)
        fm.OutputFile(output.get_name())
        fm.SetFastMethod(True)
        fm.SetMaxOpenedFiles(400)
        fm.SetPrintLevel(0)
        ngood = 0
        for inp in inputs:
            ngood += fm.AddFile(inp.get_name(), False)
        if ngood != len(inputs):
            MutableFile(output).rm()
            raise RuntimeError("Trying to merge {0} files into {1}, but only {2} of them got included properly".format(len(inputs), output.get_name(), ngood))
        self.logger.info("Added {0} files to be merged".format(len(inputs)))
        fm.Merge()
        self.logger.info("Done merging files into {0}".format(output.get_name()))

        # ch = r.TChain("t")
        # for inp in inputs:
        #     ch.Add(inp.get_name())
        # self.logger.info("Added {0} files to be merged, containing {1} events".format(len(inputs),ch.GetEntries()))
        # ch.Merge(output.get_name(), "fast")
        # self.logger.info("Done merging files into {0}".format(output.get_name()))

if __name__ == "__main__":
    pass
