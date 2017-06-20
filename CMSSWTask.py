import commands
import os
import time
import pickle

from Constants import Constants
from Task import Task
from File import File, EventsFile
import Utils

class CMSSWTask(Task):
    def __init__(self, **kwargs):

        """
        This is a many-to-one workflow.
        In the end, input-output mapping might look like
        [
            [ ["i1.root","i2.root"], "o1.root" ],
            [ ["i3.root","i4.root"], "o2.root" ],
            [ ["i5.root"], "o3.root" ],
        ]
        """
        self.sample = kwargs.get("sample", None)

        # self.create_inputs = kwargs.get("create_inputs", [])
        self.min_completion_fraction = kwargs.get("min_completion_fraction", 1.0)
        self.open_dataset = kwargs.get("open_dataset", False)
        self.events_per_output = kwargs.get("events_per_output", -1)
        self.files_per_output = kwargs.get("files_per_output", -1)
        self.output_name = kwargs.get("output_name","output.root")
        self.output_dir = kwargs.get("output_dir",None)
        self.scram_arch = kwargs.get("scram_arch","slc6_amd64_gcc530")
        self.tag = kwargs.get("tag",None)

        # TODO
        self.global_tag = kwargs.get("global_tag")
        self.pset = kwargs.get("pset", None)
        self.cmssw_version = kwargs.get("cmssw_version", None)
        self.tarfile = kwargs.get("output_name",None)


        # If we didn't get an output directory, use the canonical format. E.g.,
        #   /hadoop/cms/store/user/namin/ProjectMetis/MET_Run2017A-PromptReco-v2_MINIAOD_CMS4_V00-00-03
        hadoop_user = os.environ.get("USER") # NOTE, might be different some weird folks
        self.output_dir = "/hadoop/cms/store/user/{0}/ProjectMetis/{1}_{2}/".format(hadoop_user,self.sample.get_datasetname().replace("/","_")[1:],self.tag)

        # Absolutely require some parameters
        if not self.sample:
            raise Exception("Need to specify a sample!")
        if not self.tag:
            raise Exception("Need to specify a tag to identify the processing!")
        if not self.tarfile or not self.cmssw_version or not self.pset:
            raise Exception("Need tarfile, cmssw_version, and pset to do stuff!")

        # I/O mapping (many-to-one as described above)
        self.io_mapping = []

        # Some storage params
        self.prepared_inputs = False
        self.made_taskdir = False
        self.job_submission_history = {}

        # Make a unique name from this task for pickling purposes
        self.unique_name = "{0}_{1}_{2}".format(self.get_task_name(),self.sample.get_datasetname().replace("/","_")[1:],self.tag)

        # Pass all of the kwargs to the parent class
        super(self.__class__, self).__init__(**kwargs)

        # Load from backup
        if kwargs.get("load_from_backup",True):
            self.load()

        # If we didn't get a globaltag, use the one from DBS
        # NOTE: This is declared as something to backup and placed after the
        # self.load() so that we don't spam get_globaltag() as it makes a 
        # DIS query each time. Would be smarter to remove need to back up
        # and put maybe a caching decorator for the config query in the
        # SamplesDBS class!
        if not self.global_tag: self.global_tag = self.sample.get_globaltag()

        # print self.job_submission_history

        # Can keep calling update_mapping afterwards to re-query input files
        self.update_mapping()

    def get_taskdir(self):
        task_dir = "{0}/tasks/{1}/".format(self.get_basedir(),self.unique_name)
        if not self.made_taskdir:
            Utils.do_cmd("mkdir -p {0}/logs/std_logs/".format(task_dir))
            self.made_taskdir = True
        return task_dir

    def backup(self):
        """
        Declare attributes to automatically back up (and later, load) here
        """
        to_backup = ["io_mapping","executable_path","pset_path", \
                     "package_path","prepared_inputs","made_taskdir", \
                     "job_submission_history","global_tag"]
        fname = "{0}/backup.pkl".format(self.get_taskdir())
        with open(fname,"w") as fhout:
            d = {}
            nvars = 0
            for tob in to_backup:
                if hasattr(self,tob): 
                    d[tob] = getattr(self,tob)
                    nvars += 1
            pickle.dump(d, fhout)
            self.logger.debug("Backed up {0} variables to {1}".format(nvars,fname))

    def load(self):
        fname = "{0}/backup.pkl".format(self.get_taskdir())
        if os.path.exists(fname):
            with open(fname,"r") as fhin:
                data = pickle.load(fhin)
                nvars = len(data.keys())
                for key in data:
                    setattr(self,key,data[key])
                self.logger.debug("Loaded backup with {0} variables from {1}".format(nvars,fname))

    def update_mapping(self, flush=False):
        """
        Given the sample, make the input-output mapping by chunking
        """
        # get set of filenames from File objects that have already been mapped
        already_mapped_inputs = set(map(lambda x: x.get_name(),self.get_inputs(flatten=True)))
        already_mapped_outputs = map(lambda x: x.get_index(),self.get_outputs())
        nextidx = 1
        if already_mapped_outputs:
            nextidx = max(already_mapped_outputs)+1
        original_nextidx = nextidx+0
        new_files = []
        # if dataset is "closed" and we already have some inputs, then
        # don't bother doing get_files() again (wastes a DBS query)
        if len(already_mapped_inputs) > 0 and not self.open_dataset:
            files  = []
        else:
            files = [f for f in self.sample.get_files() if f.get_name() not in already_mapped_inputs]
        flush = (not self.open_dataset) or flush
        prefix, suffix = self.output_name.rsplit(".",1)
        chunks, leftoverchunk = Utils.file_chunker(files, events_per_output=self.events_per_output, files_per_output=self.files_per_output, flush=flush)
        for chunk in chunks:
            output_path = "{0}_{1}.{2}".format(prefix,nextidx,suffix)
            output_file = EventsFile(output_path)
            nevents_in_output = sum(map(lambda x: x.get_nevents(), chunk))
            output_file.set_nevents(nevents_in_output)
            self.io_mapping.append([chunk, output_file])
            nextidx += 1
        if (nextidx-original_nextidx > 0):
            self.logger.debug("Updated mapping to have {0} more entries".format(nextidx-original_nextidx))


    def get_sample(self):
        return self.sample

    def get_outputdir(self):
        return self.output_dir

    def get_inputs(self, flatten=False):
        """
        Return list of lists, but only list if flatten is True
        """
        ret = [x[0] for x in self.io_mapping]
        if flatten: return sum(ret,[])
        else: return ret

    def get_completed_outputs(self):
        """
        Return list of completed output objects
        """
        return [o for o in self.get_outputs(flatten=True) if o.exists()]

    def get_outputs(self):
        """
        Return list of lists, but only list if flatten is True
        """
        return [x[1] for x in self.io_mapping]

    def complete(self, return_fraction=False):
        """
        Return bool for completion, or fraction if
        return_fraction specified as True
        """
        bools = map(lambda output: output.exists(), self.get_outputs())
        frac = 1.0*sum(bools)/len(bools)
        if return_fraction:
            return frac
        else:
            return frac >= self.min_completion_fraction

    def process(self):
        """
        Main logic for looping through (inputs,output) pairs. In this
        case, this is where we submit, resubmit, etc. to condor
        At the end, we call backup() for good measure!!
        """
        self.prepare_inputs()

        condor_job_dicts = self.get_running_condor_jobs()
        condor_job_indices = set([int(rj["jobnum"]) for rj in condor_job_dicts])
        for ins, out in self.io_mapping:
            out.update()
            index = out.get_index()
            on_condor = index in condor_job_indices
            done = out.exists() and not on_condor
            # done = True
            if done:
                self.logger.debug("This output ({0}) exists, skipping the processing".format(out))
                continue

            if not on_condor:
                pass
                # Keep a log of condor_ids for each output file that we've submitted
                succeeded, cluster_id = self.submit_condor_job(ins, out)
                if succeeded:
                    if index not in self.job_submission_history: self.job_submission_history[index] = []
                    self.job_submission_history[index].append(cluster_id)
                    self.logger.debug("Job for ({0}) submitted to {1}".format(out, cluster_id))

            else:
                this_job_dict = next(rj for rj in condor_job_dicts if int(rj["jobnum"]) == index)
                cluster_id = this_job_dict["ClusterId"]

                running = this_job_dict.get("JobStatus","I") == "R"
                idle = this_job_dict.get("JobStatus","I") == "I"
                held = this_job_dict.get("JobStatus","I") == "H"
                hours_since = abs(time.time()-int(this_job_dict["EnteredCurrentStatus"]))/3600.

                if running:
                    self.logger.debug("Job for ({0}) running for {1:.1f} hrs".format(out, hours_since))
                elif idle:
                    self.logger.debug("Job for ({0}) idle for {1:.1f} hrs".format(out, hours_since))
                elif held:
                    self.logger.debug("Job for ({0}) held for {1:.1f} hrs with hold reason: {2}".format(out, hours_since, this_job_dict["HoldReason"]))

                    if hours_since > 5.0:
                        self.logger.debug("Job for ({0}) removed for excessive hold time".format(out))
                        Utils.condor_rm([cluster_id])

        self.backup()

    def get_running_condor_jobs(self):
        """
        Get list of dictionaries for condor jobs satisfying the 
        classad given by the unique_name, requesting an extra
        column for the second classad that we submitted the job
        with (the job number)
        I.e., each task has the same taskname and each job
        within a task has a unique job num corresponding to the 
        output file index
        """
        return Utils.condor_q(selection_pairs=[["taskname",self.unique_name]], extra_columns=["jobnum"])


    def submit_condor_job(self, ins, out):

        outdir = self.output_dir
        outname_noext = self.output_name.rsplit(".",1)[0]
        inputs_commasep = ",".join(map(lambda x: x.get_name(), ins))
        index = out.get_index()
        pset_full = os.path.abspath(self.pset_path)
        pset_basename = os.path.basename(self.pset_path)
        cmssw_ver = self.cmssw_version
        scramarch = self.scram_arch
        nevts = -1
        executable = self.executable_path
        arguments = [ outdir, outname_noext, inputs_commasep,
                index, pset_basename, cmssw_ver, scramarch, nevts ]
        logdir_full = os.path.abspath("{0}/logs/".format(self.get_taskdir()))
        package_full = os.path.abspath(self.package_path)
        return Utils.condor_submit(executable=executable, arguments=arguments,
                inputfiles=[package_full,pset_full], logdir=logdir_full,
                selection_pairs=[["taskname",self.unique_name],["jobnum",index]],
                fake=False)


    def prepare_inputs(self):
        if self.prepared_inputs: return

        # need to take care of executable, tarfile, and pset
        self.executable_path = "{0}/executable.sh".format(self.get_taskdir())
        self.pset_path = "{0}/pset.py".format(self.get_taskdir())
        self.package_path = "{0}/package.tar.gz".format(self.get_taskdir())

        # take care of executable. easy.
        Utils.do_cmd("cp {0}/executables/condor_cmssw_exe.sh {1}".format(self.get_basedir(),self.executable_path))

        # add some stuff to end of pset (only tags and dataset name.
        # rest is done within the job in the executable)
        pset_location_in = self.pset
        pset_location_out = self.pset_path
        with open(pset_location_in,"r") as fhin:
            data_in = fhin.read()
        with open(pset_location_out,"w") as fhin:
            fhin.write(data_in)
            fhin.write( """
if hasattr(process,"eventMaker"):
    process.eventMaker.CMS3tag = cms.string('{tag}')
    process.eventMaker.datasetName = cms.string('{dsname}')
process.GlobalTag.globaltag = "{gtag}"\n\n""".format(
            tag=self.tag, dsname=self.get_sample().get_datasetname(), gtag=self.global_tag
            ))

        # take care of package tar file. easy.
        Utils.do_cmd("cp {0} {1}".format(self.tarfile,self.package_path))

        self.prepared_inputs = True



if __name__ == "__main__":
    pass