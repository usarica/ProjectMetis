<img src="http://i.imgur.com/oYKKgyW.png" width="350">

[![Build Status](https://travis-ci.org/aminnj/ProjectMetis.png)](https://travis-ci.org/aminnj/ProjectMetis)
[![Coverage Status](https://coveralls.io/repos/github/aminnj/ProjectMetis/badge.png)](https://coveralls.io/github/aminnj/ProjectMetis)
[![Awesome Emojis](https://camo.githubusercontent.com/13c4e50d88df7178ae1882a203ed57b641674f94/68747470733a2f2f63646e2e7261776769742e636f6d2f73696e647265736f726875732f617765736f6d652f643733303566333864323966656437386661383536353265336136336531353464643865383832392f6d656469612f62616467652e737667)](https://gist.github.com/rxaviers/7360908)

As an overview, ProjectMetis seeks to host the following functionality:
* Ability to create arbitrary tasks with defined inputs and outputs using Python
* Ability to chain tasks into a queue, handling dependencies transparently
* Failure handling (where appropriate)

Concrete things that ProjectMetis can do:
* Submission of arbitrary CMSSW jobs on a dataset (or list of files) to condor
  * A dataset could be a published DBS dataset, a directory (containing files), or a dataset published on DIS
  * Arbitrary CMSSW jobs include CMS4
* Submit arbitrary "bash" jobs to condor
  * Coupled with the above, this facilitates babymaking
* By chaining a set of CMSSW tasks, can go from LHE to MINIAOD quite elegantly

In the process of fulfilling the above, ProjetMetis exposes some nice standalone API for:
* `condor_q`, `condor_submit`, etc.
* CRAB job submission/monitoring
* DIS integration (i.e., queries to internal SNT database, MCM, PhEDEx, DBS)
* File operations

## Installation and Setup
0. Checkout this repository
1. Set up environment via `source setup.sh`. Note that this doesn't overwrite an existing CMSSW environment if you already have one

## Example
To submit CMS4 jobs on a dataset, literally just need the dataset name, a pset, and a tarred up CMSSW environment.
Here's a quick preview, but there are more use case examples in `examples/`.
```python
from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser

def main():
    task = CMSSWTask(
            sample = DBSSample(dataset="/ZeroBias6/Run2017A-PromptReco-v2/MINIAOD"),
            events_per_output = 450e3,
            output_name = "merged_ntuple.root",
            tag = "CMS4_V00-00-03",
            pset = "pset_test.py",
            pset_args = "data=True prompt=True",
            cmssw_version = "CMSSW_9_2_1",
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-03_workaround.tar.gz",
            is_data = True,
    )
    
    # Do pretty much everything
    #  - get list of files (or new files that have appeared)
    #  - chunk inputs to construct outputs
    #  - submit jobs to condor
    #  - resubmit jobs that fail
    task.process()

    # Get a nice json summary of files, event counts, 
    # condor job resubmissions, log file locations, etc.
    # and push it to a web area (with dashboard goodies)
    StatsParser(data=total_summary, webdir="~/public_html/dump/metis_test/").do()

if __name__ == "__main__":

    # Do stuff, sleep, do stuff, sleep, etc.
    for i in range(100):
        main()

        # 1 hr power nap so we wake up refreshed
        # and ready to process some more data
        time.sleep(1.*3600)

        # Since everything is backed up, totally OK to Ctrl+C and pick up later
```


## Unit tests
Unit tests will be written in `test/` following the convention of appending `_t.py` to the class which it tests.
Workflow tests will also be written in `test/` following the convention of prepending `test_` to the name, e.g., `test_DummyMoveWorkflow.py`.

The full unit test suite is run using the executable `mtest` in `scripts/` (if Metis is set up properly, you need only execute the command `mtest`). For more fine-grained control, try
* for all class unit tests, execute the following from this project directory: `python -m unittest discover -p "*_t.py"`
* for all workflow tests, execute `python -m unittest discover -p "test_*.py"`
* for all tests, execute: `python -m unittest discover -s test -p "*.py"`

## Development
General workflow is 
* Make changes
* Test with `mtest` (or if it's a minor change, ignore this and let the continuous integration take care of testing)
* Submit a PR
Some design/development philosophies
* Every commit :blue_book: should contain at least :one: emoji representing the theme of the commit. For example,
:new: can be used with a completely new feature, :beetle: for bugs, :question: if you're unsure if something is broken
by the commit, :anguished: to express frustration, and :poop: for those super-special commits.

## TODO
Submit a Github issue for any bug report or feature request.

* SNTSample in principle allows anyone to update the sample on DIS. We don't want this for "central" samples, so rework this
* We have all the ingredients to replicate CRAB submission/status functionality, so do it
* Add more TODOs
* On dashboard, a unique sample is specified by (datasetname,tag), not just (datasetname) as it is right now. Fix this

