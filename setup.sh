#!/bin/bash

[ ! -z "$CMSSW_BASE" ] || {
    [ -e /cvmfs/ ] && {
        source /code/osgcode/cmssoft/cms/cmsset_default.sh
        cd /cvmfs/cms.cern.ch/slc6_amd64_gcc530/cms/cmssw/CMSSW_9_2_8/ && eval `scramv1 runtime -sh` && cd -
        source /cvmfs/cms.cern.ch/crab3/crab.sh
    }
}

export METIS_BASE=`pwd`

# CRAB screws up our PYTHONPATH. Go figure.
export PYTHONPATH=${METIS_BASE}:$PYTHONPATH

# Add some scripts to the path
export PATH=${METIS_BASE}/scripts:$PATH
