#!/bin/bash

OUTPUTDIR=$1
OUTPUTNAME=$2
INPUTFILENAMES=$3
IFILE=$4
PSET=$5
CMSSWVERSION=$6
SCRAMARCH=$7
NEVTS=$8
FIRSTEVT=$9
EXPECTEDNEVTS=${10}
OTHEROUTPUTS=${11}
PSETARGS="${@:12}" # since args can have spaces, we take 10th-->last argument as one

# Make sure OUTPUTNAME doesn't have .root since we add it manually
OUTPUTNAME=$(echo $OUTPUTNAME | sed 's/\.root//')

echo -e "\n--- begin header output ---\n" #                     <----- section division
echo "OUTPUTDIR: $OUTPUTDIR"
echo "OUTPUTNAME: $OUTPUTNAME"
echo "INPUTFILENAMES: $INPUTFILENAMES"
echo "IFILE: $IFILE"
echo "PSET: $PSET"
echo "CMSSWVERSION: $CMSSWVERSION"
echo "SCRAMARCH: $SCRAMARCH"
echo "NEVTS: $NEVTS"
echo "EXPECTEDNEVTS: $EXPECTEDNEVTS"
echo "OTHEROUTPUTS: $OTHEROUTPUTS"
echo "PSETARGS: $PSETARGS"
# echo "CLASSAD: $(cat $_CONDOR_JOB_AD)"

echo "GLIDEIN_CMSSite: $GLIDEIN_CMSSite"
echo "hostname: $(hostname)"
echo "uname -a: $(uname -a)"
echo "time: $(date +%s)"
echo "args: $@"

echo -e "\n--- end header output ---\n" #                       <----- section division

if [ -f "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]; then
    echo "sourcing environment: source $OSG_APP/cmssoft/cms/cmsset_default.sh"
    source "$OSG_APP"/cmssoft/cms/cmsset_default.sh
elif [ -f /cvmfs/cms.cern.ch/cmsset_default.sh ]; then
    echo "sourcing environment: source /cvmfs/cms.cern.ch/cmsset_default.sh"
    source /cvmfs/cms.cern.ch/cmsset_default.sh
else
    echo "ERROR! Couldn't find either /cvmfs/cms.cern.ch/cmsset_default.sh or $OSG_APP/cmssoft/cms/cmsset_default.sh"
    exit 0
fi

export SCRAM_ARCH=${SCRAMARCH}


# holy crap this is a mess. :( why does PAT code have to do such insane
# things with paths?
# if the first file in the tarball filelist starts with CMSSW, then it is
# a tarball made outside of the full CMSSW directory, and must be handled
# differently
tarfile=package.tar.gz
if [ ! -z $(tar -tf ${tarfile} | head -n 1 | grep "^CMSSW") ]; then
    echo "this is a full cmssw tar file"
    tar xf ${tarfile}
    cd $CMSSWVERSION
    echo $PWD
    echo "Running ProjectRename"
    scramv1 b ProjectRename
    echo "Running `scramv1 runtime -sh`"
    eval `scramv1 runtime -sh`
    mv ../$PSET pset.py
    mv ../${tarfile} .
else
    echo "this is a selective cmssw tar file"
    eval `scramv1 project CMSSW $CMSSWVERSION`
    cd $CMSSWVERSION
    eval `scramv1 runtime -sh`
    mv ../$PSET pset.py
    if [ -e ../${tarfile} ]; then
        mv ../${tarfile} ${tarfile};
        tar xf ${tarfile};
    fi
    scram b
    [ -e package.tar.gz ] && tar xf package.tar.gz
fi


# # logging every 45 seconds gives ~100kb log file/3 hours
# dstat -cdngytlmrs --float --nocolor -T --output dsout.csv 180 >& /dev/null &

echo "process.maxEvents.input = cms.untracked.int32(${NEVTS})" >> pset.py
echo "if hasattr(process,'externalLHEProducer'):" >> pset.py
echo "    process.externalLHEProducer.nEvents = cms.untracked.uint32(${NEVTS})" >> pset.py
echo "set_output_name(\"${OUTPUTNAME}.root\")" >> pset.py
if [ "$INPUTFILENAMES" != "dummyfile" ]; then 
    echo "process.source.fileNames = cms.untracked.vstring([" >> pset.py
    for INPUTFILENAME in $(echo "$INPUTFILENAMES" | sed -n 1'p' | tr ',' '\n'); do
        INPUTFILENAME=$(echo $INPUTFILENAME | sed 's|^/hadoop/cms||')
        # INPUTFILENAME="root://xrootd.unl.edu/${INPUTFILENAME}"
        echo "\"${INPUTFILENAME}\"," >> pset.py
    done
    echo "])" >> pset.py
fi
if [ "$FIRSTEVT" -ge 0 ]; then
    # events to skip, event number to assign to first event
    echo "try:" >> pset.py
    echo "    if not 'Empty' in str(process.source): process.source.skipEvents = cms.untracked.uint32(max(${FIRSTEVT}-1,0))" >> pset.py
    echo "except: pass" >> pset.py
    echo "try:" >> pset.py
    echo "    process.source.firstEvent = cms.untracked.uint32(${FIRSTEVT})" >> pset.py
    echo "except: pass" >> pset.py
fi

echo "before running: ls -lrth"
ls -lrth 

echo -e "\n--- begin running ---\n" #                           <----- section division

cmsRun pset.py ${PSETARGS}

if [ "$?" != "0" ]; then
    echo "Removing output file because cmsRun crashed with exit code $?"
    rm ${OUTPUTNAME}.root
fi


echo -e "\n--- end running ---\n" #                             <----- section division

echo "after running: ls -lrth"
ls -lrth



echo -e "\n--- begin copying output ---\n" #                    <----- section division

echo "Sending output file $OUTPUTNAME.root"

COPY_SRC="file://`pwd`/${OUTPUTNAME}.root"
COPY_DEST="gsiftp://gftp.t2.ucsd.edu${OUTPUTDIR}/${OUTPUTNAME}_${IFILE}.root"
echo "Running: env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy -p -f -t 4200 --verbose --checksum ADLER32 ${COPY_SRC} ${COPY_DEST}"
env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy -p -f -t 4200 --verbose --checksum ADLER32 ${COPY_SRC} ${COPY_DEST} 
COPY_STATUS=$?
if [[ $COPY_STATUS != 0 ]]; then
    echo "Removing output file because gfal-copy crashed with code $COPY_STATUS"
    env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-rm --verbose ${COPY_DEST}
    REMOVE_STATUS=$?
    if [[ $REMOVE_STATUS != 0 ]]; then
        echo "Uhh, gfal-copy crashed and then the gfal-rm also crashed with code $REMOVE_STATUS"
    fi
fi

for OTHEROUTPUT in $(echo "$OTHEROUTPUTS" | sed -n 1'p' | tr ',' '\n'); do
    [ -e ${OTHEROUTPUT} ] && {
        NOROOT=$(echo $OTHEROUTPUT | sed 's/\.root//')
        gfal-copy -p -f -t 4200 --verbose file://`pwd`/${NOROOT}.root gsiftp://gftp.t2.ucsd.edu${OUTPUTDIR}/${NOROOT}_${IFILE}.root --checksum ADLER32
    }
done

echo -e "\n--- end copying output ---\n" #                      <----- section division

echo -e "\n--- begin dstat output ---\n" #                      <----- section division
# cat dsout.csv
echo -e "\n--- end dstat output ---\n" #                        <----- section division
# kill %1 # kill dstat

# cd ../
# echo "cleaning up"
# rm -rf CMSSW*

