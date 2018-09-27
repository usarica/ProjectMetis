from __future__ import print_function

import os
import datetime
import time

def log_parser(fname):
    if fname.endswith(".err"):
        fname = fname.replace(".err", ".out")

    inheader = False
    indstat = False
    indstatnumbers = False
    colnames = []
    valuematrix = []
    d_log = {"args": {}, "dstat": {}}

    if not os.path.exists(fname):
        return d_log

    def try_float(x):
        if ":" in x:
            return x
        return float(x)

    with open(fname, "r") as fhin:
        for line in fhin:
            # figure out where we are
            if "begin header output" in line:
                inheader = True
            elif "end header output" in line:
                inheader = False
            elif "begin dstat output" in line:
                indstat = True
            elif "end dstat output" in line:
                indstat = False

            if inheader:
                if ":" in line:
                    argname, argval = map(lambda x: x.strip(), line.split(":", 1))
                    d_log["args"][argname] = argval

            if indstat:

                if indstatnumbers and "," in line:
                    valuematrix.append(map(try_float, line.strip().split(",")))
                    pass

                if line.startswith('"usr"'):  # warning, this assumes the line structure!
                    colnames = map(lambda x: x.replace('"',"").strip(), line.strip().split(","))
                    indstatnumbers = True

    # transpose the value matrix to get list of the columns,
    # then zip it together with column names and fill d_log
    for cname, cvals in zip(colnames, zip(*valuematrix)):
        # sometimes we have duplicate column names (writ, read),
        # but I only care about the first ones
        if cname in d_log["dstat"]:
            continue
        d_log["dstat"][cname] = list(cvals)

    return d_log

def infer_error(fname):
    fname = fname.replace(".out", ".err")
    to_return = ""
    if not os.path.exists(fname):
        return to_return
    with open(fname, "r") as fhin:
        data = fhin.read()
    if "----- Begin Fatal Exception" in data:
        exception = data.split("----- Begin Fatal Exception", 1)[-1].split("----- End Fatal Exception", 1)[0]
        exception_name = exception.split("An exception of category", 1)[-1].split()[0].replace("'","")
        last_lines = ", ".join(map(lambda x: x.strip(), exception.strip().splitlines()[-4:]))
        to_return = "[{0}] {1}".format(exception_name, unicode(last_lines[:500], errors="ignore"))
    return to_return

def get_timestamp(fname):
    fname = fname.replace(".err", ".out") # NOTE err -> out
    to_return = ""
    if not os.path.exists(fname):
        return to_return
    ts = None
    with open(fname, "r") as fhin:
        iline = 0
        for line in fhin:
            iline += 1
            if iline > 25: break
            if line.startswith("time"):
                tsstr = line.split(":")[-1].strip()
                ts = int(tsstr)
                break
    return ts

def get_site(fname):
    fname = fname.replace(".err", ".out") # NOTE err -> out
    to_return = ""
    if not os.path.exists(fname):
        return to_return
    site = None
    with open(fname, "r") as fhin:
        iline = 0
        for line in fhin:
            iline += 1
            if iline > 25: break
            if "GLIDEIN_CMSSite" in line:
                site = line.split(":")[-1].strip()
            elif line.startswith("hostname"):
                hostname = line.split(":")[-1].strip()
                if site: site += " ({})".format(hostname)
                break
    return site


def get_event_rate(fname): # pragma: no cover
    fname = fname.replace(".out", ".err")
    avg_rate = -1
    if not os.path.exists(fname):
        return avg_rate
    processingpairs = []
    with open(fname, "r") as fhin:
        for line in fhin:
            if line.startswith("Begin processing the"):
                record = float("".join([b for b in line.split("record")[0].split("the")[-1] if b in "1234567890"]))
                toparse = line.split()[-2]
                if ":" not in toparse: continue
                dtobj = datetime.datetime.strptime( toparse, "%H:%M:%S.%f" ).replace(year=2016)
                ts = time.mktime(dtobj.timetuple())+(dtobj.microsecond/1.e6)
                processingpairs.append([record,ts])
    try:
        records, tss = zip(*processingpairs)
        drecords = map(lambda x:x[0]-x[1],zip(records[1::2],records[::2]))
        dtss = map(lambda x:x[0]-x[1],zip(tss[1::2],tss[::2]))
        divs = map(lambda x:x[1]/x[0],zip(drecords,dtss))
        avg_rate = 1.0/(sorted(divs)[len(divs)//2])
        # Above 5 lines equivalent to the below two lines
        # I just didn't want numpy as a dependency
        # pp = np.array(processingpairs)
        # avg_rate = np.median(np.diff(pp[:,0])/np.diff(pp[:,1]))
    except IndexError:
        pass
    except ValueError:
        pass
    return avg_rate

if __name__ == "__main__":
    # logObj = log_parser("/home/jguiang/ProjectMetis/log_files/tasks/CMSSWTask_SinglePhoton_Run2017B-PromptReco-v1_MINIAOD_CMS4_V00-00-03/logs/std_logs/1e.1090614.0.out")
    # print(logObj["epoch"])
    # print(logObj.keys())

    print(infer_error("/home/users/namin/2017/ProjectMetis/tasks/CMSSWTask_DoubleEG_Run2017B-PromptReco-v2_MINIAOD_CMS4_V00-00-03/logs/std_logs//1e.1124399.0.err"))
    blah = log_parser("/home/users/namin/2017/ProjectMetis/tasks/CMSSWTask_DoubleEG_Run2017B-PromptReco-v2_MINIAOD_CMS4_V00-00-03/logs/std_logs//1e.1124399.0.out")
    # print blah["dstat"]["read"]
    print(blah["dstat"]["epoch"])
    # print blah
    pass

