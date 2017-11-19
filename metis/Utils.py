from __future__ import print_function

import math
import time                                                
import os
try:
    import commands
except:
    # python3 compatibility
    import subprocess as commands
import logging
import datetime
import shelve
from collections import Counter

class cached(object): # pragma: no cover
    """
    decorate with
    @cached(default_max_age = datetime.timedelta(seconds=5*60))
    """
    def __init__(self, *args, **kwargs):
        self.cached_function_responses = {}
        self.default_max_age = kwargs.get("default_max_age", datetime.timedelta(seconds=0))
        self.cache_file = "cache.shelf"

    def __call__(self, func):
        def inner(*args, **kwargs):
            self.cached_function_responses = shelve.open(self.cache_file)
            max_age = kwargs.get('max_age', self.default_max_age)
            funcname = func.__name__
            key = "|".join([str(funcname), str(args), str(kwargs)])
            if not max_age or key not in self.cached_function_responses or (datetime.datetime.now() - self.cached_function_responses[key]['fetch_time'] > max_age):
                if 'max_age' in kwargs: del kwargs['max_age']
                res = func(*args, **kwargs)
                self.cached_function_responses[key] = {'data': res, 'fetch_time': datetime.datetime.now()}
            to_ret = self.cached_function_responses[key]['data']
            self.cached_function_responses.close()
            return to_ret
        return inner


def time_it(method): # pragma: no cover
    """
    Decorator for timing things will come in handy for debugging
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        # print '%r (%r, %r) %2.2f sec' % \
        #       (method.__name__, args, kw, te-ts)
        print('%r %2.2f sec' % \
              (method.__name__, te-ts))
        return result

    return timed

def do_cmd(cmd, returnStatus=False, dryRun=False):
    if dryRun:
        print("dry run: {}".format(cmd))
        status, out = 1, ""
    else:
        status, out = commands.getstatusoutput(cmd)
    if returnStatus: return status, out
    else: return out

def get_proxy_file():
    return "/tmp/x509up_u{0}".format(os.getuid())

def get_timestamp():
    # return current time as a unix timestamp
    return int(datetime.datetime.now().strftime("%s"))

def from_timestamp(timestamp):
    # return datetime object from unix timestamp
    return datetime.datetime.fromtimestamp(int(timestamp))

def timedelta_to_human(td):
    if td.days >= 2:
        return "{} days".format(td.days)
    else:
        return "{} hours".format(int(td.total_seconds()//3600))

def metis_base():
    return os.environ.get("METIS_BASE",".")+"/"

class CustomFormatter(logging.Formatter): # pragma: no cover
    # stolen from
    # https://stackoverflow.com/questions/1343227/can-pythons-logging-format-be-modified-depending-on-the-message-log-level
    err_fmt = '[%(asctime)s] [%(filename)s:%(lineno)s] [%(levelname)s] %(message)s'
    dbg_fmt = '[%(asctime)s] [%(filename)s:%(lineno)s] [%(levelname)s] %(message)s'
    info_fmt = '[%(asctime)s] %(message)s'

    def __init__(self, fmt="%(levelno)s: %(msg)s"):
        logging.Formatter.__init__(self, fmt)

    def format(self, record):
        format_orig = self._fmt
        if record.levelno == logging.DEBUG: self._fmt = CustomFormatter.dbg_fmt
        elif record.levelno == logging.INFO: self._fmt = CustomFormatter.info_fmt
        elif record.levelno == logging.ERROR: self._fmt = CustomFormatter.err_fmt
        result = logging.Formatter.format(self, record)
        self._fmt = format_orig
        return result

def setup_logger(logger_name="logger_metis"): # pragma: no cover
    """
    logger_name = u.setup_logger()
    logger = logging.getLogger(logger_name)
    logger.info("blah")
    logger.debug("blah")
    """


    # set up the logger to use it within run.py and Samples.py
    logger = logging.getLogger(logger_name)
    # if the logger is setup, don't add another handler!! otherwise
    # this results in duplicate printouts every time a class
    # calls setup_logger()
    if len(logger.handlers):
        return logger_name
    logger.setLevel(logging.DEBUG)
    customformatter = CustomFormatter()
    fh = logging.FileHandler(logger_name + ".log")
    fh.setLevel(logging.DEBUG) # DEBUG level to logfile
    ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG) # DEBUG level to console (for actual usage, probably want INFO)
    ch.setLevel(logging.INFO) # DEBUG level to console (for actual usage, probably want INFO)
    formatter = logging.Formatter('[%(asctime)s] [%(filename)s:%(lineno)s] [%(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(customformatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger_name

def condor_q(selection_pairs=None, user="$USER", cluster_id="", extra_columns=[]):
    """
    Return list of dicts with items for each of the columns
    - Selection pair is a list of pairs of [variable_name, variable_value]
    to identify certain condor jobs (no selection by default)
    - Empty string for user can be passed to show all jobs
    - If cluster_id is specified, only that job will be matched
    """

    # These are the condor_q -l row names
    columns = ["ClusterId", "JobStatus", "EnteredCurrentStatus", "CMD", "ARGS", "Out", "Err", "HoldReason"]
    columns.extend(extra_columns)

    # HTCondor mappings (http://pages.cs.wisc.edu/~adesmet/status.html)
    status_LUT = { 0: "U", 1: "I", 2: "R", 3: "X", 4: "C", 5: "H", 6: "E" }

    columns_str = " ".join(columns)
    selection_str = ""
    if selection_pairs:
        for sel_pair in selection_pairs:
            if len(sel_pair) != 2:
                raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
            selection_str += " -const '{0}==\"{1}\"'".format(*sel_pair)

    # Constraint ignores removed jobs ("X")
    cmd = "condor_q {0} {1} -constraint 'JobStatus != 3' -autoformat:t {2} {3}".format(user, cluster_id, columns_str,selection_str)
    output = do_cmd(cmd)

    jobs = []
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) == len(columns):
            tmp = dict(zip(columns, parts))
            tmp["JobStatus"] = status_LUT.get( int(tmp.get("JobStatus",0)),"U" ) if tmp.get("JobStatus",0).isdigit() else "U"
            jobs.append(tmp)
    return jobs

def condor_rm(cluster_ids=[]): # pragma: no cover
    """
    Takes in a list of cluster_ids to condor_rm for the current user
    """
    if cluster_ids:
        do_cmd("condor_rm {0}".format(",".join(map(str,cluster_ids))))

def condor_release(): # pragma: no cover
    do_cmd("condor_release {0}".format(os.getenv("USER")))

def condor_submit(**kwargs): # pragma: no cover
    """
    Takes in various keyword arguments to submit a condor job.
    Returns (succeeded:bool, cluster_id:int)
    fake=True kwarg returns (True, -1)
    """

    for needed in ["executable","arguments","inputfiles","logdir"]:
        if needed not in kwargs:
            raise RuntimeError("To submit a proper condor job, please specify: {0}".format(needed))

    params = {}

    params["universe"] = kwargs.get("universe", "Vanilla")
    params["executable"] = kwargs["executable"]
    params["arguments"] = " ".join(map(str,kwargs["arguments"]))
    params["inputfiles"] = ",".join(kwargs["inputfiles"])
    params["logdir"] = kwargs["logdir"]
    params["proxy"] = "/tmp/x509up_u{0}".format(os.getuid())
    params["timestamp"] = get_timestamp()

    if kwargs.get("use_xrootd", False): params["sites"] = kwargs.get("sites","T2_US_UCSD,T2_US_Wisconsin,T2_US_Florida,T2_US_Purdue,T2_US_Nebraska,T2_US_Caltech")
    else: params["sites"] = kwargs.get("sites","T2_US_UCSD")
    # if os.getenv("USER") in ["namin"] and "T2_US_UCSD" in params["sites"]:
    #     params["sites"] += ",UAF,UCSB"

    params["extra"] = ""
    if "selection_pairs" in kwargs:
        for sel_pair in kwargs["selection_pairs"]:
            if len(sel_pair) != 2:
                raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
            params["extra"] += '+{0}="{1}"\n'.format(*sel_pair)

    # if the sites only includes UAF, do not even bother giving a proxy
    params["proxyline"] = "x509userproxy={proxy}".format(proxy=params["proxy"]) if not(params["sites"] == "UAF") else ""

    template = """
universe={universe}
+DESIRED_Sites="{sites}"
executable={executable}
arguments={arguments}
transfer_executable=True
transfer_input_files={inputfiles}
transfer_output_files = ""
+Owner = undefined
+project_Name = \"cmssurfandturf\"
log={logdir}/{timestamp}.log
output={logdir}/std_logs/1e.$(Cluster).$(Process).out
error={logdir}/std_logs/1e.$(Cluster).$(Process).err
notification=Never
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
{proxyline}
{extra}
queue
    """

    if kwargs.get("return_template",False):
        return template.format(**params)

    if kwargs.get("fake",False):
        return True, -1

    with open(".tmp_submit.cmd","w") as fhout:
        fhout.write(template.format(**params))

    out = do_cmd("mkdir -p {0}/std_logs/  ; condor_submit .tmp_submit.cmd ".format(params["logdir"]))

    succeeded = False
    cluster_id = -1
    if "job(s) submitted to cluster" in out:
        succeeded = True
        cluster_id = int(out.split("submitted to cluster ")[-1].split(".",1)[0])
    else:
        raise RuntimeError("Couldn't submit job to cluster because:\n----\n{0}\n----".format(out))

    return succeeded, cluster_id

def file_chunker(files, files_per_output=-1, events_per_output=-1, flush=False):
    """
    Chunks a list of File objects into list of lists by
    - max number of files (if files_per_output > 0)
    - max number of events (if events_per_output > 0)
    Chunking happens in order while traversing the list, so
    any leftover can be pushed into a final chunk with flush=True
    """
   
    num = 0
    chunk, chunks = [], []
    for f in files:
        # if the current file's nevents would push the chunk
        # over the limit, then start a new chunk
        if (num >= files_per_output > 0) or (num+f.get_nevents() > events_per_output > 0):
            chunks.append(chunk)
            num, chunk = 0, []
        chunk.append(f)
        if (files_per_output > 0): num += 1
        elif (events_per_output > 0): num += f.get_nevents()
    # push remaining partial chunk if flush is True
    if (len(chunk) == files_per_output) or (flush and len(chunk) > 0):
        chunks.append(chunk)
        chunk = []
    # return list of lists (chunks) and leftover (chunk) which should
    # be empty if flushed
    return chunks, chunk

def make_tarball(fname, **kwargs): # pragma: no cover
    from UserTarball import UserTarball
    ut = UserTarball(name=fname, **kwargs)
    ut.addFiles()
    ut.close()
    return os.path.abspath(fname)

def update_dashboard(webdir=None, jsonfile=None): # pragma: no cover
    if not webdir:
        raise Exception("Um, we need a web directory, dude.")
    if not os.path.exists(os.path.expanduser(webdir)):
        mb = metis_base()
        do_cmd("mkdir -p {}/plots/".format(webdir), dryRun=False)
        do_cmd("cp -rp {}/dashboard/* {}/".format(mb,webdir), dryRun=False)
    if jsonfile and os.path.exists(jsonfile):
        do_cmd("cp {} {}/".format(jsonfile, webdir), dryRun=False)
        do_cmd("cp plots/* {}/plots/".format(webdir), dryRun=False)

def hsv_to_rgb(h, s, v): # pragma: no cover
    """
    Takes hue, saturation, value 3-tuple
    and returns rgb 3-tuple
    """
    if s == 0.0: v*=255; return [v, v, v]
    i = int(h*6.)
    f = (h*6.)-i; p,q,t = int(255*(v*(1.-s))), int(255*(v*(1.-s*f))), int(255*(v*(1.-s*(1.-f)))); v*=255; i%=6
    if i == 0: return [v, t, p]
    if i == 1: return [q, v, p]
    if i == 2: return [p, v, t]
    if i == 3: return [p, q, v]
    if i == 4: return [t, p, v]
    if i == 5: return [v, p, q]

def send_email(subject, body=""): # pragma: no cover
    email = do_cmd("git config --list | grep 'user.email' | cut -d '=' -f2")
    firstname = do_cmd("git config --list | grep 'user.name' | cut -d '=' -f2 | cut -d ' ' -f1")
    if "@" not in email:
        return
    do_cmd("echo '{0}' | mail -s '[UAFNotify] {1}' {2}".format(body, subject, email))

def get_stats(nums):
    length = len(nums)
    totsum = sum(nums)
    mean = 1.0*totsum/length
    sigma = math.sqrt(1.0*sum([(mean-v)*(mean-v) for v in nums])/(length-1))
    maximum, minimum = max(nums), min(nums)
    return {
            "length": length,
            "mean": mean,
            "sigma": sigma,
            "totsum": totsum,
            "minimum": minimum,
            "maximum": maximum,
            }

def get_hist(vals, do_unicode=True, width=50): # pragma: no cover
    d = dict(Counter(vals))
    maxval = max([d[k] for k in d.keys()])
    maxstrlen = max([len(k) for k in d.keys()])
    scaleto=width-maxstrlen
    fillchar = "*"
    verticalbar = "|"
    if do_unicode:
        fillchar = unichr(0x2589).encode('utf-8')
        verticalbar = "\x1b(0x\x1b(B"
    buff = ""
    for w in sorted(d, key=d.get, reverse=True):
        strbuff = "%%-%is %s %%s (%%i)" % (maxstrlen,verticalbar)
        if(maxval < scaleto):
            buff += strbuff % (w, fillchar * d[w], d[w])
        else: # scale to scaleto width
            buff += strbuff % (w, fillchar * max(1,int(float(scaleto)*d[w]/maxval)), d[w])
        buff += "\n"
    return buff

def nlines_back(n):
    """
    return escape sequences to move character up `n` lines
    and to the beginning of the line
    """
    return "\033[{0}A\r".format(n+1)

def print_logo(animation=True): # pragma: no cover

    main_template = """
          a          __  ___      / \    @
          b         /  |/  / ___  | |_   _   ___
      f d c e g    / /|_/ / / _ \ | __| | | / __|
      h   i   j   / /  / / |  __/ | |_  | | \__ \\
      k   l   m  /_/  /_/   \___|  \__| |_| |___/
      """

    d_symbols = {}
    d_symbols["v"] = unichr(0x21E3).encode('utf-8')
    d_symbols[">"] = unichr(0x21E2).encode('utf-8')
    d_symbols["<"] = unichr(0x21E0).encode('utf-8')
    d_symbols["o"] = unichr(0x25C9).encode('utf-8')
    d_symbols["#"] = unichr(0x25A3).encode('utf-8')

    d_mapping = {}
    d_mapping["a"] = d_symbols["o"]
    d_mapping["b"] = d_symbols["v"]
    d_mapping["c"] = d_symbols["#"]
    d_mapping["d"] = d_symbols["<"]
    d_mapping["e"] = d_symbols[">"]
    d_mapping["f"] = d_symbols["#"]
    d_mapping["g"] = d_symbols["#"]
    d_mapping["h"] = d_symbols["v"]
    d_mapping["i"] = d_symbols["v"]
    d_mapping["j"] = d_symbols["v"]
    d_mapping["k"] = d_symbols["#"]
    d_mapping["l"] = d_symbols["#"]
    d_mapping["m"] = d_symbols["#"]

    steps = [
            "a",
            "ab",
            "abc",
            "abcde",
            "abcdefg",
            "abcdefghij",
            "abcdefghijklm",
            ]

    if not animation:
        to_print = main_template[:]
        for key in d_mapping: to_print = to_print.replace(key, d_mapping[key])
        print(to_print)
    else:
        for istep,step in enumerate(steps):
            if istep != 0: print(nlines_back(7))
            to_print = main_template[:]
            for key in d_mapping:
                if key in step: to_print = to_print.replace(key, d_mapping[key])
                else: to_print = to_print.replace(key, " ")
            print(to_print)
            time.sleep(0.07)

if __name__ == "__main__":
    pass

