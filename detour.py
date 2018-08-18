#!/usr/bin/env python3
import argparse, datetime, json, logging, os, shutil, signal, sys, tempfile, time, re, pdb, textwrap, glob
import itertools as it, subprocess as sp, traceback as tb
import contextlib
from pathlib import Path
from collections import OrderedDict as ordict, defaultdict as ddict
import collections
import collections.abc # enough already

if not hasattr(Path, "write_text"):
  def read_text(self, encoding=None, errors=None):
    with self.open(mode='r', encoding=encoding, errors=errors) as f:
      return f.read()
  def write_text(self, data, encoding=None, errors=None):
    if not isinstance(data, str):
      raise TypeError('data must be str, not %s' % data.__class__.__name__)
    with self.open(mode='w', encoding=encoding, errors=errors) as f:
      return f.write(data)
  Path.read_text = read_text
  Path.write_text = write_text


logger = logging.Logger("detour")
logger.setLevel(logging.INFO)

# job dependencies are considered to be everything in the current directory, except
# hidden files and __pycache__ and notebooks
rsync_filter = """
  --include .d2filter
  --exclude .*
  --exclude __pycache__
  --exclude *.npz*-numpy.npy
  --exclude *.ipynb
""".strip().split() + ["--filter=: /.d2filter"]

local_runsdir = ".detours"


def make_that_dir(path):
  path = str(path) # convert pathlib Path (which has mkdir but exist_ok is py>=3.5)
  os.makedirs(path, exist_ok=True)

def pdb_post_mortem(fn):
  def wfn(*args, **kwargs):
    try:
      return fn(*args, **kwargs)
    except:
      tb.print_exc()
      pdb.post_mortem()
      raise
  return wfn

def study_labels(study):
  for path in sorted(glob.glob(".detours/*/config.json")):
    config = Config.from_file(path)
    if getattr(config, "study", None) == study:
      yield config.label

def dedup(xs):
  seen = set()
  ys = []
  for x in xs:
    if x not in seen:
      seen.add(x)
      ys.append(x)
  return ys

def abridge(s, maxlen=80):
  if len(s) < maxlen: return s
  k = (maxlen - 3) // 2
  return s[:k] + "..." + s[len(s) - k:]

def get_stdout_file(rundir):
  for pattern in "session.script slurm-*.out".split():
    expression = str(Path(rundir, pattern))
    paths = glob.glob(expression)
    if paths:
      path, = paths
      return path
  return None

def extract_exceptions(label):
  stdout_path = get_stdout_file(Path(local_runsdir, label))
  if not stdout_path:
    logger.warn("no stdout found for %s", label)

  re_ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
  re_exception = re.compile(r"^[A-Za-z_.]+(Error|Exception)\s*:\s*.*$")
  exceptions = []
  with open(stdout_path, "r") as stdout:
    for line in stdout:
      line = re_ansi_escape.sub("", line)
      match = re_exception.match(line)
      if match:
        exceptions.append(line.strip())
  exceptions = dedup(exceptions)
  def fn(ex):
    # attempt to filter out uninteresting exceptions while keeping kills
    if "CalledProcessError" in ex:
      if "Signals.SIG" in ex:
        return True
      return False
    return True
  exceptions = filter(fn, exceptions)
  return exceptions

def purgemany(labels):
  configs_by_remote = ddict(list)
  for label in labels:
    config = Config.from_label(label)
    configs_by_remote[config.remote].append(config)
  for remote, configs in configs_by_remote.items():
    remote = get_remote(remote)
    remote.purgemany(configs)

@pdb_post_mortem
def main(argv):
  _, subcommand, argv = argv[0], argv[1], argv[2:]

  # a subcommand that deals with multiple configs... TODO figure out how to organize this mess
  if subcommand == "pullstudy":
    study = argv[0]
    for label in study_labels(study):
      if not Path(local_runsdir, label, "terminated").exists():
        sp.check_call(["detour", "pull", label])
    sys.exit(0)
  if subcommand == "purgestudy":
    study = argv[0]
    purgemany(study_labels(study))
    sys.exit(0)
  if subcommand == "purgemany":
    purgemany(argv)
    sys.exit(0)
  if subcommand == "studystatus":
    study = argv[0]
    by_status = ddict(list)
    for label in study_labels(study):
      terminated_path = Path(local_runsdir, label, "terminated")
      if not terminated_path.exists():
        sp.check_call(["detour", "pull", label])
      if terminated_path.exists():
        status = terminated_path.read_text()
        if "CalledProcessError" in status:
          # uninformative exception in "detour run" command; extract the true exception from slurm-*.out
          exceptions = extract_exceptions(label)
          exceptions = [abridge(ex, 80) for ex in exceptions]
          status = "; ".join(exceptions)
      else:
        if sp.check_call(["detour", "status", label]):
          status = "not running"
        else:
          status = "dunno"
      by_status[status].append(label)
    for key, value in by_status.items():
      print(len(value), "runs:", key)
      if len(argv) > 1 and "--verbose" in argv:
        for label in value:
          print("  ", label)
    sys.exit(0)
  if subcommand == "pullremote":
    remote = argv[0]
    for path in sorted(glob.glob(".detours/*/config.json")):
      config = Config.from_file(path)
      if getattr(config, "remote", None) != remote:
        continue
      if not Path(Path(path).parent, "terminated").exists():
        sp.check_call(["detour", "pull", config.label])
    sys.exit(0)
  if subcommand == "fix_configs":
    for path in sorted(glob.glob(".detours/*")):
      label = Path(path).name
      assert re.match("^[0-9]{8}_[0-9]{6}_[0-9a-z]{32}$", label)
      config_path = Path(path, "config.json")
      if config_path.exists():
        config = Config.from_file(config_path)
      else:
        config = Config()
      group_path = Path(path, "tree", "group")
      if group_path.exists():
        study = group_path.read_text()
        config.underride(study=study)
      config.underride(remote="mila", label=label)
      config.to_file(config_path)
    sys.exit(0)
  # FIXME implement resubmit_sticklers to resubmit jobs that never started and aren't in the slurm queue (i.e. if not terminated, take job_id, check queue, resubmit)

  if subcommand not in subcommands:
    raise KeyError("unknown subcommand", subcommand)

  config = Config()
  argv = config.parse_args(argv)

  if subcommand in labeltaking_subcommands:
    if argv:
      label, argv = argv[0], argv[1:]
      # whatever the user specifies takes precedence over automatic config propagation arguments
      config.override(label=label)

  if hasattr(config, "label") and not getattr(config, "on_remote", 0):
    config.underride(Config.from_file(
      Path(local_runsdir, config.label, "config.json")))

  # finally, add defaults
  config.underride(Config.defaults)

  print("config", config.__dict__)
  # NOTE: in future there may be some subcommands that don't require a remote, e.g. list jobs and states

  assert config.remote
  remote = get_remote(config.remote)
  getattr(remote, subcommand)(config, *argv)


class Config(object):
  # in order to be able to invoke ourselves locally and remotely and in ways that must be robust to
  # several levels of string mangling, this object deals with what would otherwise just be flags.
  defaults = dict(label=None, study=None, remote=None, bypass=0, on_remote=0)

  def __init__(self, items=(), **kwargs):
    self.override(items, **kwargs)

  def override(self, items=(), **kwargs):
    if isinstance(items, Config):
      items = items.__dict__
    self.__dict__.update(items)
    self.__dict__.update(kwargs)

  def underride(self, items=(), **kwargs):
    if isinstance(items, Config):
      items = items.__dict__
    if isinstance(items, collections.abc.Mapping):
      items = items.items()
    for key, value in it.chain(kwargs.items(), items):
      self.__dict__.setdefault(key, value)

  def with_overrides(self, items=(), **kwargs):
    # `kwargs` overrides `items` overrides `self`
    config = Config(self)
    config.override(items, **kwargs)
    return config

  def with_underrides(self, items=(), **kwargs):
    # `self` overrides `kwargs` overrides `items`
    config = Config(self)
    config.underride(items, **kwargs)
    return config

  @staticmethod
  def from_file(path):
    return Config(json.loads(Path(path).read_text()))

  @staticmethod
  def from_label(label):
    return Config.from_file(Path(".detours", label, "config.json"))

  def to_file(self, path):
    path.write_text(json.dumps(self.__dict__))

  def parse_args(self, argv):
    # arg format: (key:value )* (::)? argv
    configdict = ordict()

    while argv and ":" in argv[0] and argv[0] != "::":
      arg, argv = argv[0], argv[1:]
      key, value = arg.split(":")
      configdict[key] = parse_value(value)

    if argv and argv[0] == "::":
      argv = argv[1:]

    for key, value in configdict.items():
      if key not in Config.defaults:
        raise KeyError("unknown config key", key)
      self.__dict__[key] = value

    return argv

  def to_argv(self, subcommand):
    assert not self.bypass # not sure it works currently but don't want to get rid of it
    return (["detour", subcommand] +
            ["%s:%s" % (key, getattr(self, key))
             for key in Config.defaults if hasattr(self, key)])


def parse_value(s):
  s = s.strip()
  try:
    return int(s)
  except ValueError:
    try:
      return float(s)
    except ValueError:
      return s


def get_remote(key):
  remotes = dict()

  def register_remote(klass):
    remotes[klass.__name__] = klass
    return klass

  # NOTE: can't register new subcommands at this point; would have to move these classes into global
  # scope
  @register_remote
  class local(InteractiveRemote):
    ssh_wrapper = "pshaw local".split()
    host = "localhost"
    runsdir = Path("/home/tim/detours")

  @register_remote
  class mila(InteractiveRemote):
    ssh_wrapper = "pshaw mila".split()
    host = "elisa3"
    runsdir = Path("/data/milatmp1/cooijmat/detours")

  @register_remote
  class cedar(BatchRemote):
    ssh_wrapper = "pshaw cedar".split()
    host = "cedar"
    runsdir = Path("/home/cooijmat/projects/rpp-bengioy/cooijmat/detours")

  return remotes[key]()


subcommands = set()

def register_subcommand(fn):
  subcommands.add(fn.__name__)
  return fn

# convenience decorators for remote interaction
def _remotely(fn, synchronized=True):
  fn = register_subcommand(fn)
  subcommand = fn.__name__
  def wfn(remote, config, *argv):
    if config.on_remote:
      return fn(remote, config, *argv)
    else:
      if synchronized:
        with remote.synchronization(config):
          return remote.enter_ssh(config, subcommand, *argv)
      else:
        return remote.enter_ssh(config, subcommand, *argv)
  return wfn

def remotely(fn): return _remotely(fn, synchronized=True)
def remotely_nosync(fn): return _remotely(fn, synchronized=False)

def locally(fn): return register_subcommand(fn)

# convenience decorator for subcommands that optionally take a label as first argument.
labeltaking_subcommands = set()
def labeltaking(fn):
  subcommand = fn.__name__
  subcommands.add(subcommand)
  labeltaking_subcommands.add(subcommand)
  return fn

class Remote(object):
  def rundir(self, label): return Path(self.runsdir, label)
  def ssh_rundir(self, label): return "%s:%s" % (self.host, self.rundir(label))
  ssh_runsdir = property(lambda self: "%s:%s" % (self.host, self.runsdir))

  @locally
  @labeltaking
  def pull(self, config):
    sp.check_call(self.ssh_wrapper + ["rsync", "-rltvz", "--ignore-missing-args"] + rsync_filter +
                  ["%s/" % self.ssh_rundir(config.label),
                   "%s/%s" % (local_runsdir, config.label)])

  @locally
  @labeltaking
  def push(self, config):
    sp.check_call(self.ssh_wrapper + ["rsync", "-rltvz"] + rsync_filter +
                  ["%s/%s/" % (local_runsdir, config.label),
                   self.ssh_rundir(config.label)])

  @remotely
  @labeltaking
  def visit(self, config):
    sp.check_call("bash", cwd=str(self.rundir(config.label)))

  @locally
  @labeltaking
  def stdout(self, config):
    path = get_stdout_file(Path(local_runsdir, config.label))
    if not path:
      raise ValueError("no stdout found for %s", config.label)
    sp.check_call(["less", "+G", path])

  @locally
  @labeltaking
  def invocation(self, config):
    invocation = json.loads(Path(local_runsdir, config.label, "invocation.json").read_text())
    print(repr(invocation))

  @remotely_nosync
  @labeltaking
  def status(self, config):
    def mark_lost():
      terminated_path = Path(self.runsdir, config.label, "terminated")
      if not terminated_path.exists():
        terminated_path.write_text("lost")
    try:
      job_id = Path(self.runsdir, config.label, "job_id").read_text()
    except FileNotFoundError:
      print(config.label, "no job_id; marking as lost")
      mark_lost()
    else:
      result = sp.call(["squeue", "-j", job_id])
      if result:
        print("invalid job %s (marking as lost)", job_id)
        mark_lost()
        return result

  @locally
  @labeltaking
  def purge(self, config):
    self.purgemany([config])
    return
    sp.check_call(self.ssh_wrapper + ["ssh", self.host] +
                  ["rm", "-rf", str(Path(self.runsdir, config.label))])
    sp.check_call(["rm", "-r", str(Path(local_runsdir, config.label))])

  @locally
  def purgemany(self, configs):
    print([config.label for config in configs])
    sp.check_call(self.ssh_wrapper + ["ssh", self.host] +
                  ["rm", "-rf"] +
                  [str(Path(self.runsdir, config.label))
                   for config in configs])
    sp.check_call(["rm", "-r"] +
                  [str(Path(local_runsdir, config.label))
                   for config in configs])

  @locally
  def package(self, config, *invocation):
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # gather files and determine checksum
    path = Path(tempfile.mkdtemp())
    sp.check_call(self.ssh_wrapper + ["rsync", "-rlzF"] + rsync_filter +
                  ["./", str(path)])
    checksum_output = sp.check_output(
      # (would prefer to use find -print0 and tar --null, but the latter doesn't seem to work)
      "tar -cf - %s | sha256sum" % path,
      shell=True)
    # (shortened to 128 bits or 32 hex characters to fit in screen's session name limit)
    checksum = checksum_output.decode().splitlines()[0].split()[0][:32]

    # create rundir and move files into place
    label = "%s_%s" % (timestamp, checksum)
    make_that_dir(Path(local_runsdir, label))
    shutil.move(path, Path(local_runsdir, label, "tree"))

    Path(local_runsdir, label, "invocation.json").write_text(json.dumps(invocation))
    config.override(label=label)
    config.to_file(Path(local_runsdir, label, "config.json"))
    logger.warn("invocation: %r", invocation)
    logger.warn("label: %r", label)

  @remotely
  @labeltaking
  def run(self, config):
    status = None
    try:
      conda_activate("py36")
      # record job number so we can determine status later, as well as whether it got started at all
      Path(self.runsdir, config.label, "job_id").write_text(os.environ["SLURM_JOB_ID"])
      invocation = json.loads(Path(self.runsdir, config.label, "invocation.json").read_text())
      os.environ["DETOUR_LABEL"] = config.label # for the user program
      # TODO: capture stdout/stderr without breaking interactivity
      status = sp.check_call(invocation, cwd=str(Path(self.runsdir, config.label, "tree")))
    finally:
      # touch a file to indicate the process has terminated
      Path(self.runsdir, config.label, "terminated").touch()
      # record status (separate from touch in case this crashes)
      if status is None:
        status = tb.format_exc()
      Path(self.runsdir, config.label, "terminated").write_text(str(status))

  @locally
  @labeltaking
  def synchronizing(self, config):
    while not Path(local_runsdir, config.label, "terminated").exists():
      interruptible_sleep(30)
      # TODO: what to do on failure?
      sp.call(config.to_argv("pull"),
              stdin=sp.DEVNULL, stdout=sp.DEVNULL, stderr=sp.DEVNULL)

  def enter_ssh(self, config, subcommand, *argv):
    detour_program = os.path.realpath(sys.argv[0])
    sp.check_call(self.ssh_wrapper + ["scp", detour_program, "%s:bin/detour" % self.host])
    sp.check_call(self.ssh_wrapper + ["ssh", "-t", self.host] +
                  config.with_overrides(on_remote=1).to_argv(subcommand) + list(argv))

  @contextlib.contextmanager
  def synchronization(self, config):
    synchronizer = sp.Popen(config.to_argv("synchronizing"),
                            stdin=sp.DEVNULL, stdout=sp.DEVNULL, stderr=sp.DEVNULL)
    yield
    synchronizer.terminate()
    synchronizer.wait()
    if not Path(local_runsdir, config.label, "terminated").exists():
      sp.check_call(config.to_argv("pull"))

class BatchRemote(Remote):
  @locally
  def launch(self, config, *argv):
    try:
      self.package(config, *argv)
    except KeyboardInterrupt:
      self.purge(config)
    self.push(config)
    self.enter_job(config)

  @remotely_nosync
  @labeltaking
  def enter_job(self, config):
    rundir = self.rundir(config.label)
    make_that_dir(rundir)

    # make a script describing the job
    command = " ".join(config.to_argv("run"))
    Path(rundir, "sbatch.sh").write_text(textwrap.dedent("""
         #!/bin/bash
         #SBATCH --time=23:59:59
         #SBATCH --account=rpp-bengioy
         #SBATCH --mem=16G
         #SBATCH --gres=gpu:1
         # some jobs need big gpus :-/
         ##SBATCH --gres=gpu:lgpu:4
         module load cuda/9.0.176 cudnn/7.0
         export LD_LIBRARY_PATH=$EBROOTCUDA/lib64:$EBROOTCUDNN/lib64:$LD_LIBRARY_PATH
         echo $HOSTNAME
         nvidia-smi
         %s
         """ % command).strip())

    output = sp.check_output(["sbatch", "sbatch.sh"], cwd=str(rundir))
    match = re.match(rb"\s*Submitted\s*batch\s*job\s*(?P<id>[0-9]+)\s*", output)
    if not match:
      print(output)
    Path(rundir, "job_id").write_bytes(match.group("id"))

  # just making sure this isn't getting called
  def enter_screen(self, config): assert False

class InteractiveRemote(Remote):
  @locally
  def launch(self, config, *argv):
    self.package(config, *argv)
    self.push(config)
    self.enter_screen(config)

  @remotely
  @labeltaking
  def enter_screen(self, config):
    rundir = self.rundir(config.label)
    make_that_dir(rundir)

    screenlabel = get_screenlabel(config.label)
    sp.check_call(["pkscreen", "-S", screenlabel], cwd=str(rundir))
    # wrap in `script` to capture output
    sp.check_call(["screen", "-S", screenlabel, "-p", "0", "-X", "stuff",
                   "script -e session.script && exit^M"], cwd=str(rundir))
    # `stuff` sends the given string to stdin, i.e. we run the command as if the user had typed
    # it.  the benefit is that we can attach and we are in a bash prompt with exactly the same
    # environment as the program ran in (as opposed to would be the case with some other ways of
    # keeping the screen window alive after the program terminates)
    # NOTE: too bad the command is "detour ..." which isn't really helpful
    # NOTE: && exit ensures screen terminates iff the command terminated successfully.
    sp.check_call(["screen", "-S", screenlabel, "-p", "0", "-X", "stuff",
                   "%s && exit^M" % " ".join(config.to_argv("enter_job"))],
                  cwd=str(rundir))
    sp.check_call(["screen", "-x", screenlabel],
                  env=dict(TERM="xterm-color"))

  @remotely
  @labeltaking
  def enter_job(self, config):
    excluded_hosts = [
      "mila00", # requires nvidia acknowledgement
      "mila01", # requires nvidia acknowledgement
    ]
    sp.check_call(["sinter", "--gres=gpu", "-Cgpu12gb", "--qos=unkillable", "--mem=16G",
                   "--exclude=%s" % ",".join(excluded_hosts),
                   # bash complains about some ioctl mystery, but it's fine
                   "bash", "-lic", " ".join(config.to_argv("run"))])

  @remotely
  @labeltaking
  def attach(self, config):
    sp.check_call(["screen", "-x", get_screenlabel(config.label)])

def get_screenlabel(label):
  return "detour_%s" % label

def interruptible_sleep(seconds):
  for second in range(seconds):
    time.sleep(1)

def udict(*mappings, **more_mappings):
  return dict((key, value)
              for mapping in list(mappings) + [more_mappings]
              for key, value in dict(mapping).items())

# -_______________-
def conda_activate(name):
  os.environ["CONDA_DEFAULT_ENV"] = name
  os.environ["CONDA_PATH_BACKUP"] = os.environ["PATH"]
  os.environ["CONDA_PREFIX"] = os.path.join(os.environ["HOME"], ".conda", "envs", name)
  os.environ["PATH"] = "%s:%s" % (os.path.join(os.environ["CONDA_PREFIX"], "bin"),
                                  os.environ["PATH"])

if __name__ == "__main__":
  main(sys.argv)
