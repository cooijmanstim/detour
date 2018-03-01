#!/usr/bin/env python3
import argparse, datetime, json, logging, os, shutil, signal, sys, tempfile, time, re, pdb
import subprocess as sp, traceback as tb
import contextlib
from pathlib import Path
from collections import OrderedDict as ordict

logger = logging.Logger("detour")
logger.setLevel(logging.INFO)

# job dependencies are considered to be everything in the current directory, except
# hidden files and __pycache__ and notebooks
# TODO and large files
find_filter = """
  -not -path */.* -and
  -not -path */__pycache__/* -and
  -not -name *.npz*-numpy.npy -and
  -not -name *.ipynb
""".strip().split()
rsync_filter = """
  --exclude .*
  --exclude __pycache__
  --exclude *.npz*-numpy.npy
  --exclude *.ipynb
""".strip().split()

local_runsdir = ".detours"
local_subcommands = "launch attach push pull".split()

def pdb_post_mortem(fn):
  def wfn(*args, **kwargs):
    try:
      return fn(*args, **kwargs)
    except:
      tb.print_exc()
      pdb.post_mortem()
      raise
  return wfn

@pdb_post_mortem
def main(argv):
  subcommand = argv[1]
  # NOTE: in push/pull/attach the remote has to be figured out from metadata in the rundir, but
  # these have lower weight than values from the environment, which in turn have lower weight than
  # values from command-line arguments.
  config = Config()
  print("from env", config.__dict__)
  argv = config.parse_args(argv[2:])
  print("from argv", config.__dict__)
  if config.label and subcommand in local_subcommands:
    config = config.with_underrides(Config.from_file(
      Path(local_runsdir, config.label, "config")))
    print("from config", config.__dict__)

  # NOTE: in future there may be some subcommands that don't require a remote, e.g. list jobs and states
  assert config.remote
  remote = get_remote(config.remote)
  getattr(remote, subcommand)(config, *argv)


class Config(object):
  # in order to be able to invoke ourselves locally and remotely and in ways that must be robust to
  # several levels of string mangling, this object deals with what would otherwise just be flags.
  keys = "label remote bypass".split()
  defaults = dict(label=None, remote=None, bypass=0)

  def __init__(self, items=(), **kwargs):
    self.__dict__.update(Config.defaults)
    self.override(items, **kwargs)
    assert not self.bypass # not sure it works currently but don't want to get rid of it

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
    return Config(json.loads(path.read_bytes()))

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
      if key not in Config.keys:
        raise KeyError("unknown config key", key)
      self.__dict__[key] = value

    return argv

  def to_argv(self, subcommand):
    return (["detour", subcommand] +
            ["%s:%s" % (key, getattr(self, key))
             for key in Config.keys if hasattr(self, key)])


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

  @register_remote
  class local(Remote):
    ssh_wrapper = "pshaw local".split()
    host = "localhost"
    runsdir = Path("/home/tim/detours")

  @register_remote
  class mila(Remote):
    ssh_wrapper = "pshaw mila".split()
    host = "elisa3"
    runsdir = Path("/data/milatmp1/cooijmat/detours")

  @register_remote
  class cedar(Remote):
    ssh_wrapper = "pshaw cedar".split()
    host = "cedar"
    runsdir = Path("/home/cooijmat/projects/rpp-bengioy")
    # FIXME: for cedar must chgroup rpp-bengioy all files created. bashrc?

  return remotes[key]()


class Remote(object):
  def rundir(self, label): return Path(self.runsdir, label)
  def ssh_rundir(self, label): return "%s:%s" % (self.host, self.rundir(label))
  ssh_runsdir = property(lambda self: "%s:%s" % (self.host, self.runsdir))

  def pull(self, config):
    sp.check_call(self.ssh_wrapper + ["rsync", "-avz"] + rsync_filter +
                  ["%s/" % self.ssh_rundir(config.label),
                   "%s/%s" % (local_runsdir, config.label)])

  def push(self, config):
    sp.check_call(self.ssh_wrapper + ["rsync", "-avz"] + rsync_filter +
                  ["%s/%s/" % (local_runsdir, config.label),
                   self.ssh_rundir(config.label)])

  def attach(self, config):
    with self.synchronization(config):
      sp.check_call(self.ssh_wrapper + ["ssh", "-t", self.host,
                                        "screen", "-x", get_screenlabel(config.label)])

  def launch(self, config, *argv):
    # NOTE it's a long way from `launch` to `run`:
    # launch -> enter_ssh -> enter_screen -> enter_job -> enter_conda -> run
    label = self.prepare_launch(argv)
    config = config.with_overrides(label=label)
    self.push(config)
    with self.synchronization(config):
      self.enter_ssh(config)

  # FIXME specialize: LocalRemote has this as a no-op if config.bypass
  def enter_ssh(self, config):
    detour_program = os.path.realpath(sys.argv[0])
    sp.check_call(self.ssh_wrapper + ["scp", detour_program, "%s:bin/detour" % self.host])
    sp.check_call(self.ssh_wrapper + ["ssh", "-t", self.host] +
                  config.to_argv("enter_screen"))

  def enter_screen(self, config):
    rundir = self.rundir(config.label)
    rundir.mkdir(parents=True, exist_ok=True)

    screenlabel = get_screenlabel(config.label)
    sp.check_call(["pkscreen", "-S", screenlabel], cwd=str(rundir))
    # `stuff` sends the given string to stdin, i.e. we run the command as if the user had typed
    # it.  the benefit is that we can attach and we are in a bash prompt with exactly the same
    # environment as the program ran in (as opposed to would be the case with some other ways of
    # keeping the screen window alive after the program terminates)
    # NOTE: too bad the command is "detour ..." which isn't really helpful
    # NOTE: && exit ensures screen terminates iff the command terminated successfully.
    sp.check_call(["screen", "-S", screenlabel, "-p", "0", "-X", "stuff",
                   "%s && exit^M" % " ".join(config.to_argv("enter_job"))],
                  cwd=str(rundir))
    # FIXME specialize: don't attach on cedar
    sp.check_call(["screen", "-x", screenlabel],
                  env=dict(TERM="xterm-color"))

  def enter_job(self, config):
    excluded_hosts = [
      "mila00", # requires nvidia acknowledgement
      "mila01", # requires nvidia acknowledgement
      "bart13", # borken for good?
      "bart14",
    ]
    sp.check_call(["sinter", "--gres=gpu", "-Cgpu12gb", "--qos=unkillable", "--mem=16G",
                   "--exclude=%s" % ",".join(excluded_hosts),
                   # bash complains about some ioctl mystery, but it's fine
                   "bash", "-lic", " ".join(config.to_argv("enter_conda"))])

  def enter_conda(self, config):
    env = os.environ
    env = activate_environment("py36test", env)
    env["DETOUR_LABEL"] = config.label
    sp.check_call(config.to_argv("run"), env=env)

  def run(self, config):
    status = None
    try:
      invocation = json.loads(Path(self.runsdir, config.label, "invocation.json").read_text())
      # TODO: capture stdout/stderr without breaking interactivity
      status = sp.check_call(invocation, cwd=str(Path(self.runsdir, config.label, "tree")))
    finally:
      # touch a file to indicate the process has terminated
      Path(self.runsdir, config.label, "terminated").touch()
      # record status (separate from touch in case this crashes)
      if status is None:
        status = tb.format_exc()
      Path(self.runsdir, config.label, "terminated").write_text(str(status))

  # FIXME specialize: sync differently on cedar because we don't attach to screen and wait for completion
  @contextlib.contextmanager
  def synchronization(self, config):
    yield
    return
    childpid = os.fork()
    if childpid == 0:
      while True:
        interruptible_sleep(30)
        self.pull(config)
        #sp.check_call(config.to_argv("pull"),
        #              stdin=sp.DEVNULL, stdout=sp.DEVNULL, stderr=sp.DEVNULL)
        if Path(local_runsdir, config.label, "terminated").exists():
          break
    else:
      try:
        yield
      finally:
        os.kill(childpid, signal.SIGTERM)
        os.waitpid(childpid, 0)

    if not Path(local_runsdir, config.label, "terminated").exists():
      #sp.check_call(config.to_argv("pull"),
      #              stdin=sp.DEVNULL, stdout=sp.DEVNULL, stderr=sp.DEVNULL)
      pass

  def prepare_launch(self, invocation):
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    checksum_output = sp.check_output(
      "|".join([
        # would prefer to use find -print0 and tar --null, but the latter doesn't seem to work
        "find . -type f '%s'" % "' '".join(find_filter),
        "tar -cf - --no-recursion --verbatim-files-from --files-from=-",
        "sha256sum",
       ]),
      shell=True)
    # (shortened to 128 bits or 32 hex characters to fit in screen's session name limit)
    checksum = checksum_output.decode().splitlines()[0].split()[0][:32]
    label = "%s_%s" % (timestamp, checksum)
    logger.warn("invocation: %r", invocation)
    logger.warn("label: %r", label)
    Path(local_runsdir, label).mkdir(parents=True)
    sp.check_call(["rsync", "-a", "."] + rsync_filter +
                  [Path(local_runsdir, label, "tree")])
    Path(local_runsdir, label, "invocation.json").write_text(json.dumps(invocation))
    return label

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
def activate_environment(name, env):
  env["CONDA_DEFAULT_ENV"] = name
  env["CONDA_PATH_BACKUP"] = env["PATH"]
  env["CONDA_PREFIX"] = os.path.join(env["HOME"], ".conda", "envs", name)
  env["PATH"] = "%s:%s" % (os.path.join(env["CONDA_PREFIX"], "bin"), env["PATH"])
  return env

if __name__ == "__main__":
  main(sys.argv)
