#!/usr/bin/env python3
import argparse, datetime, json, logging, os, shutil, signal, sys, tempfile, time, re, pdb, textwrap, glob
import itertools as it, subprocess as sp, traceback as tb, functools as ft
import contextlib
from pathlib import Path
from collections import OrderedDict as ordict, defaultdict as ddict
import base64
import collections

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

class Database(object):
  def __init__(self, runsdir):
    self.runsdir = runsdir

  def get_rundir(self, label):
    return Path(self.runsdir, label)

  def ensure_rundir(self, label):
    rundir = self.get_rundir(label)
    make_that_dir(rundir)
    return rundir

  def set_invocation(self, label, invocation):
    self.get_invocation_path(label).write_text(json.dumps(invocation))

  def get_invocation(self, label):
    return json.loads(self.get_invocation_path(label).read_text())

  def get_invocation_path(self, label):
    return Path(self.runsdir, label, "invocation.json")

  def set_config(self, label, config):
    self.get_config_path(label).write_text(json.dumps(config))

  def get_config(self, label):
    return json.loads(self.get_config_path(label).read_text())

  def get_config_path(self, label):
    return Path(self.runsdir, label, "config.json")

  def get_remote_key(self, label):
    return self.get_config(label)["remote"]

  def get_remote(self, label):
    return get_remote(self.get_remote_key(label))

  def known_terminated(self, label):
    return self.get_terminated_path(label).exists()

  def get_terminated_path(self, label):
    return Path(self.runsdir, label, "terminated")

  def mark_terminated(self, label, context):
    terminated_path = self.get_terminated_path(label)
    assert not terminated_path.exists()
    terminated_path.touch()
    terminated_path.write_text(context)

  def mark_lost(self, label):
    self.mark_terminated(label, "lost")

  def package(self, *invocation, **config):
    try:
      timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

      # gather files and determine checksum
      path = Path(tempfile.mkdtemp())
      sp.check_call(["rsync", "-rlzF"] + rsync_filter + ["./", str(path)])
      checksum_output = sp.check_output(
        # (would prefer to use find -print0 and tar --null, but the latter doesn't seem to work)
        "tar -cf - %s | sha256sum" % path,
        shell=True)
      # (shortened to 128 bits or 32 hex characters to fit in screen's session name limit)
      checksum = checksum_output.decode().splitlines()[0].split()[0][:32]

      label = "%s_%s" % (timestamp, checksum)
      config["label"] = label

      # create rundir and move files into place
      shutil.move(path, Path(self.ensure_rundir(label), "tree"))

      self.set_invocation(label, invocation)
      self.set_config(label, config)

      logger.warning("invocation: %r", invocation)
      logger.warning("label: %r", label)
      logger.warning("config: %r", config)

      return label
    except KeyboardInterrupt:
      assert False # FIXME remove packagage wherever it is
      raise

  def study_labels(self, study):
    for path in sorted(glob.glob(self.runsdir + "/*/config.json")):
      config = json.loads(Path(path).read_text())
      if config.get("study", None) == study:
        yield config["label"]

  def get_screenlabel(self, label):
    return "detour_%s" % label

  def get_stdout_file(self, label):
    for pattern in "session.script slurm-*.out".split():
      expression = str(Path(self.runsdir, label, pattern))
      paths = glob.glob(expression)
      if paths:
        path, = paths
        return path
    return None

  def get_status(self, label):
    try:
      status = self.get_terminated_path(label).read_text()
    except FileNotFoundError:
      status = "unterminated"
    if "CalledProcessError" in status:
      # uninformative exception in "detour run" command; extract the true exception from slurm-*.out
      exceptions = self.extract_exceptions(label)
      exceptions = [abridge(ex, 80) for ex in exceptions]
      status = "; ".join(exceptions)
    return status

  def extract_exceptions(self, label):
    stdout_path = self.get_stdout_file(label)
    if not stdout_path:
      logger.warning("no stdout found for %s", label)
      return []

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

  def get_job_id(self, label):
    try:
      return Path(self.runsdir, label, "job_id").read_text()
    except FileNotFoundError:
      return None

  def set_job_id(self, label, job_id):
    Path(self.runsdir, label, "job_id").write_text(job_id)

def serialize(x): return base64.urlsafe_b64encode(json.dumps(x).encode("utf-8")).decode("utf-8")
def deserialize(x): return json.loads(base64.urlsafe_b64decode(x).decode("utf-8"))

def rpc_encode_argv(*argv, identifier=None):
  rpc_argv = [serialize(argv)]
  if identifier is not None:
    rpc_argv.append(identifier)
  return rpc_argv

def rpc_decode_argv(arg, identifier=None):
  return deserialize(arg), dict(identifier=identifier)

def detour_rpc_argv(method, *argv, **kwargs):
  identifier = kwargs.pop("identifier", None)
  rpc_argv = rpc_encode_argv(*argv, identifier=identifier)
  return detour_argv("rpc", method, *rpc_argv, **kwargs)

def detour_argv(*argv, **config):
  the_config = dict()
  the_config.update(global_config)
  the_config.update(config)
  flags = []
  for key, value in the_config.items():
    if isinstance(value, bool): # sigh
      if value:
        flags.append("--%s" % key)
    else:
      flags.append("--%s=%s" % (key, value))
  return ["detour"] + flags + ["--"] + list(argv)

def detour_parse_argv(*argv):
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument("--on_remote", choices="local mila cedar".split())
  parser.add_argument("--verbose", default=False, action="store_true")
  parser.add_argument("subcommand")
  parser.add_argument("argv", nargs=argparse.REMAINDER)
  args = parser.parse_args(argv)
  config = dict(on_remote=args.on_remote,
                verbose=args.verbose)
  return config, args.subcommand, args.argv

class Subcommands(dict):
  def register(self, key, fn):
    self[key] = fn

  def invoke(self, key, *argv, **kwargs):
    return self[key](*argv, **kwargs)

global_config = dict()
localdb = Database(".detours")
subcommands = Subcommands()

def subcommand(fn):
  subcommands.register(fn.__name__, fn)

class Main(object):
  def __call__(self, argv):
    try:
      config, subcommand, argv = detour_parse_argv(*argv)
      global_config.update(config)
      subcommands.invoke(subcommand, *argv)
    except Exception: # TODO was "except:"; trying to let keyboardinterrupt pass through
      tb.print_exc()
      pdb.post_mortem()
      raise

  @subcommand
  def rpc(method, *rpc_argv):
    assert global_config["on_remote"]
    remote = global_config["on_remote"]
    args, kwargs = rpc_decode_argv(*rpc_argv)
    getattr(get_remote(remote), method)(*args, **kwargs)

  @subcommand
  def wtfrpc(method, *rpc_argv):
    # decode and print an rpc argv
    print(method, rpc_decode_argv(*rpc_argv))

  @subcommand
  def launchcmd(*argv):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote", choices="local mila cedar".split())
    parser.add_argument("--study")
    parser.add_argument("invocation", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    label = localdb.package(*args.invocation, study=args.study, remote=args.remote)
    remote = localdb.get_remote(label)
    remote.launch([label])

  @subcommand
  def launch(*labels):
    for remote, labels in groupby(labels, localdb.get_remote).items():
      remote.launch(labels)

  @subcommand
  def package(*argv):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote", choices="local mila cedar".split())
    parser.add_argument("--study")
    parser.add_argument("invocation", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    label = localdb.package(*args.invocation, study=args.study, remote=args.remote)
    print(label)

  @subcommand
  def pullstudy(study):
    labels = list(localdb.study_labels(study))
    for remote, labels in groupby(labels, localdb.get_remote).items():
      remote.pull(labels)

  @subcommand
  def push(*labels):
    for remote, labels in groupby(labels, localdb.get_remote).items():
      remote.push(labels)

  @subcommand
  def pull(*labels):
    for remote, labels in groupby(labels, localdb.get_remote).items():
      remote.pull(labels)

  @subcommand
  def purge(*labels):
    for remote, labels in groupby(labels, localdb.get_remote).items():
      remote.purge(labels)

  @subcommand
  def purgestudy(study):
    labels = list(localdb.study_labels(study))
    for remote, labels in groupby(labels, localdb.get_remote).items():
      remote.purge(labels)

  @subcommand
  def studystatus(study):
    labels = list(localdb.study_labels(study))
    statuses = get_statuses(labels)
    for status, labels in groupby(labels, lambda label: statuses[label]).items():
      print(len(labels), "runs:", status)
      if global_config.get("verbose", False):
        for label in labels:
          print("  ", label)

  @subcommand
  def stdout(label):
    path = localdb.get_stdout_file(label)
    if not path:
      raise ValueError("no stdout found for %s", label)
    sp.check_call(["less", "+G", path])

  @subcommand
  def visit(label): localdb.get_remote(label).visit(label)
  @subcommand
  def status(label): localdb.get_remote(label).status(label)
  @subcommand
  def attach(label): localdb.get_remote(label).attach(label)

def get_statuses(labels):
  all_labels = labels
  statuses = ddict()
  for remote, labels in groupby(labels, localdb.get_remote_key).items():
    print(remote, labels)
    remote = get_remote(remote)
    unterminated = [label for label in labels if not localdb.known_terminated(label)]
    remote.pull(unterminated)
    # for unterminated, figure out statuses remotely
    unterminated = [label for label in labels if not localdb.known_terminated(label)]
    if unterminated:
      statuses.update(remote.statusmany(unterminated))
    # for terminated, figure out statuses locally
    terminated = [label for label in labels if localdb.known_terminated(label)]
    statuses.update((label, localdb.get_status(label)) for label in terminated)
  assert set(statuses.keys()) == set(all_labels)
  return statuses


def get_remote(key):
  remotes = dict()

  def register_remote(klass):
    remotes[klass.__name__] = klass
    return klass

  # NOTE: can't register new subcommands at this point; would have to move these classes into global
  # scope
  @register_remote
  class local(InteractiveRemote):
    key = "local"
    ssh_wrapper = "pshaw local".split()
    host = "localhost"
    runsdir = Path("/home/tim/detours")

  @register_remote
  class mila(InteractiveRemote):
    key = "mila"
    ssh_wrapper = "pshaw mila".split()
    host = "elisa3"
    runsdir = Path("/data/milatmp1/cooijmat/detours")

  @register_remote
  class cedar(BatchRemote):
    key = "cedar"
    ssh_wrapper = "pshaw cedar".split()
    host = "cedar"
    runsdir = Path("/home/cooijmat/projects/rpp-bengioy/cooijmat/detours")

  return remotes[key]()


# helper to reduce the brain damage
def decorator_with_args(decorator):
  def uno(*args, **kwargs):
    def dos(function):
      return decorator(function, *args, **kwargs)
    return dos
  return uno

class RemoteCommand(object):
  def __init__(self, interactive=False):
    self.interactive = interactive

  # note the local part is used as a contextmanager; it wraps around the remote part.
  # it should contain `result = yield args` where args are the arguments to the
  # remote subcommand, and the result is the result returned by the remote part.
  # note the result goes through serialize/deserialize so is constrained to what json
  # can express. finally, the local part should yield its return value.
  def locally(self, fn, name=None):
    self.local_fn = fn
    self.subcommand = fn.__name__ if name is None else name
    return self

  def remotely(self, fn, name=None):
    self.remote_fn = fn
    self.subcommand = fn.__name__ if name is None else name
    return self

  def _call_while_local(self, remote, *args, **kwargs):
    local_fn_invocation = self.local_fn(remote, *args, **kwargs)
    argv = next(local_fn_invocation)
    result = self.roundtrip(remote, self.subcommand, *argv)
    return local_fn_invocation.send(result)

  def __call__(self, remote, *args, **kwargs):
    if global_config["on_remote"]:
      self._call_while_remote(remote, *args, **kwargs)
    else:
      self._call_while_local(remote, *args, **kwargs)

  # total mindfuck courtesy of https://stackoverflow.com/a/47433786/7601527.
  # callable objects can't decorate methods. python strikes again!
  def __get__(self, the_self, type=None):
    return ft.partial(self, the_self)

  @staticmethod
  def from_interactive(interactive, *args, **kwargs):
    if interactive: return InteractiveRemoteCommand(*args, **kwargs)
    else: return NoninteractiveRemoteCommand(*args, **kwargs)

class InteractiveRemoteCommand(RemoteCommand):
  def _call_while_remote(self, remote, *args, **kwargs):
    self.remote_fn(remote, *args, **kwargs)

  def roundtrip(self, remote, method, *argv):
    detour_bin_path = os.path.realpath(sys.argv[0])
    # TODO: can avoid this if we make a local copy each time we copy to remote. then we can check against local copy
    sp.check_call(remote.ssh_wrapper + ["scp", detour_bin_path, "%s:bin/detour" % remote.host])
    return sp.check_call(remote.ssh_wrapper + ["ssh", "-t", remote.host] +
                         detour_rpc_argv(method, *argv, on_remote=remote.key))

class NoninteractiveRemoteCommand(RemoteCommand):
  # non-interactive effectively means the command has a structured return
  # value. this case is painful because there are only two channels through
  # which the remote command can communicate back to us: the return code and
  # the output. we serialize the structured return value and dump it in the
  # output, labeled with an identifier.
  def _call_while_remote(self, remote, *args, **kwargs):
    identifier = kwargs.pop("identifier")
    result = self.remote_fn(remote, *args, **kwargs)
    print("rpc-response", identifier, serialize(result))

  def roundtrip(self, remote, method, *argv):
    detour_bin_path = os.path.realpath(sys.argv[0])
    # TODO: can avoid this if we make a local copy each time we copy to remote. then we can check against local copy
    sp.check_call(remote.ssh_wrapper + ["scp", detour_bin_path, "%s:bin/detour" % remote.host])
    # try to maintain interactivity so we can drop into pdb in the remote code.
    # if all goes well (i.e. no exceptions) there will be no interaction and the
    # (serialized) return value of the remote code will be in the output,
    # labeled with the rpc identifier. a better way to do this would be
    # to capture only stdout and not stderr, but that is just not possible:
    # https://stackoverflow.com/questions/34186035/can-you-fool-isatty-and-log-stdout-and-stderr-separately
    import random
    identifier = serialize(random.random())
    command = (remote.ssh_wrapper + ["ssh", "-t", remote.host] +
               detour_rpc_argv(method, *argv, on_remote=remote.key, identifier=identifier))
    output = check_output_interactive(command)
    for line in output.splitlines():
      if line.startswith("rpc-response"):
        parts = line.split()
        if parts[0] == "rpc-response" and parts[1] == identifier:
          result = parts[2]
          break
    else:
      raise RuntimeError("no rpc-response in output")
    return deserialize(result)

# decorator to define a two-stage command
@decorator_with_args
def locally(fn, interactive=False):
  rc = RemoteCommand.from_interactive(interactive)
  rc.locally(fn)
  return rc

@decorator_with_args
def remotely(fn, interactive=False):
  rc = RemoteCommand.from_interactive(interactive)
  rc.remotely(fn)
  # by default, the local part is a no-op
  def local_fn(remote, *args, **kwargs):
    result = yield args
    yield result
  rc.locally(local_fn, name=fn.__name__)
  return rc

@decorator_with_args
def remotely_only(fn, interactive=False):
  rc = RemoteCommand.from_interactive(interactive)
  rc.remotely(fn)
  def local_fn(remote, *args, **kwargs):
    raise RuntimeError("subcommand %s can only be run remotely" % fn.__name__)
  rc.locally(local_fn, name=fn.__name__)
  return rc

class Remote(object):
  def __init__(self):
    self.database = Database(self.runsdir)

  def pull(self, labels):
    if not labels: return
    self.rsync(["%s:%s" % (self.host, self.database.get_rundir(label))
                for label in labels],
               localdb.runsdir)

  def rsync(self, sources, destination):
    for i in range(3):
      try:
        sp.check_call(self.ssh_wrapper + ["rsync", "-urltvz", "--ignore-missing-args"] +
                      rsync_filter + sources + [destination])
      except sp.CalledProcessError as e:
        if e.returncode == 23:
          # some files/attrs were not transferred -- maybe they were being written to.
          # try again a few times
          delay = 2 ** i
          logging.warning("rsync failed to transfer everything, retrying in %i seconds", delay)
          interruptible_sleep(delay)
        else:
          raise
      else:
        # success, don't retry
        break

  def push(self, labels):
    if not labels: return
    self.rsync([localdb.get_rundir(label) for label in labels],
               "%s:%s" % (self.host, self.database.runsdir))

  def purge(self, labels):
    if not labels: return
    sp.check_call(self.ssh_wrapper + ["ssh", self.host] +
                  ["rm", "-rf"] +
                  [str(self.database.get_rundir(label)) for label in labels])
    sp.check_call(["rm", "-r"] +
                  [str(localdb.get_rundir(label)) for label in labels])

  @remotely(interactive=True)
  def visit(self, label):
    sp.check_call("bash", cwd=str(self.database.get_rundir(label)))

  @remotely(interactive=False)
  def statusmany(self, *labels):
    statuses = dict()
    job_ids = ordict()

    for label in labels:
      if not self.database.get_rundir(label).exists():
        statuses[label] = "no remote copy"
      else:
        job_id = self.database.get_job_id(label)
        if job_id is None:
          self.database.mark_lost(label)
          statuses[label] = "lost"
        else:
          job_ids[job_id] = label

    try:
      blob = sp.check_output(["squeue", "-O", "jobid,state,reason,timeused,timeleft", "-j", ",".join(job_ids.keys())], stderr=sp.STDOUT)
      error = 0
      # example output:
      # JOBID               STATE               REASON              TIME                TIME_LEFT
      # 11922138            PENDING             Priority            0:00                1-00:00:00
      # 11922147            PENDING             Priority            0:00                1-00:00:00
      # 11922157            PENDING             Priority            0:00                1-00:00:00
      for line in blob.splitlines()[1:]:
        line = line.decode("utf-8")
        job_id, state, reason, timeused, timeleft = line.split()
        statuses[job_ids[job_id]] = ordict(state=state, reason=reason, timeused=timeused, timeleft=timeleft)
    except sp.CalledProcessError as e:
      blob = e.output
      error = e.returncode
      import pdb; pdb.set_trace()

    for job_id, label in job_ids.items():
      if label not in statuses:
        # this could happen if the job has terminated, in which case the local part should
        # pull it out of the `terminated` file
        logger.error("could not determine status of job %s with label %s", job_id, label)
        import pdb; pdb.set_trace()

    return statuses

  @statusmany.locally
  def statusmany(self, labels):
    # FIXME would like to cancel the whole thing if no labels;
    # find a way to communicate this to the caller
    #if not labels: return
    statuses = yield labels
    for label, status in statuses.items():
      if status == "no remote copy":
        localdb.mark_terminated(label, "no remote copy")
    yield statuses

  @remotely_only(interactive=True)
  def run(self, label):
    status = None
    try:
      conda_activate("py36")
      # record job number so we can determine status later, as well as whether it got started at all
      self.database.set_job_id(label, os.environ["SLURM_JOB_ID"])
      invocation = self.database.get_invocation(label)
      os.environ["DETOUR_LABEL"] = label # for the user program
      status = sp.check_call(invocation, cwd=str(Path(self.database.get_rundir(label), "tree")))
    finally:
      if status is None:
        status = tb.format_exc()
      self.database.mark_terminated(label, status)

  def synchronizing(self, label):
    while not localdb.known_terminated(label):
      interruptible_sleep(30)
      # TODO: what to do on failure?
      self.pull([label])

  @contextlib.contextmanager
  def synchronization(self, label):
    synchronizer = sp.Popen(detour_rpc_argv("synchronizing"),
                            stdin=sp.DEVNULL, stdout=sp.DEVNULL, stderr=sp.DEVNULL)
    yield
    synchronizer.terminate()
    synchronizer.wait()
    if not localdb.known_terminated(label):
      self.pull([label])

class BatchRemote(Remote):
  def launch(self, labels):
    self.push(labels)
    self.submit_jobs(labels)

  @remotely(interactive=False)
  def submit_jobs(self, labels):
    for label in labels:
      rundir = self.database.ensure_rundir(label)

      # make a script describing the job
      command = " ".join(detour_rpc_argv("run", label))
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
        import pdb; pdb.set_trace()
      job_id = match.group("id").decode("ascii")
      self.database.set_job_id(label, job_id)
      return job_id

class InteractiveRemote(Remote):
  def launch(self, labels):
    self.push(labels)
    # interactive means run one by one
    for label in labels:
      self.enter_screen(label)

  @remotely(interactive=True)
  def enter_screen(self, label):
    rundir = self.database.ensure_rundir(label)

    screenlabel = self.database.get_screenlabel(label)
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
                   "%s && exit^M" % " ".join(detour_rpc_argv("submit_job", label))],
                  cwd=str(rundir))
    sp.check_call(["screen", "-x", screenlabel],
                  env=dict(TERM="xterm-color"))

  @enter_screen.locally
  def enter_screen(self, label):
    with self.synchronization(label):
      yield [label]

  @remotely_only(interactive=True)
  def submit_job(self, label):
    excluded_hosts = [
      "mila00", # requires nvidia acknowledgement
      "mila01", # requires nvidia acknowledgement
      "bart10", # intermittent pauses? september 2018
    ]
    sp.check_call(["sinter", "--gres=gpu", "-Cgpu12gb", "--qos=unkillable", "--mem=16G",
                   "--exclude=%s" % ",".join(excluded_hosts),
                   # bash complains about some ioctl mystery, but it's fine
                   "bash", "-lic", " ".join(detour_rpc_argv("run", label))])

  @remotely(interactive=True)
  def attach(self, label):
    sp.check_call(["screen", "-x", self.database.get_screenlabel(label)])

  @attach.locally
  def attach(self, label):
    with self.synchronization(label):
      yield [label]

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

def make_that_dir(path):
  path = str(path) # convert pathlib Path (which has mkdir but exist_ok is py>=3.5)
  os.makedirs(path, exist_ok=True)

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

def groupby(iterable, key):
  groups = ddict(list)
  for item in iterable:
    groups[key(item)].append(item)
  return groups

def parse_bool_arg(s):
  if s.lower() in "yes true t y 1".split(): return True
  elif s.lower() in "no false f n 0".split(): return False
  else: raise argparse.ArgumentTypeError('Boolean value expected.')

def check_output_interactive(command):
  import pty, os
  master, slave = pty.openpty()
  output = []
  def read(fd):
    data = os.read(fd, 1024)
    output.append(data.decode("utf-8"))
    return data
  pty.spawn(command, read)
  return "".join(output)

if __name__ == "__main__":
  logger.debug(sys.argv[1:])
  Main()(sys.argv[1:])
