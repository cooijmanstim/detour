#!/usr/bin/env python3
import argparse, datetime, json, logging, os, shutil, signal, sys, tempfile, time, re, pdb, textwrap, glob, inspect, zlib
import itertools as it, subprocess as sp, traceback as tb, functools as ft
import contextlib
from pathlib import Path
from collections import OrderedDict as ordict, defaultdict as ddict, deque, namedtuple
import base64
import collections

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
  --exclude *.pdf
  --exclude altair*.json
""".strip().split() + ["--filter=: /.d2filter"]

class attrdict(dict):
  __getattr__ = dict.get
  __setattr__ = dict.__setitem__

# globals
G = attrdict(config=attrdict(), localdb=None)

def dict_from_class(cls):
  crud = "__module__ __dict__ __weakref__ __doc__".split()
  members = {k: v for k, v in vars(cls).items() if k not in crud}
  return members

class FunctionTree:
  def __init__(self, fn):
    self.fn = fn
    self.children = {}
  def add_children(self, cls):
    for k, v in dict_from_class(cls).items():
      if callable(v):
        # wrap bare functions
        v = FunctionTree(v)
      self.children[k] = v

@FunctionTree
def main(*, on_remote=None):
  try:
    G.config.update(on_remote=on_remote)
    G.localdb = Database(".detours")
    yield
  except Exception: # TODO was "except:"; trying to let keyboardinterrupt pass through
    tb.print_exc()
    pdb.post_mortem()
    raise
@main.add_children
class _:

  # remote procedure calls
  def rpc(method, *rpc_argv):
    remote = get_remote(G.config.on_remote)
    args, kwargs = RPCCodec.decode_args(*rpc_argv) # NOTE kwargs may include rpc_identifier
    try:
      with Path(remote.database.runsdir, "detour_rpc_log").open("a") as log:
        log.write("%s %r %r\n" % (method, args, kwargs))
    except: pass
    getattr(remote, method)(*args, **kwargs)
  def wtfrpc(method, *rpc_argv):
    # decode and print an rpc argv
    args, kwargs = RPCCodec.decode_args(*rpc_argv)
    print(method, args, kwargs)

  # packaging and launching runs
  def launchcmd(*invocation, remote=None, interactive=False, study=None, preset=None):
    label = G.localdb.package(*invocation, study=study, remote=remote)
    remote = G.localdb.get_remote(label)
    if args.interactive: remote.launch_interactive([label], preset=args.preset)
    else:                remote.launch_batch      ([label], preset=args.preset)
  def launch(*labels, preset=None, interactive=False):
    for remote, labels in G.localdb.by_remote(labels):
      interruptible_sleep(2) # throttle
      if interactive: remote.launch_interactive(labels, preset=preset)
      else:           remote.launch_batch      (labels, preset=preset)
  def package(*invocation, remote=None, study=None):
    label = G.localdb.package(*invocation, study=study, remote=remote)
    print(label)

  def visit(label): do_single_remote(label, "visit")
  def attach(label): do_single_remote(label, "attach")
  def push(*labels): do_bulk_remote(labels, "push")
  def pull(*labels): do_bulk_remote(labels, "pull")
  def purge(*labels): do_bulk_remote(labels, "purge", ignore_nonexistent=True)

  def stdout(label):
    path = G.localdb.get_stdout_path(label)
    if not path:
      raise ValueError("no stdout found for %s", label)
    sp.check_call(["less", "+G", path])

  def status(*labels, verbose=False, refresh=False):
    labels = G.localdb.designated_labels(labels)
    statuses = get_statuses(labels, ignore_cache=refresh)
    def criterion(label):
      status = statuses[label]
      if isinstance(status, dict): return status["state"]
      else:                        return status
    for status, labels in groupby(labels, criterion).items():
      print(len(labels), "runs:", status)
      if verbose:
        for label in labels:
          print("  ", label)

  def resubmit(*labels):
    labels = G.localdb.designated_labels(labels)
    for remote, labels in G.localdb.by_remote(labels):
      remote.resubmit(labels)
  def resubmitunrun(*labels, refresh=False):
    labels = G.localdb.designated_labels(labels)
    statuses = get_statuses(labels, ignore_cache=refresh)
    unrun_labels = [label for label in labels
                    if statuses[label] in ["lost", "no remote copy"]]
    print("about to resubmit %i unrun jobs" % len(unrun_labels))
    for unrun_label in unrun_labels:
      print(unrun_label)
    import pdb; pdb.set_trace()
    for remote, labels in G.localdb.by_remote(unrun_labels):
      push_labels = [label for label in labels if statuses[label] == "no remote copy"]
      remote.push(push_labels)
      remote.resubmit(labels)

  def studies(runsdir=None, *, verbose=False):
    db = Database(runsdir) if runsdir is not None else G.localdb
    def get_total_size(configs):
      return db.get_total_size([config["label"] for config in configs])
    for study, configs in groupby(db.configs, lambda config: config["study"]).items():
      print("%50s %4i runs on %s"
            % (study, len(configs),
              ", ".join("%s (%i) (%s)" %
                        (remote, len(remote_configs), get_total_size(remote_configs))
                        for remote, remote_configs in
                        groupby(configs, lambda config: config["remote"]).items())))
      if verbose:
        for config in configs:
          print("  ", config["label"], config["remote"], get_total_size([config]))


def get_statuses(labels, ignore_cache=False):
  all_labels = labels
  statuses = ddict()
  for remote, labels in G.localdb.by_remote(labels):
    if ignore_cache:
      # pull all labels whether locally considered terminated or not
      remote.pull(labels)
    else:
      unterminated = [label for label in labels if not G.localdb.known_terminated(label)]
      if unterminated:
        remote.pull(unterminated)
    # for unterminated, figure out statuses remotely
    unterminated = [label for label in labels if not G.localdb.known_terminated(label)]
    if unterminated or ignore_cache:
      statuses.update(remote.statusmany(unterminated))
    # for terminated, figure out statuses locally
    terminated = [label for label in labels if G.localdb.known_terminated(label)]
    statuses.update((label, G.localdb.get_status(label)) for label in terminated)
  assert set(statuses.keys()) == set(all_labels)
  return statuses

def do_bulk_remote(labels, method, ignore_nonexistent=False):
  labels = G.localdb.designated_labels(labels, ignore_nonexistent=ignore_nonexistent)
  for remote, labels in G.localdb.by_remote(labels):
    getattr(remote, method)(labels)
def do_single_remote(label, method):
  remote = G.localdb.get_remote(label)
  getattr(remote, method)(label)


class RemoteCommand(object):
  def __init__(self, interactive=False):
    self.interactive = interactive

  # note the local part is akin to a contextmanager; it wraps around the remote part.
  # it is given a keyword argument `call_remote` which takes care of the ssh tunnelling
  # and calls the remote part. its signature is `result = call_remote(*args, **kwargs)`,
  # where `result` is the return value of the remote part.
  # note the args, kwargs and result go through serialize/deserialize so are constrained
  # to what json can express. finally, local_fn should return its return value.
  def locally(self, fn, name=None):
    self.local_fn = fn
    self.subcommand = fn.__name__ if name is None else name
    return self

  def remotely(self, fn, name=None):
    self.remote_fn = fn
    self.subcommand = fn.__name__ if name is None else name
    return self

  # total mindfuck courtesy of https://stackoverflow.com/a/47433786/7601527.
  # callable objects can't decorate methods. python strikes again!
  def __get__(self, the_self, type=None):
    return ft.partial(self, the_self)

  @staticmethod
  def from_interactive(interactive, *args, **kwargs):
    return RemoteCommand(interactive)

  def __call__(self, remote, *args, **kwargs):
    if not G.config.on_remote:
      # call local_fn, which will call call_remote with args & kwargs.
      # call_remote will do the ssh roundtrip to invoke remote_fn with said args & kwargs.
      def call_remote(*args, **kwargs):
        return self.roundtrip(remote, self.subcommand, *args, **kwargs)
      return self.local_fn(remote, *args, call_remote=call_remote, **kwargs)
    else:
      if self.interactive:
        self.remote_fn(remote, *args, **kwargs)
      else:
        # non-interactive effectively means the command has a structured return
        # value. this case is painful because ssh only gives us two channels
        # through which the remote command can communicate back to us: the return
        # code and the output. we serialize the structured return value and dump
        # it in the output, labeled with an identifier.
        rpc_identifier = kwargs.pop("rpc_identifier")
        result = self.remote_fn(remote, *args, **kwargs)
        print("rpc-response", rpc_identifier, serialize(result))

  def roundtrip(self, remote, method, *argv, **kwargs):
    # TODO: can avoid this if we make a local copy each time we copy to remote. then we can check against local copy
    detour_bin_path = os.path.realpath(sys.argv[0])
    sp.check_call(remote.ssh_wrapper + ["scp", detour_bin_path, "%s:bin/detour" % remote.host])

    if self.interactive:
      if True:
        # hoop-jumpery to get a motherfucking login shell. let's see how many levels of wrapping and mangling we need
        rpc_argv = detour_rpc_argv(method, *argv, rpc_kwargs=kwargs, on_remote=remote.key)
        thing = " ".join(rpc_argv)
        assert "'" not in thing
        return sp.check_call(remote.ssh_wrapper + ["ssh", "-t", remote.host, "bash -l -c '%s'" % thing])
      else:
        return sp.check_call(remote.ssh_wrapper + ["ssh", "-t", remote.host] +
                             detour_rpc_argv(method, *argv, rpc_kwargs=kwargs, on_remote=remote.key))
    else:
      # try to maintain interactivity so we can drop into pdb in the remote code.
      # if all goes well (i.e. no exceptions) there will be no interaction and the
      # (serialized) return value of the remote code will be in the output,
      # labeled with the rpc identifier. a better way to do this would be
      # to capture only stdout and not stderr, but that is just not possible:
      # https://stackoverflow.com/questions/34186035/can-you-fool-isatty-and-log-stdout-and-stderr-separately
      rpc_identifier = random_string(8)
      command = (remote.ssh_wrapper + ["ssh", "-t", remote.host] +
                 detour_rpc_argv(method, *argv, on_remote=remote.key,
                                 rpc_kwargs=dict(rpc_identifier=rpc_identifier, **kwargs)))
      output = check_output_interactive(command)
      for line in output.splitlines():
        if line.startswith("rpc-response"):
          parts = line.split()
          if parts[0] == "rpc-response" and parts[1] == rpc_identifier:
            result = parts[2]
            break
      else:
        raise RuntimeError("no rpc-response in output")
      return deserialize(result)

# helper to reduce the brain damage
def decorator_with_args(decorator):
  def uno(*args, **kwargs):
    def dos(function):
      return decorator(function, *args, **kwargs)
    return dos
  return uno

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
  def local_fn(remote, *args, call_remote=None, **kwargs):
    return call_remote(*args, **kwargs)
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
               G.localdb.runsdir)

  def rsync(self, sources, destination):
    for subset in segments(sources, 100):
      for i in range(5):
        try:
          sp.check_call(self.ssh_wrapper + ["rsync", "-urltvz", "--ignore-missing-args"] +
                        rsync_filter + subset + [destination])
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
    self.rsync([G.localdb.get_rundir(label) for label in labels],
               "%s:%s" % (self.host, self.database.runsdir))

  def purge(self, labels):
    if not labels: return
    for subset in segments(labels, 100):
      sp.check_call(self.ssh_wrapper + ["ssh", self.host] +
                    ["rm", "-rf"] +
                    [str(self.database.get_rundir(label)) for label in subset])
      sp.check_call(["rm", "-r"] +
                    [str(G.localdb.get_rundir(label)) for label in subset])

  @remotely(interactive=True)
  def visit(self, label):
    sp.check_call("bash", cwd=str(self.database.get_rundir(label)))

  @remotely(interactive=False)
  def statusmany(self, labels):
    # FIXME move this all into database?
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
      if "Invalid job id specified" in blob.decode("utf-8"):
        # a preliminary trial suggests this error only occurs when len(job_ids) == 1, and
        # invalid job ids are ignored otherwise. we can ignore this error.
        # the missing entries in statuses will be caught below.
        pass
      else:
        logging.error("something went wrong in call to squeue, dropping into pdb")
        import pdb; pdb.set_trace()

    for job_id, label in job_ids.items():
      if label not in statuses:
        # this could happen if the job has terminated, in which case the local part should
        # pull it out of the `terminated` file. however due to race conditions (the job
        # terminated in the time it took for control to arrive here) we'll take care of it.
        statuses[label] = self.database.get_status(label)

    if any(label not in statuses for label in job_ids.values()):
      logger.error("could not determine status of the following jobs:")
      for job_id, label in job_ids.items():
        if label not in statuses:
          logger.error("job %s with label %s", job_id, label)
      import pdb; pdb.set_trace()

    return statuses

  @statusmany.locally
  def statusmany(self, labels, call_remote):
    if not labels: return dict()
    statuses = call_remote(labels)
    for label, status in statuses.items():
      if status == "no remote copy":
        G.localdb.mark_terminated(label, "no remote copy")
    return statuses

  @remotely_only(interactive=True)
  def run(self, label):
    sp.check_call(["hostname"])
    sp.check_call(["nvidia-smi"])
    status = None
    try:
      # record job number so we can determine status later, as well as whether it got started at all
      job_id = os.environ["SLURM_JOB_ID"]
      self.database.set_job_id(label, job_id)
      logger.warning("detour run label %s job_id %s start %s", label, job_id, get_timestamp())
      invocation = self.database.get_invocation(label)
      os.environ["DETOUR_LABEL"] = label # for the user program
      logger.warning("invoking %s", invocation)
      status = sp.check_call(invocation, cwd=str(Path(self.database.get_rundir(label), "tree")))
    finally:
      if status is None:
        status = tb.format_exc()
      self.database.mark_terminated(label, str(status))

  @locally(interactive=False)
  def synchronizing(self, label):
    while not G.localdb.known_terminated(label):
      interruptible_sleep(30)
      # TODO: what to do on failure?
      self.pull([label])

  @contextlib.contextmanager
  def synchronization(self, label):
    synchronizer = sp.Popen(detour_rpc_argv("synchronizing", label),
                            stdin=sp.DEVNULL, stdout=sp.DEVNULL, stderr=sp.DEVNULL)
    yield
    synchronizer.terminate()
    synchronizer.wait()
    if not G.localdb.known_terminated(label):
      self.pull([label])


  # BATCH functionality
  def launch_batch(self, labels, preset=None):
    self.push(labels)
    self.submit_jobs(labels, preset=preset)

  @remotely(interactive=False)
  def resubmit(self, labels, preset=None):
    job_ids = dict()
    for label in labels:
      # clear state before call to _submit_job, not after, to avoid clearing new job_id
      self.database.clear_state(label)
      interruptible_sleep(2) # throttle
      job_ids[label] = self._submit_batch_job(label, preset=preset)
    return job_ids

  @resubmit.locally
  def resubmit(self, labels, preset=None, call_remote=None):
    job_ids = call_remote(labels, preset=preset)
    # clear state of the jobs that the remote has launched
    for label in job_ids.keys():
      G.localdb.clear_state(label)
    return job_ids

  @remotely(interactive=False)
  def submit_jobs(self, labels, preset=None):
    job_ids = dict()
    for label in labels:
      interruptible_sleep(2) # throttle
      job_ids[label] = self._submit_batch_job(label, preset=preset)
    return job_ids


  # INTERACTIVE functionality
  def launch_interactive(self, labels, preset=None):
    # interactive means run one by one
    for label in labels:
      self.push([label])
      logger.warning("enter_screen %s", label)
      self.enter_screen(label, preset=preset)

  @remotely(interactive=True)
  def enter_screen(self, label, preset=None):
    logger.warning("entered_screen %s", label)
    rundir = self.database.ensure_rundir(label)

    screenlabel = self.database.get_screenlabel(label)
    inner_command = ["script", "-e", "session_%s.script" % get_timestamp()]
    sp.check_call(["screen", "-d", "-m", "-S", screenlabel] + inner_command, cwd=str(rundir))

    # `stuff` sends the given string to stdin, i.e. we run the command as if the user had typed
    # it.  the benefit is that we can attach and we are in a bash prompt with exactly the same
    # environment as the program ran in (as opposed to would be the case with some other ways of
    # keeping the screen window alive after the program terminates)
    # NOTE: too bad the command is "detour ..." which isn't really helpful
    # NOTE: && exit ensures screen terminates iff the command terminated successfully.
    sp.check_call(["screen", "-S", screenlabel, "-p", "0", "-X", "stuff",
                   "%s && exit^M" % " ".join(detour_rpc_argv("submit_interactive_job", label))],
                  cwd=str(rundir))
    sp.check_call(["screen", "-x", screenlabel],
                  env=dict(TERM="xterm-color"))

  @enter_screen.locally
  def enter_screen(self, label, preset=None, call_remote=None):
    with self.synchronization(label):
      call_remote(label, preset=preset)

  @remotely_only(interactive=True)
  def submit_interactive_job(self, label, preset=None):
    return self._submit_interactive_job(label, preset=preset)

  @remotely(interactive=True)
  def attach(self, label):
    sp.check_call(["screen", "-x", self.database.get_screenlabel(label)])

  @attach.locally
  def attach(self, label, call_remote):
    with self.synchronization(label):
      call_remote(label)


def get_remote(key):
  return REMOTES[key]()

@dict
@vars
class REMOTES:
  class mila(Remote):
    key = "mila"
    ssh_wrapper = "pshaw mila".split()
    host = "mila2"
    runsdir = Path("/network/tmp1/cooijmat/detours")

    excluded_hosts = [
      # TODO figure out if we need to exclude anything. mila00/01 don't exist anymore
      #"mila00", # requires nvidia acknowledgement
      #"mila01", # requires nvidia acknowledgement
      "leto08", # consistently no devices found
      "instinct1", # broken in many ways?
      "eos19", # stall
    ]

    def _submit_interactive_job(self, label, preset=None):
      preset_flags = ["--%s=%s" % item for item in get_preset(preset, self.key).items()]
      command = " ".join(detour_rpc_argv("run", label))
      sp.check_call(["srun", *preset_flags,
                     "--exclude=%s" % ",".join(self.excluded_hosts),
                     "--qos=unkillable",
                     "--pty",
                     # NOTE: used to have bash -lic, but this sets the crucial CUDA_VISIBLE_DEVICES variable to the empty string
                     "bash", "-ic", 'conda activate py36; %s' % command])

    def _submit_batch_job(self, label, preset=None):
      rundir = self.database.ensure_rundir(label)
      command = " ".join(detour_rpc_argv("run", label))
      sbatch_flags = dict(qos="high",
                          exclude=",".join(self.excluded_hosts),
                          **get_preset(preset, self.key))
      sbatch_crud = "\n".join("#SBATCH --%s=%s" % item for item in sbatch_flags.items())

      # TODO run in $SLURMTMP if it ever matters
      Path(rundir, "sbatch.sh").write_text(textwrap.dedent("""
        #!/bin/bash
        %s
        #set -e
        source ~/.bashrc
        conda activate py36
        %s
      """ % (sbatch_crud, command)).strip())

      output = sp.check_output(["sbatch", "sbatch.sh"], cwd=str(rundir))
      match = re.match(rb"\s*Submitted\s*batch\s*job\s*(?P<id>[0-9]+)\s*", output)
      if not match:
        print(output)
        import pdb; pdb.set_trace()
      job_id = match.group("id").decode("ascii")
      self.database.set_job_id(label, job_id)
      return job_id

  class cedar(Remote):
    key = "cedar"
    ssh_wrapper = "pshaw cedar".split()
    host = "cedar"
    runsdir = Path("/home/cooijmat/projects/rpp-bengioy/cooijmat/detours")

    def _submit_interactive_job(self, label, preset=None):
      # NOTE untested
      excluded_hosts = []
      preset_flags = ["--%s=%s" % item for item in get_preset(preset, self.key).items()]
      command = " ".join(detour_rpc_argv("run", label))
      sp.check_call(["srun", *preset_flags,
                     "--account=rpp=bengioy",
                     "--exclude=%s" % ",".join(excluded_hosts),
                     "--pty",
                     # NOTE: used to have bash -lic, but this sets the crucial CUDA_VISIBLE_DEVICES variable to the empty string
                     "bash", "-ic", 'source $HOME/environment_setup.sh; %s' % command])

    def _submit_batch_job(self, label, preset=None):
      rundir = self.database.ensure_rundir(label)

      # make a script describing the job
      command = " ".join(detour_rpc_argv("run", label))
      sbatch_crud = "\n".join("#SBATCH --%s=%s" % item for item in get_preset(preset, self.key).items())
      # TODO run in $SLURMTMP if it ever matters
      Path(rundir, "sbatch.sh").write_text(textwrap.dedent("""
        #!/bin/bash
        #SBATCH --account=rpp-bengioy
        %s
        source $HOME/environment_setup.sh
        %s
      """ % (sbatch_crud, command)).strip())

      output = sp.check_output(["sbatch", "sbatch.sh"], cwd=str(rundir))
      match = re.match(rb"\s*Submitted\s*batch\s*job\s*(?P<id>[0-9]+)\s*", output)
      if not match:
        print(output)
        import pdb; pdb.set_trace()
      job_id = match.group("id").decode("ascii")
      self.database.set_job_id(label, job_id)
      return job_id

class Database(object):
  # TODO: all these getters and setters suggest a Run object that manages each run's properties

  def __init__(self, runsdir):
    self.runsdir = runsdir

  def get_rundir(self, label):
    return Path(self.runsdir, label)

  def ensure_rundir(self, label):
    rundir = self.get_rundir(label)
    make_that_dir(rundir)
    return rundir

  def exists(self, label):
    return self.get_rundir(label).exists()

  @property
  def configs(self):
    for config_path in glob.glob(os.path.join(self.runsdir, "*", "config.json")):
      config = json.loads(Path(config_path).read_text())
      yield config

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

  def by_remote(self, labels):
    for remote_key, labels in groupby(labels, self.get_remote_key).items():
      remote = get_remote(remote_key)
      yield remote, labels

  def get_job_id(self, label):
    try:
      return self.get_job_id_path(label).read_text()
    except FileNotFoundError:
      return None

  def set_job_id(self, label, job_id):
    self.get_job_id_path(label).write_text(job_id)

  def get_job_id_path(self, label):
    return Path(self.runsdir, label, "job_id")

  def known_terminated(self, label):
    return self.get_terminated_path(label).exists()

  def get_terminated_path(self, label):
    return Path(self.runsdir, label, "terminated")

  def mark_terminated(self, label, context):
    terminated_path = self.get_terminated_path(label)
    if terminated_path.exists():
      logger.warning("overwriting termination file for run %s", label)
    terminated_path.touch()
    terminated_path.write_text(context)

  def mark_lost(self, label):
    self.mark_terminated(label, "lost")

  def unmark_lost(self, label):
    assert self.is_marked_lost(label)
    self.get_terminated_path(label).unlink()

  def is_marked_lost(self, label):
    try:
      return self.get_terminated_path(label).read_text() == "lost"
    except FileNotFoundError:
      return False

  def clear_state(self, label):
    try:
      self.get_job_id_path(label).unlink()
    except FileNotFoundError:
      pass
    try:
      self.get_terminated_path(label).unlink()
    except FileNotFoundError:
      pass
    # we can probably leave the output files of past runs?

  def package(self, *invocation, **config):
    # NOTE: did I want to include invocation in the digest?
    # gather files and determine checksum
    with tempfile.TemporaryDirectory() as tmpdir:
      path = Path(tmpdir, "tree")
      sp.check_call(["rsync", "-rlzF"] + rsync_filter + ["./", str(path)])
      digest_output = sp.check_output(
        # (would prefer to use find -print0 and tar --null, but the latter doesn't seem to work)
        "tar -cf - %s | sha256sum" % path,
        shell=True, cwd=tmpdir)
      # with timestamp, 4 characters (32 bits) should be plenty
      digest = digest_output.decode().splitlines()[0].split()[0][:4]

      timestamp = get_timestamp()
      label = "%s_%s" % (timestamp, digest)

      # create rundir and move files into place
      shutil.move(path, Path(self.ensure_rundir(label), "tree"))

    config["label"] = label
    self.set_invocation(label, invocation)
    self.set_config(label, config)
    logger.warning("invocation: %r", invocation)
    logger.warning("label: %r", label)
    logger.warning("config: %r", config)
    return label

  def study_labels(self, study):
    for path in sorted(glob.glob(self.runsdir + "/*/config.json")):
      config = json.loads(Path(path).read_text())
      if config.get("study", None) == study:
        yield config["label"]
  def designated_labels(self, labels, ignore_nonexistent=False, deduplicate=True):
    result = []
    for label in labels:
      path = self.get_rundir(label)
      if path.exists():
        result.append(label)
      else: # assume it's a study
        study_labels = self.study_labels(label)
        if not study_labels and not ignore_nonexistent:
          raise KeyError("unknown run or study", label)
        result.extend(study_labels)
    if deduplicate:
      result = dedup(result)
    return result

  def get_screenlabel(self, label):
    return "detour_%s" % label

  def get_stdout_path(self, label):
    for pattern in "session*.script slurm-*.out".split():
      expression = str(Path(self.runsdir, label, pattern))
      paths = glob.glob(expression)
      if paths:
        path = sorted(paths, key=os.path.getmtime)[-1]
        return Path(path)
    return None

  def get_output(self, label):
    stdout_path = self.get_stdout_path(label)
    if not stdout_path:
      logger.warning("no stdout found for %s", label)
      return ""
    return stdout_path.read_text()

  def get_status(self, label):
    def _from_output(label):
      output = self.get_output(label)
      errors = extract_errors(output)
      errors = [abridge(error, 80) for error in errors]
      return "; ".join(errors)

    try:
      status = self.get_terminated_path(label).read_text()
      # status may be a traceback in case of exception
      if "CalledProcessError" in status:
        # uninformative exception in "detour run" command; extract the true exception from slurm-*.out
        status = _from_output(label)
    except FileNotFoundError:
      # check the output for slurm errors. e.g.:
      # slurmstepd: error: *** JOB 11943965 ON cdr248 CANCELLED AT 2018-09-18T09:30:03 DUE TO TIME LIMIT ***
      status = _from_output(label)
      # not marked as terminated; either the job has not terminated, or
      # it was terminated abruptly, e.g. by SIGKILL from slurm
      if not status:
        status = "unterminated"
    return status

  def get_total_size(self, labels):
    rundirs = [self.get_rundir(label) for label in labels]
    entries = sp.check_output("du -chs".split() + rundirs).decode("utf-8").splitlines()
    # last line is "<size> total"
    return entries[-1].split()[0]


def extract_errors(output):
  errors = []
  if "error:" in output:
    # look for low-level errors (slurmstepd, linking, ...)
    for line in output.splitlines():
      if "error:" in line:
        errors.append(line)
  # also include any exceptions we find
  exceptions = extract_exceptions(output)
  errors.extend(exceptions)
  return errors

def extract_exceptions(output):
  re_ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
  re_exception = re.compile(r"^[A-Za-z_.]+(Error|Exception)\s*:?\s*.*$")
  exceptions = []
  for line in output.splitlines():
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

def get_preset(key, remote):
  key = key or "classic"
  presets = dict(light=dict(time="1:00:00", mem="4G", gres="gpu:1"),
                 classic=dict(time="23:59:59", mem="16G", gres="gpu:1"))
  preset = presets[key]
  return preset


class RPCCodec:
  @ft.singledispatch
  def encode(x):
    return "x", serialize(x)
  encode.register(int)(lambda x: ("i", str(x)))
  encode.register(float)(lambda x: ("f", str(x)))
  encode.register(str)(lambda x: ("s", str(x)) if x.isidentifier() else ("x", serialize(x)))

  @classmethod
  def decode(cls, tag, s):
    try:
      return dict(i=int, f=float, s=str)[tag](s)
    except KeyError:
      assert tag == "x"
      return deserialize(s)

  @classmethod
  def encode_args(cls, *args, **kwargs):
    # serialize args/kwargs, human readable where possible, e.g.:
    #   sfi,sxsf arg1 arg3 arg4 key1 value1 key2 value2
    #   \      /
    #    legend indicates how to handle each arg:
    #   s for string, i for int, f for float, x for serialized
    #   comma (,) indicates end of positional arguments and beginning of kwarg pairs
    tags = []
    argv = []
    def _process(x):
      tag, s = cls.encode(x)
      tags.append(tag)
      argv.append(s)
    for x in args:
      _process(x)
    tags.append(",")
    for key, value in kwargs.items():
      _process(key); _process(value)
    return ["".join(tags), *argv]

  @classmethod
  def decode_args(cls, legend, *argv):
    tags = deque(legend)
    argv = deque(argv)
    assert len(legend) == len(argv) + 1

    args = []
    while tags:
      tag = tags.popleft()
      if tag == ",":
        break
      args.append(cls.decode(tag, argv.popleft()))

    kwargs = {}
    assert len(tags) % 2 == 0
    while tags:
      key, value = [cls.decode(tags.popleft(), argv.popleft()) for _ in range(2)]
      kwargs[key] = value

    return args, kwargs


class CommandTreeTools:
  @classmethod
  def parse_and_call(cls, fntree, argv):
    parser = cls.construct_parser(fntree)
    args = parser.parse_args(argv)
    cls.call(fntree, args)

  @classmethod
  def construct_parser(cls, fntree, _constructor=argparse.ArgumentParser):
    parser = _constructor()
    cls.populate_parser(parser, fntree.fn)
    if fntree.children:
      subparsers = parser.add_subparsers(
        dest="_subcommand", # required=True, FIXME documented to exist but not accepted
        parser_class=NestedArgumentParser.factory(namespace_name="_subnamespace"))
      for name, subtree in fntree.children.items():
        cls.construct_parser(subtree, _constructor=ft.partial(subparsers.add_parser, name))
    return parser

  @classmethod
  def call(cls, fntree, args):
    ba = inspect.BoundArguments(
      signature=inspect.signature(fntree.fn),
      arguments={k: v for k, v in vars(args).items() if not k.startswith("_")})
    if fntree.children:
      with contextlib.contextmanager(fntree.fn)(*ba.args, **ba.kwargs):
        cls.call(fntree.children[args._subcommand], args._subnamespace)
    else:
      fntree.fn(*ba.args, **ba.kwargs)

  @classmethod
  def populate_parser(cls, parser, fn):
    sig = inspect.signature(fn)
    for name, parameter in sig.parameters.items():
      # parameters that start with underscores are considered private and
      # unreachable through command line. also I use underscore prefixes
      # to indicate special members of the Namespace object.
      if name.startswith("_"):
        continue

      # NOTE treating positional-or-keyword as positional
      positional = parameter.kind in [parameter.POSITIONAL_ONLY,
                                      parameter.POSITIONAL_OR_KEYWORD,
                                      parameter.VAR_POSITIONAL]
      singular = parameter.kind in [parameter.POSITIONAL_ONLY,
                                    parameter.KEYWORD_ONLY,
                                    parameter.POSITIONAL_OR_KEYWORD]

      args, kwargs = [], {} # for add_argument
      if positional: args.append(name)
      else:          args.append(f"--{name}")
      if singular:
        # determine type from default # TODO use type annotations if this is too limited
        default = parameter.default
        if   isinstance(default, bool):  kwargs.update(action="store_true")
        elif isinstance(default, int):   kwargs.update(type=int)
        elif isinstance(default, float): kwargs.update(type=float)
        if default is not parameter.empty and not isinstance(default, bool):
          kwargs.update(nargs="?")
        # don't fill in any defaults during parsing
        kwargs.update(default=argparse.SUPPRESS)
      else:
        if positional: kwargs.update(nargs=argparse.REMAINDER)
        else:          raise NotImplementedError()
      parser.add_argument(*args, **kwargs)

def detour_rpc_argv(method, *args, **kwargs):
  rpc_kwargs = kwargs.pop("rpc_kwargs", dict())
  rpc_argv = RPCCodec.encode_args(*args, **rpc_kwargs)
  return detour_argv("rpc", method, *rpc_argv, **kwargs)

def detour_argv(*argv, **config):
  the_config = dict()
  the_config.update(G.config)
  the_config.update(config)
  flags = []
  for key, value in the_config.items():
    if isinstance(value, bool): # sigh
      if value:
        flags.append("--%s" % key)
    else:
      flags.append("--%s=%s" % (key, value))
  return ["detour"] + flags + list(argv)


# GENERAL UTILITIES #################

def interruptible_sleep(seconds):
  for second in range(seconds):
    time.sleep(1)

def make_that_dir(path):
  path = str(path) # convert pathlib Path (which has mkdir but exist_ok is py>=3.5)
  os.makedirs(path, exist_ok=True)

def dedup(xs): # deduplicate while maintaining order
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

def serialize(x): return base64.urlsafe_b64encode(zlib.compress(json.dumps(x).encode("utf-8"))).decode("utf-8")
def deserialize(x): return json.loads(zlib.decompress(base64.urlsafe_b64decode(x)).decode("utf-8"))

def random_string(length):
  import random, string
  alphabet = string.ascii_lowercase + string.ascii_uppercase + string.digits
  return "".join(random.SystemRandom().choice(alphabet) for _ in range(length))

def get_timestamp():
  return datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def segments(xs, k):
  iterator = iter(xs)
  while True:
    slice = list(it.islice(iterator, k))
    if not slice:
      break
    yield slice

def all_equal(xs):
  xs = list(xs)
  return all(x == xs[0] for x in xs)

def eqzip(*xs):
  xs = tuple(map(list, xs))
  assert all_equal(map(len, xs))
  return zip(*xs)

def unzip(tuples):
  return eqzip(*tuples)

def map_values(fn, mapping):
  return {k: fn(v) for k, v in mapping.items()}

class NestedArgumentParser(argparse.ArgumentParser):
  def __init__(self, *args, namespace_name=None, **kwargs):
    super().__init__(*args, **kwargs)
    assert namespace_name is not None
    self.namespace_name = namespace_name
  def parse_args(self, args=None, namespace=None):
    subnamespace = argparse.Namespace()
    subnamespace = super().parse_args(args=args, namespace=subnamespace)
    if namespace is None:
      namespace = argparse.Namespace()
    setattr(namespace, self.namespace_name, subnamespace)
    return namespace
  def parse_known_args(self, args=None, namespace=None):
    subnamespace = argparse.Namespace()
    subnamespace, arg_strings = super().parse_known_args(args=args, namespace=subnamespace)
    if namespace is None:
      namespace = argparse.Namespace()
    setattr(namespace, self.namespace_name, subnamespace)
    return namespace, arg_strings
  @classmethod
  def factory(cls, *args, **kwargs):
    return ft.partial(cls, *args, **kwargs)

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

if __name__ == "__main__":
  CommandTreeTools.parse_and_call(main, sys.argv[1:])

