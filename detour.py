#!/usr/bin/env python3
import argparse, datetime, json, logging, os, shutil, signal, sys, tempfile, time, re, pdb, textwrap, glob, inspect, zlib
import itertools as it, subprocess as sp, traceback as tb, functools as ft
import contextlib
from pathlib import Path
from collections import OrderedDict as ordict, defaultdict as ddict, deque, namedtuple
import base64
import collections

def decorator_with_args(decorator):
  def uno(*args, **kwargs):
    def dos(function):
      return decorator(function, *args, **kwargs)
    return dos
  return uno

class attrdict(dict):
  __getattr__ = dict.get
  __setattr__ = dict.__setitem__

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

# globals
G = attrdict(config=attrdict(), db=None)

@FunctionTree
def main(*, on_remote=None):
  try:
    G.config.update(on_remote=on_remote)
    G.remote = make_remote(on_remote)
    G.db = G.remote.database
    yield
  except Exception: # TODO was "except:"; trying to let keyboardinterrupt pass through
    tb.print_exc()
    pdb.post_mortem()
    raise
@main.add_children
class _:

  # remote procedure calls
  def rpc(method, *rpc_argv):
    args, kwargs = RPCCodec.decode_args(*rpc_argv) # NOTE kwargs may include rpc_identifier
    try:
      with Path(G.db.runsdir, "detour_rpc_log").open("a") as log:
        log.write("%s %r %r\n" % (method, args, kwargs))
    except: pass
    getattr(G.remote, method)(*args, **kwargs)
  def wtfrpc(method, *rpc_argv):
    # decode and print an rpc argv
    args, kwargs = RPCCodec.decode_args(*rpc_argv)
    print(method, args, kwargs)

  # packaging and launching runs
  def launchcmd(*invocation, interactive=False, remote=None, study=None, preset=None):
    assert not G.config.on_remote
    run = G.db.package(invocation)
    alias = G.db.autoalias(run)
    run.props.study = study
    run.props.remote = remote
    run.props.preset = preset
    if interactive: run.remote.launch_interactive([run])
    else:           run.remote.launch_batch      ([run])
    print("launched", run.present_label)
  def launch(*labels, interactive=False):
    assert not G.config.on_remote
    runs = G.db.designated_runs(labels)
    for remote, runs in G.db.by_remote(runs):
      interruptible_sleep(2) # throttle
      if interactive: remote.launch_interactive(runs)
      else:           remote.launch_batch      (runs)
  def package(*invocation, remote=None, study=None, preset=None):
    assert not G.config.on_remote
    run = G.db.package(invocation)
    run.props.study = study
    run.props.remote = remote
    run.props.preset = preset
    print(run.label)
  def autoalias(label, *, force=False):
    assert not G.config.on_remote
    print(G.db.autoalias(Run(label), force=force))

  @FunctionTree
  def props():
    yield
  @props.add_children
  class _:
    def get(label, *keys):
      run, = G.db.designated_runs([label])
      for key, value in run.props: # TODO pprint as dict?
        print(key, value)
    def set(label, key, value):
      run, = G.db.designated_runs([label])
      run.props[key] = value

  def visit  (label): do_single_remote(label, "visit")
  def attach (label): do_single_remote(label, "attach")
  def push (*labels): do_bulk_remote(labels, "push")
  def pull (*labels): do_bulk_remote(labels, "pull")
  def purge(*labels): do_bulk_remote(labels, "purge", ignore_nonexistent=True)
  def kill (*labels): do_bulk_remote(labels, "kill", ignore_nonexistent=True)
  def killpurge(*labels, _kill=kill, _purge=purge):
    _kill(*labels); _purge(*labels)

  def stdout(label):
    run, = G.db.designated_runs([label])
    path = run.get_output_path()
    if not path:
      logger.warning("no output found for %s", run.labelview)
      sys.exit(1)
    sp.check_call(["less", "+G", path])

  def status(*labels, verbose=False, refresh=False):
    runs = G.db.designated_runs(labels)
    statuses = get_statuses(runs, ignore_cache=refresh)
    def criterion(run):
      status = statuses[run]
      if isinstance(status, dict): return status["state"]
      else:                        return status
    for status, runs in groupby(runs, criterion).items():
      print(len(runs), "runs:", status)
      if verbose:
        for run in runs:
          print("  ", run.labelview)

  def resubmit(*labels):
    runs = G.db.designated_runs(labels)
    for remote, runs in G.db.by_remote(runs):
      remote.resubmit(runs)
  def resubmitunrun(*labels, refresh=False):
    runs = G.db.designated_runs(labels)
    statuses = get_statuses(runs, ignore_cache=refresh)
    unruns = [run for run in runs
              if statuses[run] in ["lost", "no remote copy"]]
    print("about to resubmit %i unrun jobs" % len(unruns))
    for unrun in unruns:
      print(unrun)
    import pdb; pdb.set_trace()
    for remote, runs in G.db.by_remote(unruns):
      push_runs = [run for run in runs if statuses[run] == "no remote copy"]
      remote.push(push_runs)
      remote.resubmit(runs)

  def studies(runsdir=None, *, verbose=False):
    db = Database(runsdir) if runsdir is not None else G.db
    for study, runs in groupby(db.get_runs(), lambda run: run.props.study).items():
      print("%50s %4i runs on %s"
            % (study, len(runs),
              ", ".join("%s (%i) (%s)" %
                        (remote, len(remote_runs), db.get_total_size(remote_runs))
                        for remote, remote_runs in
                        groupby(runs, lambda run: run.props.remote).items())))
      if verbose:
        for run in runs:
          print("  ", run.labelview, run.props.remote, db.get_total_size([run]))


def get_statuses(runs, ignore_cache=False):
  assert not G.config.on_remote
  all_runs = runs
  statuses = ddict()
  for remote, runs in G.db.by_remote(runs):
    if ignore_cache:
      # pull all labels whether locally considered terminated or not
      remote.pull(runs)
    else:
      unterminated = [run for run in runs if not run.known_terminated]
      if unterminated: # TODO move this check into remote.pull
        remote.pull(unterminated)
    # for unterminated, figure out statuses remotely
    unterminated = [run for run in runs if not run.known_terminated]
    if unterminated or ignore_cache: # FIXME condition seems wrong
      statuses.update(remote.status(unterminated))
    # for terminated, figure out statuses locally
    terminated = [run for run in runs if run.known_terminated]
    statuses.update((run, run.get_status()) for run in terminated)
  assert set(statuses.keys()) == set(all_runs)
  return statuses

def do_bulk_remote(labels, method, ignore_nonexistent=False):
  assert not G.config.on_remote
  runs = G.db.designated_runs(labels, ignore_nonexistent=ignore_nonexistent)
  for remote, runs in G.db.by_remote(runs):
    getattr(remote, method)(runs)
def do_single_remote(label, method):
  assert not G.config.on_remote
  run, = G.db.designated_runs([label])
  getattr(run.remote, method)(run)

class RemoteCommand:
  def __init__(self, interactive=False):
    self.interactive = interactive

  # RemoteCommand represents a command that should be run on the remote. it abstracts
  # away the hassles of connecting to the remote and serializing/deserializing arguments
  # and return values. basically RPC.

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
    if not G.config.on_remote: # FIXME does on_remote have to be on some `config`?
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
    # TODO: can avoid this if we make a local copy each time we copy to remote.
    # then we can check against local copy. or keep track of last update using mtime.
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
    raise RuntimeError("subcommand can only be run remotely", fn.__name__)
  rc.locally(local_fn, name=fn.__name__)
  return rc

@decorator_with_args
def locally_only(fn):
  @ft.wraps(fn)
  def wfn(*args, **kwargs):
    if G.config.on_remote:
      raise RuntimeError("subcommand can only be run locally", fn.__name__)
    return fn(*args, **kwargs)
  return wfn

class BaseRemote:
  @property
  def database(self):
    return Database(self.runsdir)

class Remote(BaseRemote):
  @locally_only()
  def pull(self, runs):
    if not runs: return
    rsync(["%s:%s" % (self.host, self.database.get_rundir(run)) for run in runs],
          G.db.runsdir, ssh_wrapper=self.ssh_wrapper)

  @locally_only()
  def push(self, runs):
    if not runs: return
    rsync([run.rundir for run in runs],
          "%s:%s" % (self.host, self.database.runsdir),
          ssh_wrapper=self.ssh_wrapper)

  @locally_only()
  def purge(self, runs):
    if not runs: return
    for subset in segments(runs, 100): # to break up long argument list (e.g. large study)
      sp.check_call(self.ssh_wrapper + ["ssh", self.host] +
                    # -f because we don't care if any don't exist
                    ["rm", "-rf"] + [str(run.rundir) for run in subset])
      sp.check_call(["rm", "-rf"] + [str(run.rundir) for run in subset])
    for run in runs:
      if run.props.alias:
        self.database.unbind_alias(run.props.alias)

  @remotely(interactive=True)
  def visit(self, run):
    sp.check_call("bash", cwd=str(run.rundir))

  @remotely(interactive=False)
  def status(self, runs):
    statuses = dict()
    run_by_jobid = ordict()

    for run in runs:
      if not run.rundir.exists():
        statuses[run] = "no remote copy"
      else:
        if run.props.jobid is None:
          run.mark_terminated("lost")
          statuses[run] = "lost"
        else:
          run_by_jobid[run.props.jobid] = run

    for jobid, status in squeue(list(run_by_jobid.keys()), fields="state reason timeused timeleft"):
      statuses[run_by_jobid[jobid]] = status

    for jobid, run in run_by_jobid.items():
      if run not in statuses:
        # this could happen if the job has terminated, in which case the local part should
        # pull it out of the `terminated` file. however due to race conditions (the job
        # terminated in the time it took for control to arrive here) we'll take care of it.
        statuses[run] = run.get_status()

    assert set(runs) == set(statuses.keys())
    return statuses

  @status.locally
  def status(self, runs, call_remote):
    if not runs: return dict()
    statuses = call_remote(runs)
    for run, status in statuses.items():
      if status == "no remote copy":
        # in this case there is no run.props.terminated file that will be rsynced back,
        # so we must manually mark terminated locally.
        run.mark_terminated("no remote copy")
    return statuses

  @remotely(interactive=True)
  def kill(self, runs):
    sp.check_call(["scancel", *(run.props.job_id for run in runs)])

  @remotely_only(interactive=True)
  def run(self, run):
    sp.check_call(["hostname"])
    sp.check_call(["nvidia-smi"])
    # record job number so we can determine status later, as well as whether it got started at all
    job_id = os.environ["SLURM_JOB_ID"]
    run.props.job_id = job_id
    logger.warning("detour run label %s job_id %s start %s", run.label, job_id, get_timestamp())
    invocation = run.props.invocation
    os.environ["DETOUR_LABEL"] = run.label # for the user program
    logger.warning("invoking %s", invocation)
    try:
      status = sp.check_call(invocation, cwd=str(Path(run.rundir, "tree")))
    except:
      status = tb.format_exc()
      raise
    finally:
      run.props.terminated = str(status)

  @locally(interactive=False)
  def synchronizing(self, run):
    while not run.known_terminated:
      interruptible_sleep(30)
      # TODO: what to do on failure?
      self.pull([run])

  @contextlib.contextmanager
  def synchronization(self, run):
    synchronizer = sp.Popen(detour_rpc_argv("synchronizing", run),
                            stdin=sp.DEVNULL, stdout=sp.DEVNULL, stderr=sp.DEVNULL)
    yield
    synchronizer.terminate()
    synchronizer.wait()
    if not run.known_terminated:
      self.pull([run])


  # BATCH functionality
  # the batch flow is launch_batch -(onto remote)-> submit_jobs -> run

  @locally_only()
  def launch_batch(self, runs):
    self.push(runs)
    self.submit_jobs(runs)

  @remotely(interactive=False)
  def submit_jobs(self, runs):
    job_ids = dict()
    for run in runs:
      interruptible_sleep(2) # throttle
      job_ids[run] = self._submit_batch_job(run)
    return job_ids

  @remotely(interactive=False)
  def resubmit(self, runs):
    job_ids = dict()
    for run in runs:
      # clear state before call to _submit_job, not after, to avoid clearing new job_id
      run.prepare_resubmit()
      interruptible_sleep(2) # throttle
      job_ids[run] = self._submit_batch_job(run)
    return job_ids

  @resubmit.locally
  def resubmit(self, runs, call_remote=None):
    job_ids = call_remote(runs)
    # clear state of the jobs that the remote has launched
    for run in job_ids.keys():
      run.prepare_resubmit()
    return job_ids


  # INTERACTIVE functionality
  # the interactive flow is launch_interactive -(onto remote)-> enter_screen -> submit_interactive_job -> run
  @locally_only()
  def launch_interactive(self, runs):
    # interactive means run one by one
    for run in runs:
      self.push([run])
      self.enter_screen(run)

  @remotely(interactive=True)
  def enter_screen(self, run):
    mkdirp(run.rundir)
    inner_command = ["script", "-e", "session_%s.script" % get_timestamp()]
    sp.check_call(["screen", "-d", "-m", "-S", run.screenlabel] + inner_command, cwd=str(run.rundir))
    # NOTE: && exit ensures screen terminates iff the command terminated successfully.
    sp.check_call(["screen", "-S", run.screenlabel, "-p", "0", "-X", "stuff",
                   "%s && exit^M" % " ".join(detour_rpc_argv("submit_interactive_job", run.run))],
                  cwd=str(run.rundir))
    sp.check_call(["screen", "-x", run.screenlabel],
                  env=dict(TERM="xterm-color"))

  @enter_screen.locally
  def enter_screen(self, run, call_remote=None):
    with self.synchronization(run):
      call_remote(run)

  @remotely_only(interactive=True)
  def submit_interactive_job(self, run):
    return self._submit_interactive_job(run)

  @remotely(interactive=True)
  def attach(self, run):
    sp.check_call(["screen", "-x", run.screenlabel])

  @attach.locally
  def attach(self, run, call_remote):
    with self.synchronization(run):
      call_remote(run)

def make_remote(key):
  if key is None:
    key = "local"
  return REMOTES[key]()

@dict
@vars
class REMOTES:
  # having a dummy `local` remote may or may not simplify things down the road
  class local(BaseRemote):
    key = "local"
    runsdir = Path(".detours")

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
      "eos15", # something or other
      "leto16", #stall
    ]

    def _submit_interactive_job(self, run):
      preset_flags = ["--%s=%s" % item for item in get_preset(run.props.preset, self.key).items()]
      command = " ".join(detour_rpc_argv("run", run))
      sp.check_call(["srun", *preset_flags,
                     "--exclude=%s" % ",".join(self.excluded_hosts),
                     "--qos=unkillable",
                     "--pty",
                     # NOTE: used to have bash -lic, but this sets the crucial CUDA_VISIBLE_DEVICES variable to the empty string
                     "bash", "-ic", 'conda activate py36; %s' % command])

    def _submit_batch_job(self, run):
      mkdirp(run.rundir)
      command = " ".join(detour_rpc_argv("run", run))
      sbatch_flags = dict(qos="high",
                          exclude=",".join(self.excluded_hosts),
                          **get_preset(run.props.preset, self.key))
      sbatch_crud = "\n".join("#SBATCH --%s=%s" % item for item in sbatch_flags.items())

      # TODO run in $SLURMTMP if it ever matters
      Path(run.rundir, "sbatch.sh").write_text(textwrap.dedent("""
        #!/bin/bash
        %s
        #set -e
        source ~/.bashrc
        conda activate py36
        %s
      """ % (sbatch_crud, command)).strip())

      output = sp.check_output(["sbatch", "sbatch.sh"], cwd=str(run.rundir))
      match = re.match(rb"\s*Submitted\s*batch\s*job\s*(?P<id>[0-9]+)\s*", output)
      if not match:
        print(output)
        import pdb; pdb.set_trace()
      job_id = match.group("id").decode("ascii")
      run.props.job_id = job_id
      return job_id

  class cedar(Remote):
    key = "cedar"
    ssh_wrapper = "pshaw cedar".split()
    host = "cedar"
    runsdir = Path("/home/cooijmat/projects/rpp-bengioy/cooijmat/detours")

    def _submit_interactive_job(self, run):
      # NOTE untested
      excluded_hosts = []
      preset_flags = ["--%s=%s" % item for item in get_preset(run.props.preset, self.key).items()]
      command = " ".join(detour_rpc_argv("run", run))
      sp.check_call(["srun", *preset_flags,
                     "--account=rpp=bengioy",
                     "--exclude=%s" % ",".join(excluded_hosts),
                     "--pty",
                     # NOTE: used to have bash -lic, but this sets the crucial CUDA_VISIBLE_DEVICES variable to the empty string
                     "bash", "-ic", 'source $HOME/environment_setup.sh; %s' % command])

    def _submit_batch_job(self, run):
      mkdirp(run.rundir)

      # make a script describing the job
      command = " ".join(detour_rpc_argv("run", run))
      sbatch_crud = "\n".join("#SBATCH --%s=%s" % item for item in get_preset(run.props.preset, self.key).items())
      # TODO run in $SLURMTMP if it ever matters
      Path(run.rundir, "sbatch.sh").write_text(textwrap.dedent("""
        #!/bin/bash
        #SBATCH --account=rpp-bengioy
        %s
        source $HOME/environment_setup.sh
        %s
      """ % (sbatch_crud, command)).strip())

      output = sp.check_output(["sbatch", "sbatch.sh"], cwd=str(run.rundir))
      match = re.match(rb"\s*Submitted\s*batch\s*job\s*(?P<id>[0-9]+)\s*", output)
      if not match:
        print(output)
        import pdb; pdb.set_trace()
      job_id = match.group("id").decode("ascii")
      run.props.job_id = job_id
      return job_id

class Run(namedtuple("Run", "label")):
  @property
  def rundir(self):
    return Path(G.db.runsdir, self.label)

  @property
  def props(self):
    return Props(Path(self.rundir, "props"))

  @property
  def config(self):
    # legacy; return whatever used to be stored in `config.json`
    config_path = Path(self.rundir, "config.json")
    if config_path.exists():
      # migrate config.json files to props
      config = json.loads(config_path.read_text())
      for key, value in config:
        self.props[key] = value
    else:
      return dict(study=self.props.study,
                  remote=self.props.remote,
                  label=self.props.label)

  @property
  def remote(self):
    return make_remote(self.props.remote)

  @property
  def screenlabel(self):
    return "detour_%s" % label
  @property
  def labelview(self):
    return G.db.present_label(self)

  def get_output_path(self):
    for pattern in "session*.script slurm-*.out".split():
      expression = str(Path(self.rundir, pattern))
      paths = glob.glob(expression)
      if paths:
        path = sorted(paths, key=os.path.getmtime)[-1]
        return Path(path)
    return None

  def get_output(self):
    path = self.get_output_path()
    if not path:
      logger.warning("no output found for %s", self.labelview)
      return ""
    return path.read_text()

  @property
  def known_terminated(self):
    return self.props.terminated is not None
  def mark_terminated(self, context):
    prev_context = self.props.terminated
    if prev_context is not None:
      logger.warning("overwriting termination file for run %s", run.viewlabel)
    self.props.terminated = context
  def prepare_resubmit(self):
    del self.props.job_id
    del self.props.terminated
    # we can probably leave the output files of past runs?

  def get_status(self):
    def _from_output():
      output = self.get_output()
      errors = extract_errors(output)
      errors = [abridge(error, 80) for error in errors]
      return "; ".join(errors)

    status = self.props.terminated
    if status is not None:
      # status may be a traceback in case of exception
      if "CalledProcessError" in status:
        # uninformative exception in "detour run" command; extract the true exception from slurm-*.out
        status = _from_output()
    else:
      # check the output for slurm errors. e.g.:
      # slurmstepd: error: *** JOB 11943965 ON cdr248 CANCELLED AT 2018-09-18T09:30:03 DUE TO TIME LIMIT ***
      status = _from_output()
      # not marked as terminated; either the job has not terminated, or
      # it was terminated abruptly, e.g. by SIGKILL from slurm
      if not status:
        status = "unterminated"
    return status


class Database(object):
  def __init__(self, runsdir):
    self.runsdir = Path(runsdir)

  def get_runs(self):
    for rundir in self.runsdir.glob("????????_??????_????"):
      yield Run(rundir.name)

  # NOTE this function is still needed because `run.rundir` uses `G.db`, when
  # sometimes you want to get the rundir for a remote that you're not currently
  # on. (see `Remote.push`/`Remote.pull`)
  def get_rundir(self, run):
    return Path(self.runsdir, run.label)

  def by_remote(self, runs):
    for remote, runs in groupby(runs, lambda run: run.props.remote).items():
      yield make_remote(remote), runs

  def package(self, invocation):
    # gather files and determine checksum
    with tempfile.TemporaryDirectory() as tmpdir:
      tmp_rundir = Path(tmpdir, "rundir") # need a toplevel without random name so it won't affect digest
      mkdirp(tmp_rundir)
      sp.check_call(["rsync", "-rlzF"] + rsync_filter + ["./", str(Path(tmp_rundir, "tree"))])

      digest_output = sp.check_output(
        # (would prefer to use find -print0 and tar --null, but the latter doesn't seem to work)
        "tar -cf - %s | sha256sum" % tmp_rundir,
        shell=True, cwd=tmpdir)
      # with timestamp, 4 characters (32 bits) should be plenty
      digest = digest_output.decode().splitlines()[0].split()[0][:4]

      timestamp = get_timestamp()
      label = "%s_%s" % (timestamp, digest)

      # move files into place
      rundir = Path(self.runsdir, label)
      if rundir.exists():
        # wondering if this is ever happens -- seems like it should have been happening silently
        raise ValueError("label already in use", label)
      mkdirp(self.runsdir)
      shutil.move(str(tmp_rundir), str(rundir))

    run = Run(label)
    run.props.invocation = invocation
    return run

  def study_labels(self, study):
    for path in self.runsdir.glob("*/config.json"):
      config = json.loads(Path(path).read_text())
      if config.get("study", None) == study:
        yield config["label"]
    # TODO this breaks abstraction, but one glob ought to be faster than iterating over self.get_runs()?
    for path in self.runsdir.glob("*/props/study"):
      study = json.loads(path.read_text())
      if study == study:
        rundir = path.parent.parent
        if not rundir.is_symlink():
          yield rundir.name
  def designated_labels(self, labels, ignore_nonexistent=False, deduplicate=True):
    result = []
    for label in labels:
      path = Path(self.runsdir, label)
      if path.is_symlink(): # an alias
        result.append(self.resolve_alias(path.name).label)
      elif path.exists(): # assume rundir, which has label as its name
        assert path.is_dir()
        result.append(label)
      else: # assume it's a study
        # TODO think about whether we can store studies as nested runsdirs
        study_labels = self.study_labels(label)
        if not study_labels and not ignore_nonexistent:
          raise KeyError("unknown run or study", label)
        result.extend(study_labels)
    if deduplicate:
      result = dedup(result)
    return result
  def designated_runs(self, labels, ignore_nonexistent=False, deduplicate=True):
    return list(map(Run, self.designated_labels(labels)))

  def present_label(self, run):
    # NOTE alias symbolic links aren't available on remote... another reason to rethink this
    # and they couldn't possibly be made available: locally we have .detours for every project
    # but remotely we have one big detours dir
    alias = run.props.alias if not G.config.on_remote else None
    if not alias:
      return run.label

    # double-check that the alias points to the label, or else the
    # user might be misled.
    # TODO this redundancy is kinda painful to maintain. can we do better?
    # should get_alias just figure out the alias from links pointing to it?
    # that seems like a lot of work...
    assert self.resolve_alias(alias) == run

    return "%s (%s)" % (run.label, alias)

  def get_total_size(self, runs):
    entries = sp.check_output("du -chs".split() + [run.rundir for run in runs]).decode("utf-8").splitlines()
    # last line is "<size> total"
    return entries[-1].split()[0]

  def autoalias(self, run, force=False):
    alias = run.props.alias
    if alias and not force:
      return alias

    # a small set of mnemonics to bind to the most recent runs.
    aliases = """alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima
                 mike november oscar papa quebec romeo sierra tango uniform victor whiskey
                 xray yankee zulu""".split()
    # figure out which alias was least recently bound, and rebind it.
    def recency(alias):
      path = Path(self.runsdir, alias)
      # using `is_symlink` because `exists` returns False on broken symlinks
      return os.stat(str(path), follow_symlinks=False).st_ctime if path.is_symlink() else 0
    aliases = sorted(aliases, key=recency)
    alias = aliases[0]
    self.bind_alias(alias, run)
    return alias

  # an alias is maintained in two parts: a symlink in the runsdir, and a file
  # in the rundir containing the alias name in order to be able to go the other
  # direction.
  def bind_alias(self, alias, run):
    alias_path = Path(self.runsdir, alias)
    if alias_path.exists():
      if not alias_path.is_symlink():
        raise TypeError("alias path occupied by non-symlink", alias_path)
      self.unbind_alias(alias)
    alias_path.symlink_to(run.label)
    run.props.alias = alias
  def unbind_alias(self, alias):
    run = self.resolve_alias(alias)
    del run.props.alias
    Path(self.runsdir, alias).unlink()
  def resolve_alias(self, alias):
    alias_path = Path(self.runsdir, alias)
    label = alias_path.resolve().name # TODO fragile?
    run = Run(label)
    if run.props.alias != alias:
      raise RuntimeError("alias inconsistency: %s is symlinked to %s, which points back to %s"
                         % (alias, alias_path.resolve(), run.props.alias))
    return run


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

def squeue(jobids, fields="state reason timeused timeleft"):
  fields = wordlist(fields)
  try:
    blob = sp.check_output(["squeue",
                            "-O", ",".join(["jobid"] + fields),
                            "-j", ",".join(jobids)],
                           stderr=sp.STDOUT)
    error = 0
    # example output: (for default fields)
    # JOBID               STATE               REASON              TIME                TIME_LEFT
    # 11922138            PENDING             Priority            0:00                1-00:00:00
    # 11922147            PENDING             Priority            0:00                1-00:00:00
    # 11922157            PENDING             Priority            0:00                1-00:00:00
    for line in blob.splitlines()[1:]:
      line = line.decode("utf-8")
      jobid, *values = line.split()
      yield jobid, dict(eqzip(fields, values))
  except sp.CalledProcessError as e:
    blob = e.output
    error = e.returncode
    if "Invalid job id specified" in blob.decode("utf-8"):
      # a preliminary trial suggests this error only occurs when len(jobids) == 1, and
      # invalid job ids are ignored otherwise. we ignore this error to make it consistent.
      pass
    else:
      logging.error("something went wrong in call to squeue, dropping into pdb")
      import pdb; pdb.set_trace()

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
  encode.register(Run)(lambda x: ("r", x.label))

  @classmethod
  def decode(cls, tag, s):
    try:
      return dict(i=int, f=float, s=str, r=Run)[tag](s)
    except KeyError:
      assert tag == "x"
      return deserialize(s)

  @classmethod
  def encode_args(cls, *args, **kwargs):
    # serialize args/kwargs, human readable where possible, e.g.:
    #   sfi,sxsf arg1 arg3 arg4 key1 value1 key2 value2
    #   \      /
    #    legend indicates how to handle each arg:
    #   s for string, i for int, f for float, r for run, x for serialized
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
        if name.startswith("_"): continue
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

def mkdirp(path):
  os.makedirs(str(path), exist_ok=True)
  return path

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
    # TODO consider this opportunity to strip out RPC data from the interactive stream
    data = os.read(fd, 1024)
    output.append(data.decode("utf-8"))
    return data
  status = waitwhat(pty.spawn(command, read))
  output = "".join(output)
  if status:
    # NOTE output contains both stdout and stderr; we can't distinguish them
    raise sp.CalledProcessError(status, command, output=output, stderr=None)
  return output

def waitwhat(status):
  # pty.spawn returns whatever waitpid returns, which is
  #   a 16-bit number, whose low byte is the signal number that killed the
  #   process, and whose high byte is the exit status (if the signal number is
  #   zero); the high bit of the low byte is set if a core file was produced.
  # so pythonic!
  # taking a page from `_handle_exitstatus` deep in `subprocess.Popen` internals:
  if os.WIFSIGNALED(status): return -os.WTERMSIG(status)
  elif os.WIFEXITED(status): return os.WEXITSTATUS(status)
  elif os.WIFSTOPPED(status): return -os.WSTOPSIG(status) # this one shouldn't occur for us
  else: raise ValueError(status)

# we want serialization to be able to handle our objects; these functions handle
# flattening/reinstantiating our objects into jsonable types.

tupletypes = dict(tuple=tuple, Run=Run) # hard to do this generally and safely, so whitelist

jsonize_types = dict(
  list=(list,
        lambda xs: list(map(jsonize, xs)),
        lambda xs: list(map(dejsonize, xs))),
  # take care to reconstruct namedtuples properly
  tuple=(tuple,
         lambda xs: (type(xs).__name__, *map(jsonize, xs)),
         lambda xs: tupletypes[xs[0]](*map(dejsonize, xs[1:]))),
  # serialize dict as a list so our keys can be non-primitive
  dict=(dict,
        lambda xs: [(jsonize(k), jsonize(v)) for k, v in xs.items()],
        lambda xs: {dejsonize(k): dejsonize(v) for k, v in xs}))

def jsonize(x):
  # regenerate `singledispatch` table on every call to stay in sync with
  # `jsonize_types` if it ever changes. not too worried about any performance
  # implications.
  # also doing it here avoids polluting global namespace with loop variables.
  @ft.singledispatch
  def _jsonize(x):
    return (None, x)
  for tag, (type, encode, decode) in jsonize_types.items():
    @_jsonize.register(type)
    def _(x, tag=tag, encode=encode):
      return (tag, encode(x))
  return _jsonize(x)
def dejsonize(x):
  tag, payload = x
  if tag in jsonize_types:
    type, encode, decode = jsonize_types[tag]
    return decode(payload)
  else:
    assert tag is None
    return payload

def serialize(x): return base64.urlsafe_b64encode(zlib.compress(json.dumps(jsonize(x)).encode("utf-8"))).decode("utf-8")
def deserialize(x): return dejsonize(json.loads(zlib.decompress(base64.urlsafe_b64decode(x)).decode("utf-8")))

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

def wordlist(s):
  return s.split() if isinstance(s, str) else s # else assume already split

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

def rsync(sources, destination, ssh_wrapper=()):
  for subset in segments(sources, 100):
    for i in range(5):
      try:
        sp.check_call(list(ssh_wrapper) + ["rsync", "-urltvz", "--ignore-missing-args"] +
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

NODEFAULT = object()
class Props:
  def __init__(self, path):
    self.__dict__["_path"] = Path(path)
  def Set(self, key, value):
    Path(mkdirp(self._path), key).write_text(json.dumps(value))
  def Get(self, key, default=None):
    # auto migrate legacy
    if key in "alias terminated job_id invocation".split():
      filename = dict(invocation="invocation.json").get(key, key)
      path = Path(self._path.parent, filename)
      try: value = path.read_text()
      except FileNotFoundError: pass
      else:
        value = dict(invocation=json.loads).get(key, lambda s: s)(value)
        self[key] = value
        path.unlink()
    if key in "label study remote".split():
      path = Path(self._path.parent, "config.json")
      try:
        config = json.loads(path.read_text())
      except FileNotFoundError: pass
      else:
        for key_, value in config.items():
          self[key_] = value
        path.unlink()

    try: return json.loads(Path(self._path, key).read_text())
    except FileNotFoundError:
      if default is NODEFAULT: raise
      else: return default
  def Del(self, key):
    try: return Path(self._path, key).unlink()
    except FileNotFoundError: pass

  __getattr__, __setattr__, __delattr__ = Get, Set, Del
  __getitem__, __setitem__, __delitem__ = Get, Set, Del

  def __iter__(self):
    # this yields pairs, unlike `dict` which yields keys
    for path in self._path.glob("*"):
      key = path.name
      yield key, self[key]

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

