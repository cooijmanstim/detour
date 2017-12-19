#!/usr/bin/env python3
import argparse, datetime, json, logging, os, shutil, signal, sys, tempfile, time
import subprocess as sp

DETOURS = ".detours"

run_locally = False

# job dependencies are considered to be everything in the current directory, except
# hidden files and __pycache__
# TODO and large files
find_filter = "-not -path */\.* -and -not -path */__pycache__/*".strip().split()
rsync_filter = "--exclude .* --exclude __pycache__".split()

logger = logging.Logger("detour")
logger.setLevel(logging.INFO)

if run_locally:
  work_root = "/home/tim/detours"
  ssh_path_prefix = ""
else:
  work_root = "/data/milatmp1/cooijmat/detours"
  ssh_path_prefix = "elisa3:"

def get_rundir(label):
  return os.path.join(work_root, label)

def get_screenlabel(label):
  return "detour_%s" % label

# -_______________-
def activate_environment(name, env):
  env["CONDA_DEFAULT_ENV"] = name
  env["CONDA_PATH_BACKUP"] = env["PATH"]
  env["CONDA_PREFIX"] = os.path.join(env["HOME"], ".conda", "envs", name)
  env["PATH"] = "%s:%s" % (os.path.join(env["CONDA_PREFIX"], "bin"), env["PATH"])
  return env

class Main(object):
  pass

class Launch(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("invocation", nargs="*")

    args = parser.parse_args(argv)

    # ask for experiment notes if flag

    invocation = args.invocation
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    checksum_output = sp.check_output(
      "|".join([
        # would prefer to use find -print0 and tar --null, but the latter doesn't seem to work
        "find . -type f '%s'" % "' '".join(find_filter),
        "tar -cf - --no-recursion --files-from=- --verbatim-files-from",
        "sha256sum",
      ]),
      shell=True)
    # (shortened to 128 bits or 32 hex characters to fit in screen's session name limit)
    checksum = checksum_output.decode().splitlines()[0].split()[0][:32]

    label = "%s_%s" % (timestamp, checksum)

    logger.warn("invocation: %r", invocation)
    logger.warn("label: %r", label)

    workdir = os.path.join(DETOURS, label)
    os.makedirs(workdir, exist_ok=True)
    # TODO: how to do "latest" nicely if we're launching a bunch of things?
    # maybe take a --nickname argument to get a custom latest_<nickname> link
    sp.check_call(["ln", "-sfn", label, "latest"], cwd=DETOURS)

    invocation_path = os.path.join(workdir, "invocation.json")
    with open(invocation_path, "w") as invocation_file:
      json.dump(invocation, invocation_file)

    sp.check_call(["rsync", "-a", "."] + rsync_filter + [os.path.join(workdir, "tree")])
    sp.check_call(["find", workdir])
    import pdb; pdb.set_trace()

    sp.check_call(["detour", "push", label])

    childpid = os.fork()
    if childpid == 0:
      while True:
        time.sleep(30)
        sp.check_call(["detour", "pull", label],
                      stdin=sp.DEVNULL, stdout=sp.DEVNULL, stderr=sp.DEVNULL)
    else:
      try:
        sp.check_call(["detour", "dealwithscreen" if run_locally else "dealwithssh", label])
      finally:
        os.kill(childpid, signal.SIGTERM)
        os.waitpid(childpid, 0)
    sp.check_call(["detour", "pull", label])

class DealWithSsh(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")
    args = parser.parse_args(argv)
    rundir = get_rundir(args.label)
    sp.check_call(["pshaw", "mila", "scp", os.path.realpath(sys.argv[0]), "elisa3:bin/detour"])
    sp.check_call(["pshaw", "mila", "ssh", "-t", "elisa3", "detour", "dealwithscreen", args.label])

class DealWithScreen(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")
    args = parser.parse_args(argv)
    rundir = get_rundir(args.label)

    os.makedirs(rundir, exist_ok=True)

    nextstage = "run" if run_locally else "dealwithslurm"
    screenlabel = get_screenlabel(args.label)
    sp.check_call(["pkscreen", "-S", screenlabel], cwd=rundir)
    # `stuff` sends the given string to stdin, i.e. we run the command as if the user had typed it.
    # the benefit is that we can attach and we are in a bash prompt with exactly the same
    # environment as the program ran in (as opposed to would be the case with some other ways of
    # keeping the screen window alive after the program terminates)
    sp.check_call(["screen", "-S", screenlabel, "-p", "0", "-X", "stuff",
                   "detour %s %s^M" % (nextstage, args.label)],
                  cwd=rundir)
    sp.check_call(["screen", "-x", screenlabel],
                  env=dict(TERM="xterm-color"))

class DealWithSlurm(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")
    args = parser.parse_args(argv)
    sp.check_call(["sinter", "--gres=gpu", "-Cgpu12gb", "--qos=unkillable",
                   "--exclude=mila01",
                   "--exclude=eos13",
                   "sh", "-c", "detour dealwithconda %s" % args.label])

class DealWithConda(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")
    args = parser.parse_args(argv)
    env = dict(os.environ)
    env = activate_environment("py36", env)
    sp.check_call(["detour", "run", args.label], env=env)

class Run(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")
    args = parser.parse_args(argv)
    rundir = get_rundir(args.label)
    with open(os.path.join(rundir, "invocation.json")) as invocation_file:
      invocation = json.load(invocation_file)
    env = dict(os.environ)
    env["DETOUR_LABEL"] = args.label
    # TODO: capture stdout/stderr without breaking interactivity
    sp.check_call(invocation, cwd=os.path.join(rundir, "tree"), env=env)

class Pull(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")
    args = parser.parse_args(argv)
    localdir = os.path.join(DETOURS, args.label)
    # TODO: if no argument given, sync all dirs under .detours
    remotedir = get_rundir(args.label)
    sp.check_call(["pshaw", "mila", "rsync", "-avz"] + rsync_filter + ["%s%s/" % (ssh_path_prefix, remotedir), localdir])

class Push(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")
    args = parser.parse_args(argv)
    localdir = os.path.join(DETOURS, args.label)
    remotedir = get_rundir(args.label)
    sp.check_call(["pshaw", "mila", "rsync", "-avz"] + rsync_filter + ["%s/" % localdir, "%s%s" % (ssh_path_prefix, remotedir)])

class Attach(Main):
  @staticmethod
  def __call__(argv):
    assert not run_locally
    parser = argparse.ArgumentParser()
    parser.add_argument("label")
    args = parser.parse_args(argv)
    rundir = get_rundir(args.label)
    childpid = os.fork()
    if childpid == 0:
      while True:
        sp.check_call(["detour", "pull", args.label],
                      stdin=sp.DEVNULL, stdout=sp.DEVNULL, stderr=sp.DEVNULL)
        time.sleep(30)
    else:
      try:
        sp.check_call(["pshaw", "mila", "ssh", "-t", "elisa3", "screen", "-x", get_screenlabel(args.label)])
      finally:
        os.kill(childpid, signal.SIGTERM)
        os.waitpid(childpid, 0)
    sp.check_call(["detour", "pull", args.label])

def main(argv):
  # usage: detour <command> [command args...]
  #logger.warn("argv %r", argv)
  return dict(launch=Launch,
              dealwithssh=DealWithSsh,
              dealwithscreen=DealWithScreen,
              dealwithslurm=DealWithSlurm,
              dealwithconda=DealWithConda,
              run=Run, pull=Pull, push=Push, attach=Attach)[argv[1]]()(argv[2:])

if __name__ == "__main__":
  main(sys.argv)
