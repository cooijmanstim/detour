#!/usr/bin/env python3
import argparse, datetime, json, logging, os, shutil, signal, sys, tempfile, time
import subprocess as sp

run_locally = False

logger = logging.Logger("detour")
logger.setLevel(logging.INFO)

rclone = os.path.join(os.environ["HOME"], "bin", "rclone")
rclone_remote = "experiments"

if run_locally:
  work_root = "/home/tim/detours"
else:
  work_root = "/data/milatmp1/cooijmat/detours"

def get_rundir(label):
  return os.path.join(work_root, label)

def push(label, source_path):
  target_path = "%s:runs/%s" % (rclone_remote, label)
  sp.check_call([rclone, "copy", source_path, target_path])

def pull(label, target_path):
  source_path = "%s:runs/%s" % (rclone_remote, label)
  sp.check_call([rclone, "copy", source_path, target_path])

# -_______________-
def activate_environment(name, env):
  env["CONDA_DEFAULT_ENV"] = "py36"
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

    # using absolute path avoids a lot of weirdness
    invocation = args.invocation
    invocation[0] = os.path.realpath(invocation[0])

    # file dependencies are considered to be everything in the same directory as main binary
    # FIXME hmmmppff what if the main binary is python? :-(
    tree_path = os.path.dirname(invocation[0])
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    assert logger.isEnabledFor(logging.INFO) # still no output -_-
    logger.warn("tree_path: %s", tree_path)
    logger.warn("invocation: %r", invocation)
    logger.warn("timestamp: %r", timestamp)

    stage = tempfile.mkdtemp()

    tree_zip_path = os.path.join(stage, "tree.zip")
    sp.check_call(["zip", "-r", tree_zip_path, tree_path,
                   "--exclude", ".git/*",
                   "--exclude", "*/.git/*",
                   "--exclude", "detours/*",
                   "--exclude", "*/detours/*"])

    # zip recreates the entire path leading up to the main binary inside the
    # archive; to invoke it we will want a relative path into the archive's
    # structure.
    invocation[0] = invocation[0].lstrip("/")

    invocation_path = os.path.join(stage, "invocation.json")
    with open(invocation_path, "w") as invocation_file:
      json.dump(invocation, invocation_file)

    # make sure we're not inadvertently uploading big things
    assert os.path.getsize(tree_zip_path) < 1000000 # bytes

    # generate a name based on timestamp, invocation and hp/code hash
    # (shortened to 128 bits or 32 hex characters to fit in screen's session name limit)
    treehash = (sp.check_output(["sha256sum", tree_zip_path]).decode()
                  .splitlines()[0].split()[0])[:32]
    label = "%s_%s" % (timestamp, treehash)
    logger.warn("label: %r", label)

    os.makedirs("detours", exist_ok=True)
    workdir = os.path.join("detours", label)
    shutil.move(stage, workdir)
    # TODO: link "latest", although that doesn't work well if we're running multiple in parallel

    # upload to drive
    # TODO: push in a separate process, then make pull blocking.
    push(label, workdir)

    childpid = os.fork()
    if childpid == 0:
      while True:
        time.sleep(30)
        # sync periodically
        pull(label, workdir)
        sp.check_call("unzip tree.zip", cwd=workdir)
    else:
      # jump through ssh hoops to launch job
      if run_locally:
        sp.check_call(["detour", "dealwithscreen", label])
      else:
        rundir = get_rundir(label)
        sp.check_call(["scp", os.path.realpath(sys.argv[0]), "elisa3:bin/detour"])
        sp.check_call(["ssh", "-t", "elisa3", "detour", "dealwithscreen", label])

      os.kill(childpid, signal.SIGTERM)
      os.waitpid(childpid, 0)

    # fork above then watch folder if flag?
    # or just sync at this point?
    # for live plotting might use google sheets anyway
    pull(label, workdir)
    sp.check_call("unzip tree.zip", cwd=workdir)

class DealWithScreen(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")

    args = parser.parse_args(argv)

    rundir = get_rundir(args.label)
    os.makedirs(rundir)
    assert os.path.exists(rundir)
    nextstage = "run" if run_locally else "dealwithslurm"
    screenlabel = "detour_%s" % args.label
    sp.check_call(["pkscreen", "-S", screenlabel,
                   "sh", "-c",
                   "detour %s %s; exec bash" % (nextstage, args.label)],
                  cwd=rundir)
    # TODO: in separate screen window (?) watch rundir and sync changes
    # FIXME "sync"ing will have to mean zipping up the tree and copying it over,
    # so rclone can't actually avoid copying things that didn't change. no way
    # to do this efficiently without having lots of files (i.e. avoiding zip)
    # or having an archive+drive aware rclone backend.
    sp.check_call(["screen", "-x", screenlabel],
                  cwd=rundir, env=dict(TERM="xterm-color"))

class DealWithSlurm(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")

    args = parser.parse_args(argv)

    sp.check_call(["sinter", "--gres=gpu", "-Cgpu12gb", "--qos=high", "-t", "60",
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

    # download from drive
    pull(args.label, rundir)

    sp.check_call("unzip tree.zip -d tree".split(), cwd=rundir)
  
    with open(os.path.join(rundir, "invocation.json")) as invocation_file:
      invocation = json.load(invocation_file)

    # TODO: capture stdout/stderr without breaking interactivity
    # FIXME we are not running the command in the same cwd. forget about this absolute path stuff because it couldn't possibly work
    sp.check_call(invocation, cwd=os.path.join(rundir, "tree"))

    # update the tree.zip to reflect changes made by the program
    # excluding large files (eg. checkpoints)
    sp.check_call("find * -type f -not -size +100M | zip ../tree.zip --names-stdin",
                  cwd=os.path.join(rundir, "tree"), shell=True)

    push(args.label, os.path.join(rundir, "tree.zip"))

def main(argv):
  # usage: detour <command> [command args...]
  #logger.warn("argv %r", argv)
  return dict(launch=Launch,
              dealwithscreen=DealWithScreen,
              dealwithslurm=DealWithSlurm,
              dealwithconda=DealWithConda,
              run=Run)[argv[1]]()(argv[2:])

if __name__ == "__main__":
  main(sys.argv)
