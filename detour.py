#!/usr/bin/python
import argparse, datetime, json, logging, os, shutil, sys, tempfile
import subprocess as sp

# NOTE: don't do static flags like this; remote detour installation will not
# have your changes
run_locally = True

logger = logging.Logger("detour")
logger.setLevel(logging.INFO)

rclone = "/home/tim/go/bin/rclone"
rclone_remote = "experiments"

work_root = "/data/milatmp1/cooijmat/detour/runs"
work_root = "/home/tim/dev/redblack/run"

def get_rundir(label):
  return os.path.join(work_root, label)

def push(label):
  sp.run([rclone, "copy", label, "%s:runs/%s" % (rclone_remote, label)], check=True)

def pull(label):
  sp.run([rclone, "copy",
          "%s:runs/%s" % (rclone_remote, label),
          get_rundir(label)],
         check=True)

class Main(object):
  pass

class Prepare(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("invocation", nargs="*")

    args = parser.parse_args(argv)

    # ask for experiment notes if flag

    # using absolute path avoids a lot of weirdness
    # FIXME hmmmppff what if the main binary is python? :-(
    invocation = args.invocation
    invocation[0] = os.path.realpath(invocation[0])

    # file dependencies are considered to be everything in the same directory as main binary
    sourcetree = os.path.dirname(invocation[0])
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    assert logger.isEnabledFor(logging.INFO) # still no output -_-
    logger.warn("sourcetree: %s", sourcetree)
    logger.warn("invocation: %r", invocation)
    logger.warn("timestamp: %r", timestamp)

    stage = tempfile.mkdtemp()

    source_zip_path = os.path.join(stage, "source.zip")
    sp.run(["zip", "-r", source_zip_path, sourcetree, "--exclude", "*.git*"], check=True)

    # zip recreates the entire path leading up to the main binary inside the
    # archive; to invoke it we will want a relative path into the archive's
    # structure.
    invocation[0] = invocation[0].lstrip("/")

    invocation_path = os.path.join(stage, "invocation.json")
    with open(invocation_path, "w") as invocation_file:
      json.dump(invocation, invocation_file)

    # make sure we're not inadvertently uploading big things
    assert os.path.getsize(source_zip_path) < 1000000 # bytes

    # generate a name based on timestamp, invocation and hp/code hash
    # (shortened to 128 bits or 32 hex characters to fit in screen's session name limit)
    sourcehash = (sp.run(["sha256sum", source_zip_path],
                         stdout=sp.PIPE, check=True)
                  .stdout.splitlines()[0].split()[0].decode())[:32]
    label = "%s_%s" % (timestamp, sourcehash)
    logger.warn("label: %r", label)

    shutil.move(stage, label)

    # upload to drive
    push(label)

    # jump through ssh hoops to launch job
    if run_locally:
      sp.run(["detour", "dealwithscreen", label], check=True)
    else:
      sp.run(["ssh", "elisa3", "detour", "dealwithscreen", label])

    # fork above then watch folder if flag?
    # or just sync at this point?
    pass

class DealWithScreen(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")

    args = parser.parse_args(argv)

    rundir = get_rundir(label)
    os.makedirs(rundir)
    nextstage = "run" if run_locally else "dealwithslurm"
    screenlabel = "detour_%s" % args.label
    sp.run(["pkscreen", "-S", screenlabel,
            "sh", "-c",
            "detour %s %s; exec bash" % (nextstage, args.label)],
           cwd=rundir, check=True)
    # TODO: in separate screen window (?) watch rundir and sync changes
    sp.run("screen -x %s" % screenlabel,
           cwd=rundir, check=True, shell=True)

class DealWithSlurm(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")

    args = parser.parse_args(argv)

    sp.run(["sinter", "--gres=gpu", "-Cgpu12gb", "--qos=high", "-t", "60", "--exclude=mila01", "--",
            "detour", "run", args.label], check=True)

class Run(Main):
  @staticmethod
  def __call__(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("label")

    args = parser.parse_args(argv)

    # download from drive
    pull(args.label)
    rundir = get_rundir(args.label)

    sp.run("unzip source.zip -d source".split(), check=True, cwd=rundir)
  
    with open(os.path.join(rundir, "invocation.json")) as invocation_file:
      invocation = json.load(invocation_file)
  
    # TODO: capture stdout/stderr without breaking interactivity
    sp.run(invocation, check=True, cwd=os.path.join(rundir, "source"))

def main(argv):
  # usage: detour <command> [command args...]
  return dict(prepare=Prepare,
              dealwithscreen=DealWithScreen,
              dealwithslurm=DealWithSlurm,
              run=Run)[argv[1]]()(argv[2:])

if __name__ == "__main__":
  main(sys.argv)
