import datetime, os, time
from collections import defaultdict as ddict, OrderedDict as ordict, deque
import numpy as np
from holster import H

def is_sequence(x):
  if isinstance(x, str):
    return False
  try:
    iter(x)
  except TypeError:
    return False
  else:
    return True

def eqzip(*xss):
  xss = list(map(list, xss))
  if not all(len(xs) == len(xss[0]) for xs in xss):
    raise ValueError("eqzip on variable-length sequences")
  return zip(*xss)

# decorator to take as first argument an iterable of key/value pairs or a dict-like object
# (including Holster), and/or a dict through kwargs
def updatelike(fn):
  def wrapped_fn(items=(), **kwargs):
    if hasattr(items, "keys"): # so pythonic
      items = list(items.items())
    if isinstance(items, holster.BaseHolster):
      items = list(items.Items())
    items = list(items)
    items.extend(kwargs.items())
    return fn(items)
  return wrapped_fn

class Experiment(object):
  def __init__(self, label=None):
    self.label = label or os.environ["DETOUR_LABEL"]
    self.trackers = H()
    self.results = set()
    self.backends = H()
    self.clocks = H()
    self.clocks.numerical_walltime = NumericalWallClock(self)
    self.clocks.walltime = WallClock(self)

    # TODO: schedule self.flush?

  def __enter__(self):
    return self

  def __exit__(self, *args, **kwargs):
    self.flush()

  def hp(self, **kwargs):
    # convenience method to declare a hyperparameter
    (key, value), = kwargs.items()
    if key not in self.trackers:
      self.declare(key, operation="constant", backends="numpy sheets")
    return self.trackers[key](value)

  @updatelike
  def hps(self, items):
    for key, value in items:
      self.hp({key: value})

  def declare(self, key, **kwargs):
    # declare a quantity to be tracked
    tracker = Tracker(self, key, **kwargs)
    self.trackers[key] = tracker

  def register_result(self, path):
    # take note of an output file, possibly fix up the path e.g. prefix it with a base path
    self.results.add(path)
    return path

  def clock(self, name, reset=True):
    # get or create a clock, e.g. to count steps
    if reset or name not in self.clocks:
      self.clocks[name] = Clock(self)
    return self.clocks[name]

  def get_backend(self, key):
    # get or create a backend
    if key not in self.backends:
      self.backends[key] = BaseBackend.make(key, self)
    return self.backends[key]

  def flush(self):
    # flush data to backends
    for backend in self.backends.Values():
      backend.flush()

class BaseClock(object):
  def __init__(self, experiment):
    self.experiment = experiment

class Clock(BaseClock):
  def __init__(self, experiment):
    super().__init__(experiment)
    self.time = 0

  def tick(self):
    self.time += 1

  def __call__(self):
    self.tick()

  def __iter__(self):
    return self

  def __next__(self):
    self.time += 1
    return self.time

# special clocks
class NumericalWallClock(BaseClock):
  @property
  def time(self):
    return time.time()

class WallClock(BaseClock):
  @property
  def time(self):
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

class Tracker(object):
  def __init__(self, experiment, key, aggregate=None, operation=None, backends=()):
    self.experiment = experiment
    self.key = key
    self.aggregate = aggregate or IdentityAggregate()
    self.operation = operation or "replace"
    self.backends = backends.split() if isinstance(backends, str) else list(backends)

  def __call__(self, x):
    y = self.aggregate(x)
    for backend in self.backends:
      backend = self.experiment.get_backend(backend)
      operation = getattr(backend, self.operation)
      operation(self.key, y)
    return y

class BaseBackend(object):
  def constant(self, x): raise NotImplementedError()
  def replace(self, x): raise NotImplementedError()
  def append(self, x): raise NotImplementedError()
  # TODO do clock updates between appends due to extend?
  def extend(self, x): raise NotImplementedError()

  @staticmethod
  def make(key, *args, **kwargs):
    return dict(
      numpy=Numpy,
      sheets=Sheets)[key](*args, **kwargs)

class Numpy(BaseBackend):
  def __init__(self, experiment):
    super().__init__()
    self.experiment = experiment
    self.data = H()
    self.time = ddict(H)
    self.path = self.experiment.register_result("aggregates.npz")

  def constant(self, key, value):
    if key in self.data:
      if value != self.data[key]:
        raise ValueError("attempt to change constant %s from %s to %s"
                         % (key, self.data[value], value))
    else:
      self.data[key] = value
      for name, clock in self.experiment.clocks.Items():
        self.time[name][key] = clock.time

  def replace(self, key, value):
    self.data[key] = value
    for name, clock in self.experiment.clocks.Items():
      self.time[name][key] = clock.time

  def append(self, key, value):
    if key not in self.data:
      self.data[key] = []
    self.data[key].append(value)
    for name, clock in self.experiment.clocks.Items():
      if key not in self.time[name]:
        self.time[name][key] = []
      self.time[name][key].append(clock.time)

  def extend(self, key, value):
    if key not in self.data:
      self.data[key] = []
      self.time[key] = []
    value = list(value)
    self.data[key].extend(value)
    for name, clock in self.experiment.clocks.Items():
      self.time[name][key].extend(len(value) * clock.time)

  def flush(self):
    dikt = dict(self.data.AsDict())
    dikt.update(("%s__clock_%s" % (key, name), value)
                for name, things in self.time.items()
                for key, value in things.Items())
    np.savez_compressed(self.path, **dikt)

class Sheets(BaseBackend):
  def __init__(self, experiment):
    super().__init__()
    self.experiment = experiment

    from oauth2client.service_account import ServiceAccountCredentials
    import gspread

    key_path = os.path.join(os.environ["HOME"], ".experiment_drivekey.json")
    scopes = ["https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
      key_path, scopes=scopes)

    self.client = gspread.authorize(credentials)
    self.sheet = self.client.create(self.experiment.label)

    self.sheet.share("cooijmans.tim@gmail.com", perm_type="user", role="writer")

    # NOTE: sheet 1 is reserved for meta: hyperparameters (linked from the general aggregates) and
    # detour status.
    # TODO do this linking

    self.worksheets = dict()
    self.offsets = ddict(lambda: 2) # row 1 is header
    self.widths = dict() # for vector quantities we require fixed width

  def get_worksheet(self, key):
    try:
      return self.worksheets[key]
    except KeyError:
      self.worksheets[key] = Worksheet(self.sheet, key)
      return self.worksheets[key]

  def get_offset(self, key):
    return self.offsets[key], 1

  def validate_width(self, key, width):
    if key not in self.widths:
      self.widths[key] = width
      self.install_header(key)
    elif self.widths[key] != width:
      # FIXME turn this into a warning; it only mangles the sheet layout
      raise ValueError("dimensionality of %s changed from %s to %s" % (key, self.widths[key], width))

  def install_header(self, key):
    data_headers = self.widths[key] * [key]
    clock_headers = list(self.experiment.clocks.Keys())
    wks = self.get_worksheet(key)
    wks.put(1, 1, [data_headers + clock_headers])

  def get_timestamp(self):
    return [clock.time for clock in self.experiment.clocks.Values()]

  def constant(self, key, value):
    if isinstance(value, np.ndarray):
      assert value.ndim <= 2

    # turn into list of lists
    xs = list(value) if is_sequence(value) else [value]
    xss = [list(x) if is_sequence(x) else [x] for x in xs]

    for xs in xss:
      self.validate_width(key, len(xs))

    wks = self.get_worksheet(key)
    i, j = self.get_offset(key)
    yss = wks.get(i, j, len(xss), len(xss[0]))

    if all(y == "" for ys in yss for y in ys):
      ts = self.get_timestamp()
      wks.put(i, j, [xs + ts for xs in xss])
    else:
      if not np.arrays_equal(xss, yss):
        raise ValueError("attempt to change constant %s from %s to %s"
                         % (key, yss, xss))

  def replace(self, key, value):
    if isinstance(value, np.ndarray):
      assert value.ndim <= 2

    # turn into list of lists
    xs = list(value) if is_sequence(value) else [value]
    xss = [list(x) if is_sequence(x) else [x] for x in xs]

    for xs in xss:
      self.validate_width(key, len(xs))

    ts = self.get_timestamp()
    i, j = self.get_offset(key)
    wks.put(i, j, [xs + ts for xs in xss])

  def append(self, key, value):
    if isinstance(value, np.ndarray):
      assert value.ndim <= 1

    xs = list(value) if is_sequence(value) else [value]
    self.validate_width(key, len(xs))
    ts = self.get_timestamp()
    wks = self.get_worksheet(key)
    i, j = self.get_offset(key)
    wks.put(i, j, [xs + ts])
    self.offsets[key] += 1

  def extend(self, key, value):
    if isinstance(value, np.ndarray):
      assert value.ndim <= 2

    assert is_sequence(value)
    ts = self.get_timestamp()
    xss = list(map(list, xss))
    for xs in xss:
      self.validate_width(key, len(xs))
    wks = self.get_worksheet(key)
    i, j = self.get_offset(key)
    wks.put(i, j, [xs + ts for xs in xss])
    self.offsets[key] += len(xss)

  def flush(self):
    for key, worksheet in self.worksheets.items():
      worksheet.flush()


class Worksheet(object):
  def __init__(self, sheet, key):
    self.sheet = sheet
    self.key = key
    self.num_rows = 1
    self.num_cols = 1
    self.worksheet = self.sheet.add_worksheet(
      title=self.key, rows=self.num_rows, cols=self.num_cols)
    self.backlog = []

  def range(self, top, left, bottom, right):
    grow = False
    while bottom > self.num_rows:
      self.num_rows *= 2
      grow = True
    while right > self.num_cols:
      self.num_cols *= 2
      grow = True
    if grow:
      self.worksheet.resize(rows=self.num_rows, cols=self.num_cols)
    cs = self.worksheet.range(top, left, bottom, right)
    # turn it into a list of lists...
    m, n = bottom - top + 1, right - left + 1
    css = [cs[i * n:(i + 1) * n] for i in range(m)]
    return css

  def put(self, i, j, xss):
    # put a list of lists at topleft i, j
    xss = list(map(list, xss))
    m = len(xss)
    for xs in xss:
      n = len(xs)
      cs, = self.range(i, j, i, j + n - 1)
      for c, x in eqzip(cs, xs):
        c.value = x
      i += 1
      self.backlog.extend(cs)

  def get(self, i, j, m, n):
    css = self.range(i, j, i + m - 1, j + n - 1)
    return [[c.value for c in cs] for cs in css]

  def flush(self):
    backlog, self.backlog = self.backlog, []
    if  backlog:
      self.worksheet.update_cells(backlog)

class BaseAggregate(object):
  def __call__(self, x):
    self.process(x)
    return self.value

class IdentityAggregate(BaseAggregate):
  def process(self, x):
    self.value = x

class MedianAggregate(BaseAggregate):
  def __init__(self, k=5):
    self.history = deque(maxlen=k)

  def process(self, x):
    self.history.append(x)

  @property
  def value(self):
    return np.median(self.history)

if __name__ == "__main__":
  with Experiment("TEST") as E:
    x = np.random.rand()
    # rnn with noise on input
    T = E.hp(T=100)
    k = E.hp(k=5)
    h = np.zeros((k,))
    v = np.random.rand(k, k)
    w = np.random.rand(k, k)
    b = np.zeros((k,))

    E.declare("h", operation="append", backends="numpy sheets")
    E.declare("loss", operation="append", backends="numpy sheets")
    E.declare("median_loss",
              aggregate=MedianAggregate(),
              operation="append",
              backends="numpy sheets")

    for t in E.clock("t"):
      x = np.random.rand(k)
      h = np.dot(h, w) + np.dot(x, v) + b
      loss = np.sum(h**2)
      E.trackers.h(h)
      E.trackers.loss(loss)
      print("t", t, "loss", loss)
      E.flush()
      time.sleep(1)
