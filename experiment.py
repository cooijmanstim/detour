import os, time
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
  # TODO flush regularly and at the end
  # TODO keep time

# TODO think about how to track time? are we doomed to have one single tracker call every step?
# what about time in the operation=extend case?
# idea: have Experiment track walltime and also user-defined (through a tick() method or a context manager?) notions of time
# how does this help the operation=extend case?
  def __init__(self, label=None):
    self.label = label or os.environ["DETOUR_LABEL"]
    self.trackers = H()
    self.results = set()
    self.backends = dict()

  def __enter__(self):
    return self

  def __exit__(self, *args, **kwargs):
    pass

  def hp(self, **kwargs):
    (key, value), = kwargs.items()
    if key not in self.trackers:
      self.declare(key, operation="constant", backends="numpy sheets")
    return self.trackers[key](value)

  @updatelike
  def hps(self, items):
    for key, value in items:
      self.hp({key: value})

  def declare(self, key, **kwargs):
    tracker = Tracker(self, key, **kwargs)
    self.trackers[key] = tracker

  def register_result(self, path):
    self.results.add(path)
    return path

  def get_backend(self, key):
    if key not in self.backends:
      self.backends[key] = BaseBackend.make(key, self)
    return self.backends[key]

  def flush_backends(self):
    for backend in self.backends:
      backend.flush()

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
    self.data = dict()
    self.path = self.experiment.register_result("aggregates.npz")

  def constant(self, key, value):
    if key in self.data:
      if value != self.data[value]:
        raise ValueError("attempt to change constant")
    else:
      self.data[key] = value

  def replace(self, key, value):
    self.data[key] = value

  def append(self, key, value):
    if key not in self.data:
      self.data[key] = []
    self.data[key].append(value)

  def extend(self, key, value):
    if key not in self.data:
      self.data[key] = []
    self.data[key].extend(value)

  def flush(self):
    np.savez_compressed(self.path, **self.data)

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

    self.worksheets = dict()
    self.offsets = ddict(lambda: 4) # rows 1, 2, 3 are reserved

  def get_worksheet(self, key):
    try:
      return self.worksheets[key]
    except KeyError:
      self.worksheets[key] = self.sheet.add_worksheet(title=key, rows=1000, cols=10)
      return self.worksheets[key]

  def get_offset(self, key):
    return self.offsets[key], 2

  def constant(self, key, value):
    if isinstance(value, np.ndarray):
      assert value.ndim <= 1

    wks = self.get_worksheet(key)
    i, j = self.get_offset(key)
    xs = list(value) if is_sequence(value) else [value]
    dim = len(xs)
    cells = wks.range(i, j, i, j + dim - 1)
    existing_xs = [cell.value for cell in cells]
    if any(existing_x != x
           for existing_x, x in eqzip(existing_xs, xs)
           if existing_x != ""):
      raise ValueError("attempt to change constant %s from %s to %s" % (key, existing_xs, xs))
    for cell, x in zip(cells, xs):
      cell.value = x
    wks.update_cells(cells)

  def replace(self, key, value):
    if isinstance(value, np.ndarray):
      assert value.ndim <= 1

    wks = self.get_worksheet(key)
    i, j = self.get_offset(key)
    xs = list(value) if is_sequence(value) else [value]
    dim = len(xs)
    cells = wks.range(i, j, i, j + dim - 1)
    for cell, x in zip(cells, xs):
      cell.value = x
    wks.update_cells(cells)

  def append(self, key, value):
    self.replace(key, value)
    self.offsets[key] += 1

  def extend(self, key, value):
    wks = self.get_worksheet(key)
    if isinstance(value, np.ndarray):
      assert value.ndim <= 2
    assert is_sequence(value)
    cells = []
    for di, xs in enumerate(value):
      i, j = self.get_offset(key)
      xs = list(xs) if is_sequence(xs) else [xs]
      dim = len(xs)
      row = wks.range(i + di, j, i + di, j + dim - 1)
      for cell, x in zip(row, xs):
        cell.value = x
      cells.extend(row)
      self.offsets[key] += 1
    wks.update_cells(cells)

  def flush(self):
    pass


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
    E.declare("loss", operation="append", backends="numpy sheets".split())
    E.declare("median_loss",
              aggregate=MedianAggregate(),
              operation="append",
              backends="numpy sheets".split())

    # TODO: for t in E.clock("t"):
    for t in range(T):
      x = np.random.rand(k)
      h = np.dot(h, w) + np.dot(x, v) + b
      loss = np.sum(h**2)
      E.trackers.h(h)
      E.trackers.loss(loss)
      print("t", t, "loss", loss)
      time.sleep(1)
