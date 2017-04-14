"""
Example of building an executable design/analysis model.
"""
from collections import defaultdict
import io
import json


class SeedGerminator(object):
    def __init__(self, seed, api):
        self._seed = seed
        self._api = api

    def apply(self, nm, data):
        input_port = self._api[nm]
        input_port.fn(self._seed, data)

    def run(self, datastore):
        self._seed.run(datastore)


class GenericSubsys(object):

    def __init__(self, seed_dispenser, input_ports=None):
        if input_ports is None:
            input_ports = []
        self._seed_dispenser = seed_dispenser
        self._input_ports = {}
        for ip in input_ports:
            assert ip.name not in self._input_ports
            self._input_ports[ip.name] = ip

    def get_seed(self, nm):
        seed = self._seed_dispenser.get_seed(nm)
        return SeedGerminator(seed, self._input_ports)

    def add_input_port(self, in_port):
        assert in_port not in self._input_ports
        self._input_ports[in_port.name] = in_port

    def get_input_port(self, nm):
        return self._input_ports[nm]


class OutputPort(object):

    def __init__(self, name, description, type, fn):
        self._name = name
        self._description = description
        self._type = type
        self._fn = fn

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return self._description

    @property
    def type(self):
        return self._type

    def __call__(self, datastore):
        return self._fn(datastore)


class InputPort(object):

    def __init__(self, name, description, type, fn):
        self._name = name
        self._description = description
        self._type = type
        self._fn = fn

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return self._description

    @property
    def type(self):
        return self._type

    @property
    def fn(self):
        return self._fn


class Model(object):
    """
    A connected graph of computational nodes and dataflows
    """

    def __init__(self):
        self._subsystems = {}

    def add_subsystem(self, name, subsys):
        assert name not in self._subsystems
        self._subsystems[name] = subsys

    def connect(self, src, dst, via=None):
        pass

    def run(self, name, datastore):
        pass


class IncompatiblePorts(TypeError):
    pass


def verify_port_compatibility(src, dst):
    if not issubclass(src.type, dst.type):
        raise IncompatiblePorts


class DataStore(object):
    def __init__(self):
        self._datastacks = defaultdict(DataStack)

    def __getitem__(self, nm):
        # print('***', nm)
        # print(self._datastacks[nm])
        return self._datastacks[nm]


class DataStack(object):
    def __init__(self):
        self._stack = []

    def add_new(self):
        dc = DataContainer()
        self._stack.append(dc)
        return dc

    def __getitem__(self, idx):
        # print('===',idx)
        return self._stack[idx]


class DataContainer(object):
    def __init__(self):
        self._data = {}

    def __getitem__(self, item):
        def write_fn(d):
            self._data[item] = d

        def read_fn():
            return self._data[item]

        return DataItem(read_fn, write_fn)


class DataItem(object):
    def __init__(self, read_fn, write_fn):
        self._read_fn = read_fn
        self._write_fn = write_fn

    def write(self, d):
        self._write_fn(d)

    def read(self):
        return self._read_fn()

    def open_to_read(self):
        s = self._read_fn()
        return io.StringIO(s)

    def open_to_write(self):
        sio = io.StringIO()

        class mycontext(object):
            def __enter__(_):
                return sio

            def __exit__(_, *exc):
                self._write_fn(sio.getvalue())

        return mycontext()

    def put_json(self, d):
        self._write_fn(json.dumps(d))

    def get_json(self):
        return json.loads(self._read_fn())
