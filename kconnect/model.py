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

    def __init__(self, seed_dispenser, accessor, input_ports=None, output_ports=None):
        if input_ports is None:
            input_ports = []
        if output_ports is None:
            output_ports = []

        self._seed_dispenser = seed_dispenser
        self._input_ports = {}
        self._output_ports = {}
        self._accessor = accessor

        for ip in input_ports:
            assert ip.name not in self._input_ports
            self._input_ports[ip.name] = ip

        for op in output_ports:
            assert op.name not in self._output_ports
            self._output_ports[op.name] = op

    def get_seed(self, nm: str) -> SeedGerminator:
        seed = self._seed_dispenser.get_seed(nm)
        return SeedGerminator(seed, self._input_ports)

    def add_input_port(self, in_port):
        assert in_port not in self._input_ports
        self._input_ports[in_port.name] = in_port

    def get_input_port(self, nm):
        try:
            return self._input_ports[nm]
        except:
            msg = 'No port named "%s" in available list: %s' % (nm, ', '.join(self.list_input_ports()))
            raise AttributeError(msg)

    def get(self, nm, datastore):
        data = self._accessor(datastore)
        return self._output_ports[nm](data)

    def list_output_ports(self):
        return self._output_ports.keys()

    def list_input_ports(self):
        return self._input_ports.keys()

    def get_output_port(self, nm):
        try:
            return self._output_ports[nm]
        except:
            msg = 'No port named "%s" in available list: %s' % (nm, ', '.join(self.list_output_ports()))
            raise AttributeError(msg)


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
        self._pull_dataflows = defaultdict(list)

    def add_subsystem(self, name, subsys):
        assert name not in self._subsystems
        self._subsystems[name] = subsys

    def get_subsystem(self, name):
        return self._subsystems[name]

    def get_pull_dataflows(self, nm):
        return self._pull_dataflows[nm]

    def connect(self, src, dst, via=None):
        src_subsys, src_port_nm = src.split('.')
        dst_subsys, dst_port_nm = dst.split('.')
        if src_subsys not in self._subsystems:
            raise AttributeError("Model doesn't contain subsystem: %s" % src_subsys)
        if dst_subsys not in self._subsystems:
            raise AttributeError("Model doesn't contain subsystem: %s" % dst_subsys)

        _src_port = self._subsystems[src_subsys].get_output_port(src_port_nm)
        _dst_port = self._subsystems[dst_subsys].get_input_port(dst_port_nm)

        # TODO: a 4-tuple for this is just plain dumb. work something out as
        # soon as you get the tests to pass!
        self._pull_dataflows[dst_subsys].append((src_subsys, src_port_nm, dst_subsys, dst_port_nm))

    def configure(self, model_config):
        return RunnableModel(self, model_config)

    def get(self, datastore, subsys_name, getter_name):
        subsys = self.get_subsystem(subsys_name)
        data_container = datastore[subsys_name][-1]
        return subsys.get(getter_name, data_container)

class RunnableModel(object):
    """
    Once a Model has been configured with baseline selections it becomes 'runnable'
    """
    def __init__(self, model, model_config):
        self._model = model
        self._model_config = model_config

    def run(self, name, datastore):
        subsys = self._model.get_subsystem(name)
        seed_selection = self._model_config[name]

        seed = subsys.get_seed(seed_selection)

        pull_dataflows = self._model.get_pull_dataflows(name)
        for src_subsys_nm, src_port_nm, dst_subsys_nm, dst_port_nm in pull_dataflows:
            assert dst_subsys_nm == name

            # UBC default policy: ignore and move on
            if src_subsys_nm not in datastore or not datastore[src_subsys_nm]:
                continue

            src_subsys = self._model.get_subsystem(src_subsys_nm)

            src_data_container = datastore[src_subsys_nm][-1]  # pick the latest
            cargo = src_subsys.get(src_port_nm, src_data_container)
            seed.apply(dst_port_nm, cargo)

        data_container = datastore[name].add_new()
        seed.run(data_container)

class BadPortSpec(AttributeError):
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

    def __contains__(self, nm):
        return nm in self._datastacks

    def __repr__(self):
        return repr(dict(self._datastacks))


class DataStack(object):
    def __init__(self):
        self._stack = []

    def add_new(self):
        dc = DataContainer()
        self._stack.append(dc)
        return dc

    def __getitem__(self, idx):
        return self._stack[idx]

    def __repr__(self):
        return repr(self._stack)

    def __nonzero__(self):
        return len(self._stack)

    def __bool__(self):
        return bool(self._stack)


class DataContainer(object):
    def __init__(self):
        self._data = {}

    def __getitem__(self, item):
        def write_fn(d):
            self._data[item] = d

        def read_fn():
            return self._data[item]

        return DataItem(read_fn, write_fn)

    def __repr__(self):
        return repr(self._data)


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
