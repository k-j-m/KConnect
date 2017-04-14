from kconnect.model import Model, InputPort, OutputPort, GenericSubsys
import math


#### Data models #####

class CompressorPerfInputs(object):
    def __init__(self, pr, flow):
        self.pr = pr
        self.flow = flow

class DuctInputs(object): ...


#### Subsystem-specific setters ####

def set_cmp_perf(obj, data):
    assert isinstance(data, CompressorPerfInputs)
    obj._inputs = data


#### Subsystem-specific components ####

class CmpSeedDispenser(object):

    def get_seed(self, nm):
        return CompressorSeed()


def CompIPC():
    return GenericSubsys(
        seed_dispenser=CmpSeedDispenser(),
        input_ports=[
            InputPort(
                name='set_perf_data',
                description='Set the performance requirements for the compressor',
                type=CompressorPerfInputs,
                fn=set_cmp_perf
            ),
            InputPort(
                name='set_duct_rads',
                description='Duct radius that this compressor needs to mate on to',
                type=DuctInputs,
                fn=lambda obj, data: None
            )
        ]
    )

CompHPC = CompIPC


class CompressorSeed(object):

    def __init__(self):
        self._inputs = None

    def run(self, datastore):
        flow = self._inputs.flow
        eta_min = 0.9
        eta_max = 0.95
        tau = 2.
        eta = eta_min + (eta_max - eta_min)*(1 - math.exp(-tau * flow))
        datastore['result'].put_json({'eta': eta})


class Corrections(object): ...



class PerfModel(object):

    def __init__(self):
        self._output_ports = {}
        self._output_ports['get_ipc_data'] = OutputPort(
            name='get_ipc_data',
            description='Performance inputs to the IPC',
            type=CompressorPerfInputs,
            fn=lambda d: CompressorPerfInputs(pr=d['IPC_PR'], flow=d['FLOW'])
        )
        self._output_ports['get_hpc_data'] = OutputPort(
            name='get_hpc_data',
            description='Performance inputs to the HPC',
            type=CompressorPerfInputs,
            fn=lambda d: CompressorPerfInputs(pr=d['HPC_PR'], flow=d['FLOW'])
        )



    def list_output_ports(self):
        return ['get_ipc_data', 'get_hpc_data']

    def list_input_ports(self):
        return ['set_ipc_bid', 'set_hpc_bid']

    def get_output_port(self, nm):
        return self._output_ports[nm]

    def get_seed(self, nm):
        return PerfSeed()

    def get(self, datastore, nm):
        data = datastore['result'].get_json()
        if nm == 'get_ipc_data':
            return CompressorPerfInputs(pr=data['IPC_PR'], flow=data['FLOW'])
        elif nm == 'get_hpc_data':
            return CompressorPerfInputs(pr=data['HPC_PR'], flow=data['FLOW'])
        else:
            raise Exception


class PerfSeed(object):
    def __init__(self):
        self.data = {'HPC_ETA': 0.93, 'IPC_ETA': 0.92}

    def run(self, datastore):
        # zero physics here - just a calculation
        flow = 5. * (1 - self.data['HPC_ETA'] * self.data['IPC_ETA'])
        datastore['result'].put_json({
            'HPC_PR': 5.0,
            'IPC_PR': 10.0,
            'FLOW': flow
        })

    def apply(self, nm, data):
        if nm == 'set_ipc_bid':
            self.data['IPC_ETA'] = data
        elif nm == 'set_hpc_bid':
            self.data['HPC_ETA'] = data
        else:
            raise Exception


def build_model():
    model = Model()

    # Add calculation nodes to the model
    model.add_subsystem('EngineCycle', PerfModel())
    model.add_subsystem('IPC', CompIPC())
    model.add_subsystem('HPC', CompHPC())
    model.add_subsystem('Corrections', Corrections())

    # Node connectivity (data flows)
    model.connect(src='EngineCycle.get_ipc_data', dst='IPC.set_perf_data')
    model.connect(src='EngineCycle.get_hpc_data', dst='HPC.set_perf_data')
    model.connect(src='IPC.get_perf_bid', dst='EngineCycle.set_ipc_bid')
    model.connect(src='HPC.get_perf_bid', dst='EngineCycle.set_hpc_bid', via=['Corrections.hpc_corrections'])

    return model

def execute(model, datastore):
    model.run('Corrections', datastore)

    converged = False
    while not converged:
        model.run('EngineCycle', datastore)
        model.run('IPC', datastore)
        model.run('HPC', datastore)
        converged = model.check_convergence('EngineCycle')