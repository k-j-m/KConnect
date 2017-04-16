import unittest
from collections import defaultdict

import kconnect.model as mdl
import kconnect.examples as ex

class TestExamples(unittest.TestCase):
    """
    Set up some examples so that we can mock up a real system ASAP
    """

    def test_ipc_eta(self):
        perf_data = ex.CompressorPerfInputs(pr=5.0, flow=0.8)
        ipc = ex.CompIPC()
        ipc_seed = ipc.get_seed('asdf')
        ipc_seed.apply('set_perf_data', perf_data)

        dc = mdl.DataContainer()
        ipc_seed.run(dc)

        expected_eta = 0.939905174100267
        returned_eta = dc['result'].get_json()['eta']
        self.assertAlmostEquals(expected_eta, returned_eta)

    def test_perf_and_ipc(self):
        perf = ex.PerfModel()
        ipc = ex.CompIPC()

        datastore = mdl.DataStore()

        for i in range(10):
            perf_seed = perf.get_seed('lalal')
            if i > 0:
                data_item = datastore['ipc'][-1]
                ipc_eta = ipc.get('get_perf_bid', data_item)
                perf_seed.apply('set_ipc_bid', ipc_eta)

            dc_perf = datastore['perf'].add_new()
            perf_seed.run(dc_perf)

            ipc_perf_data = perf.get('get_ipc_data', datastore['perf'][-1])
            ipc_seed = ipc.get_seed('asdf')
            ipc_seed.apply('set_perf_data', ipc_perf_data)
            dc_ipc = datastore['ipc'].add_new()
            ipc_seed.run(dc_ipc)

        print('FLOWS')
        for pdata in datastore['perf']:
            #print(pdata)
            print(pdata['result'].get_json()['FLOW'])
            #print(datastore['ipc'][0]['result'].get_json())
        print('ETAS')
        for cdata in datastore['ipc']:
            print(cdata['result'].get_json()['eta'])


class ModelTests(unittest.TestCase):

    def test_create(self):
        """
        Make our example model without doing anything smart
        """
        empty_model = mdl.Model()
        built_model = ex.build_model()

    def test_list_ports(self):
        """
        List the ports for some of our calc nodes
        """
        expected = ['get_ipc_data', 'get_hpc_data']
        returned = ex.PerfModel().list_output_ports()
        self.assertEquals(set(expected), set(returned))

        expected = ['set_ipc_bid', 'set_hpc_bid']
        returned = ex.PerfModel().list_input_ports()
        self.assertEquals(set(expected), set(returned))

    def test_output_port(self):
        port = ex.PerfModel().get_output_port('get_ipc_data')
        d = {'IPC_PR': 4.5, 'FLOW': 1.0}
        class Accessor(object):
            ipc_data = ex.CompressorPerfInputs(pr=d['IPC_PR'], flow=d['FLOW'])

        expected = 4.5
        returned = port(Accessor()).pr
        self.assertEquals(expected, returned)

    def test_input_port(self):
        port = ex.CompIPC().get_input_port('set_perf_data')

    def test_port_compatibility(self):
        in_port = ex.CompIPC().get_input_port('set_perf_data')
        out_port = ex.PerfModel().get_output_port('get_ipc_data')
        mdl.verify_port_compatibility(src=out_port, dst=in_port)

        in_port2 = ex.CompIPC().get_input_port('set_duct_rads')
        self.assertRaises(mdl.IncompatiblePorts, mdl.verify_port_compatibility, out_port, in_port2)

    def test_bad_port_connection(self):
        """
        Make a bad port connection and make sure that a suitable error is raised
        """
        model = mdl.Model()
        model.add_subsystem('EngineCycle', ex.PerfModel())
        model.add_subsystem('IPC', ex.CompIPC())

        # good connection
        model.connect(src='EngineCycle.get_ipc_data', dst='IPC.set_perf_data')

        # typos in subsys name
        self.assertRaises(AttributeError, model.connect, src='EngineCycleZ.get_ipc_data', dst='IPC.set_perf_data')
        self.assertRaises(AttributeError, model.connect, src='EngineCycle.get_ipc_data', dst='IPCZ.set_perf_data')

        # typos in port name
        self.assertRaises(AttributeError, model.connect, src='EngineCycle.get_ipc_dataZ', dst='IPC.set_perf_data')
        self.assertRaises(AttributeError, model.connect, src='EngineCycle.get_ipc_data', dst='IPC.set_perf_dataZ')

    def test_simple_model_execution(self):
        """
        Run a simple model and check that we can look at some results
        """
        model = mdl.Model()
        model.add_subsystem('EngineCycle', ex.PerfModel())
        runnable_model = model.configure({'EngineCycle': 'lalala'})
        datastore = mdl.DataStore()
        runnable_model.run('EngineCycle', datastore)
        self.assertTrue('EngineCycle' in datastore)
        ditem = datastore['EngineCycle'][-1]
        perf_data = ex.PerfAccessor(ditem)
        print(perf_data.data)
        #result = model.get_data_obj('EngineCycle', datastore)

    def test_coupled_model_execution(self):
        """
        Run a model with one node connected to another
        """
        model = mdl.Model()
        model.add_subsystem('EngineCycle', ex.PerfModel())
        model.add_subsystem('IPC', ex.CompIPC())
        # good connection
        model.connect(src='EngineCycle.get_ipc_data', dst='IPC.set_perf_data')
        runnable_model = model.configure(defaultdict(lambda: 'asdf'))
        datastore = mdl.DataStore()
        runnable_model.run('EngineCycle', datastore)
        runnable_model.run('IPC', datastore)

        print(datastore)
        print(datastore['EngineCycle'][-1]['result'].read())
        print(datastore['IPC'][-1]['result'].read())
        # perf_data = ex.PerfAccessor(datastore['EngineCycle'][-1]).data
        # print(perf_data)
        # perf_flow = perf_data['FLOW']
        # cmpr_flow = ex.CmpAccessor(datastore['IPC'][-1]['result'].get_json()['flow'])
        # self.assertEquals(perf_flow, cmpr_flow)

    def test_cyclic_model_execution(self):
        """
        Set up a simple model with a cyclic data dependency and get the thing to run
        """
        model = mdl.Model()
        model.add_subsystem('EngineCycle', ex.PerfModel())
        model.add_subsystem('IPC', ex.CompIPC())
        model.connect(src='EngineCycle.get_ipc_data', dst='IPC.set_perf_data')
        model.connect(src='IPC.get_perf_bid', dst='EngineCycle.set_ipc_bid')
        runnable_model = model.configure(defaultdict(lambda: 'asdf'))
        datastore = mdl.DataStore()
        history = []
        for _ in range(5):
            runnable_model.run('EngineCycle', datastore)
            runnable_model.run('IPC', datastore)
            d = model.get(datastore, 'EngineCycle', 'get_ipc_data')
            if history:
                self.assertTrue(d is not history[-1])
            history.append(d)
            print(d.flow)

    def test_full_model(self):
        model = ex.build_model()
        runnable_model = model.configure(defaultdict(lambda: 'asdf'))
        datastore = mdl.DataStore()
        ex.execute(runnable_model, datastore)


class TestDataStore(unittest.TestCase):
    """
    Tests for the DataStore class, which aggregates the results of the model execution
    """

    def test_create(self):
        mdl.DataStore()

    def test_append(self):
        dstore = mdl.DataStore()
        dstack = dstore['ASDF']
        new_container = dstack.add_new()
        s_exp = '{"foo": "bar"}'
        new_container['item'].write(s_exp)

        s_ret = dstore['ASDF'][0]['item'].read()
        self.assertEquals(s_ret, s_exp)

    def test_contains(self):
        dstore = mdl.DataStore()
        dstack = dstore['ASDF']
        self.assertTrue('ASDF' in dstore)

    def test_datastack(self):
        """
        Add a new container on to a clean datastack
        """
        dstack = mdl.DataStack()

    def test_datastack_implicit_false(self):
        """
        Check that a datastack's implicit false __nonzero__ method works
        """
        dstack = mdl.DataStack()
        self.assertFalse(dstack)
        dstack.add_new()
        self.assertTrue(dstack)

    def test_data_container_single_item(self):
        """
        Test the folder-like DataContainer class
        """
        dc = mdl.DataContainer()
        dc['item'].write('asdf')
        self.assertEquals(dc['item'].read(), 'asdf')

    def test_data_container_multi_items(self):
        """
        Add multiple items to the data container
        """
        dc = mdl.DataContainer()
        dc['item'].write('asdf')
        dc['item2'].write('qwerty')
        self.assertEquals(dc['item'].read(), 'asdf')
        self.assertEquals(dc['item2'].read(), 'qwerty')

    def test_dataitem_contextmgr(self):
        """
        make sure that the dataitem can be used using with syntax
        """
        d = ['zoorp']
        def read_fn():
            return d[0]
        def write_fn(data):
            d[0] = data
        ditem = mdl.DataItem(read_fn, write_fn)

        with ditem.open_to_read() as sio:
            s = sio.read()
            self.assertEquals(s, 'zoorp')

        with ditem.open_to_write() as w:
            w.write('lalala')

        self.assertEquals(d[0], 'lalala')

    def test_dataitem_json(self):
        """
        JSON is a common use case. Let's support it from the start as a 
        productivity-boosting convenience.
        """
        exp_data = {'foo':'bar'}
        dc = mdl.DataContainer()
        dc['item'].put_json(exp_data)

        ret_data = dc['item'].get_json()
        self.assertFalse(exp_data is ret_data)
        self.assertEquals(exp_data, ret_data)


if __name__ == '__main__':
    unittest.main()
