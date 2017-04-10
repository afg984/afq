'''
client for afq
'''

import threading
import collections
import socket
import os
import json
import weakref


class Receiver(object):
    '''
    event wrapper for receiving asynchronous json rpc response
    '''
    __slots__ = ('event', 'value', '__weakref__')
    def __init__(self):
        self.event = threading.Event()


class RemoteException(Exception):
    '''
    Error raised by the remote call
    '''
    pass


class RPCClinet(object):
    '''low level jsonrpc client'''
    def __init__(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        self.count = 0
        self.count_lock = threading.Lock()
        self.receivers = weakref.WeakValueDictionary()
        thread = threading.Thread(target=self._readloop)
        thread.setDaemon(True)
        thread.start()

    def _get_id(self):
        with self.count_lock:
            self.count += 1
            return self.count

    def _send_command(self, command, kwargs):
        receiver = Receiver()
        id = self._get_id()
        self.receivers[id] = receiver
        self.socket.sendall(
            json.dumps({
                'method': command,
                'params': [kwargs],
                'id': id
            }).encode() +
            b'\n'
        )
        receiver.event.wait()
        return receiver.value

    def call(self, method, **kwargs):
        '''
        call the method on the RPC server
        return
        '''
        result = self._send_command(method, kwargs)
        if result['error'] is not None:
            raise RemoteException(result['error'])
        else:
            return result['result']

    def _readloop(self):
        file = self.socket.makefile('r')
        for line in file:
            recvobj = json.loads(line)
            receiver = self.receivers[recvobj['id']]
            receiver.value = recvobj
            receiver.event.set()


ProcessStatus = collections.namedtuple(
    'ProcessStatus',
    ('exited', 'exit_status'))


class Master(object):
    '''
    Client to talk to afq
    '''
    def __init__(self, host, port):
        self.rpc = RPCClinet(host, port)

    def query(self, id):
        '''
        query the process by given id
        returns a ProcessStatus
        '''
        result = self.rpc.call('Master.Query', id=id)
        return ProcessStatus(**result)

    def launch(self, *args, **kwargs):
        '''
        launch a process

        *args: the command to run
        cwd: working directory.
             defaults to the current working directory
        env: environment variables, should be a dict.
             defaults to the current environment variables
        ncpu: number of reserved cpus, defaults to 1

        returns the id to refer to the proecss
        '''
        cwd = kwargs.get('cwd', os.getcwd())
        env = kwargs.get('env', os.environ)
        ncpu = kwargs.get('ncpu', 1)
        env = ['%s=%s' % (k, v) for (k, v) in env.items()]
        result = self.rpc.call(
            'Master.Launch',
            name=args[0],
            args=args[1:],
            cwd=cwd,
            env=env,
            ncpu=ncpu)
        return result['id']

    def signal(self, id, signal):
        '''
        send a signal to the process
        returns a ProcessStatus

        Note: The process may not terminate immediately.
        '''
        result = self.rpc.call('Master.Signal', id=id, signal=signal)
        return ProcessStatus(**result)

    def wait(self, id):
        '''
        wait for the process with the id to exit.
        returns a ProcessStatus
        '''
        result = self.rpc.call('Master.Wait', id=id)
        return ProcessStatus(**result)

    def waitany(self, *ids):
        '''
        wait for multiple processes with the corresponding ids to exit.
        returns a tuple (id, status)
        id: the first exiting process' id
        status: ProcessStatus
        '''
        result = self.rpc.call('Master.WaitAny', ids=ids)
        return result['id'], ProcessStatus(**result['status'])
