import asyncio
import json
import sys


class ClientServerProtocol(asyncio.Protocol):

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        decoded_data = data.decode()
        resp = self.make_answer(decoded_data)
        self.transport.write(resp.encode())

    def make_answer(self, data):
        command = data[0:3]
        if command == 'put':
            return self.write_to_file(data)
        elif command == 'get':
            return self.read_file(data)
        else:
            return 'error\nwrong command\n\n'

    def write_to_file(self, data):
        lst = data.split()
        if len(lst) != 4:
            return 'error\nwrong command\n\n'

        key, metric, timestamp = lst[1], lst[2], lst[3]

        try:
            metric = float(metric)
            timestamp = int(timestamp)
        except:
            return 'error\nwrong command\n\n'

        try:
            file = open('data.txt', 'r')
            dict = json.loads(file.readlines()[0])
            file.close()
            if key not in dict:
                dict[key] = [(metric, timestamp)]
            else:
                dict[key].append((metric, timestamp))
        except:
            dict = {key: [(metric, timestamp)]}

        file = open('data.txt', 'w')
        json.dump(dict, file)
        file.close()
        return 'ok\n\n'

    def read_file(self, data):
        if len(data.split()) != 2:
            return 'error\nwrong command\n\n'

        try:
            key = data.split()[1]
            file = open('data.txt', 'r')
            dict = json.loads(file.readlines()[0])
            file.close()

            if key == '*':
                resp = self.return_all_metrics(dict)

            else:
                resp = self.return_metrics_for_key(dict, key)

            return resp

        except:
            return 'ok\n\n'

    def return_all_metrics(self, dict):
        metrics = set()
        for k in dict:
            for tpl in dict[k]:
                value = tpl[0]
                timestamp = tpl[1]
                metrics.add((k, value, timestamp))

        resp = 'ok\n'
        for k in metrics:
            key, value, timestamp = k[0], k[1], k[2]
            resp += key + ' ' + str(value) + ' ' + str(timestamp) + '\n'
        resp += '\n'

        return resp

    def return_metrics_for_key(self, dict, key):
        key_lst = dict[key]
        key_set = set()

        for k in key_lst:
            value = k[0]
            timestamp = k[1]
            key_set.add((value, timestamp))

        resp = 'ok\n'
        for k in key_set:
            value = k[0]
            timestamp = k[1]
            resp += key + ' ' + str(value) + ' ' + str(timestamp) + '\n'
        resp += '\n'

        return resp


class Server:

    def __init__(self):
        pass

    def run_server(self, host, port):
        file = open('data.txt', 'w')
        file.close()
        loop = asyncio.get_event_loop()
        coro = loop.create_server(ClientServerProtocol, host, port)
        try:
            server = loop.run_until_complete(coro)
        except OSError:
            print('Ошибка операционной системы\n'
                  'Возможно, проблема в том, что адрес сокета уже используется.\n'
                  'Попробуйте запустить сервер с другим адресом.')
            sys.exit(0)
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        except:
            print('Неизвестная ошибка сервера')
            sys.exit(0)
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()


server = Server()
server.run_server('127.0.0.1', 8888)
