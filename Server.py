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
        if data[0:3] == 'put':
            return self.write_to_file(data)
        elif data[0:3] == 'get':
            return self.read_file(data)
        else:
            return 'error\nwrong command\n\n'

    def write_to_file(self, data):
        lst = data.split()
        if len(lst) == 4:
            try:
                metric = float(lst[2])
                timestamp = int(lst[3])
            except:
                return 'error\nwrong command\n\n'
            try:
                f = open('data.txt', 'r')
                dict = json.loads(f.readlines()[0])
                if lst[1] not in dict:
                    dict[lst[1]] = [(lst[2], lst[3])]
                else:
                    dict[lst[1]].append((lst[2], lst[3]))
                f.close()
                f1 = open('data.txt', 'w')
                json.dump(dict, f1)
                f1.close()
            except:
                f = open('data.txt', 'w')
                dict = {lst[1]: [(lst[2], lst[3])]}
                json.dump(dict, f)
                f.close()
            return 'ok\n\n'
        else:
            return 'error\nwrong command\n\n'

    def read_file(self, data):
        if len(data.split()) == 2:
            try:
                key = data.split()[1]
                f = open('data.txt', 'r')
                dict = json.loads(f.readlines()[0])
                if key == '*':
                    metrics = []
                    for i in dict:
                        key = i
                        dict[key] = sorted(dict[key], key=lambda tpl: tpl[1], reverse=False)
                        for e in dict[key]:
                            value = e[0]
                            timestamp = e[1]
                            if (key, value, timestamp) not in metrics:
                                metrics.append((key, value, timestamp))
                    resp = 'ok\n'
                    for i in metrics:
                        resp += i[0] + ' ' + i[1] + ' ' + i[2] + '\n'
                    resp += '\n'

                else:

                    key_lst = dict[key]
                    key_lst = sorted(key_lst, key=lambda tpl: tpl[1], reverse=False)
                    key_lst_1 = []
                    for i in key_lst:
                        if i not in key_lst_1:
                            key_lst_1.append(i)
                    resp = 'ok\n'
                    for i in key_lst_1:
                        resp += key + ' ' + i[0] + ' ' + i[1] + '\n'
                    resp += '\n'
                f.close()
                return resp
            except:
                return 'ok\n\n'
        else:
            return 'error\nwrong command\n\n'


class Server:

    def __init__(self):
        pass

    def run_server(self, host, port):
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


serv = Server()
serv.run_server('127.0.0.1', 8888)
