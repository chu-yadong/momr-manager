import selectors
import threading
import time
import socket

# 根据平台自动选择最佳的IO多路机制，比如linux就会选择epoll,windows会选择select
sel = selectors.DefaultSelector()

# IP = '192.168.1.168'
# PORT = 19876
# DATA_LEN = 4096


class MomrUdpServer(threading.Thread):
    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.selector = selectors.DefaultSelector()
        self.IP = ip
        self.PORT = port
        self.data_buffer_size = 10
        self.cmd_buffer_size = 3
        self.Run_Enable = False  # 停止标志位
        self.sock = None
        self.MOMR_Sql_Socket = None
        self.Client_Data = {}  # 字典的key为IP地址，内容为列表，包含[[new?，数据计数，数据],[new?,命令计数，命令]] 内容为格式化字符串
        self.unsigned_ip = {}  # 没有定义的连接 key ip地址，value ip——port，最后一次数据{ip,(addr, data)}
        self.newData_Flg = False
        self.addBuffer = None
        self.init_Socket()

    def init_Socket(self):
        """初始化socket为UDP连接模式"""
        try:
            # 建立UDP连接模式
            self.sock = socket.socket(type=socket.SOCK_DGRAM)
            # 绑定IP与PORT
            self.sock.bind((self.IP, self.PORT))
            # 设置为非阻塞模式
            self.sock.setblocking(False)
            self.selector.register(self.sock, selectors.EVENT_READ, self.read)
            print('初始化Socket成功，ip:{},port:{}'.format(self.IP, self.PORT))
        except Exception as e:
            print(e)

    def add_Client(self, client_ip):
        """添加客户端"""
        if client_ip not in self.Client_Data.keys():
            '''创建一个新的客户端存储点以IP地址为Key对应的数据为[数据,数据],命令，命令总时最新的只有一个。 'statues':0的时候离线,接收到数据会变成10
            其中数据与命令为格式化字符串'''
            self.Client_Data[client_ip] = {'ip_port': None, 'data': [], 'command': ''}
        else:
            print('反应器列表中已经包含此IP:{}'.format(client_ip))

    def read(self, conn):
        if not self.Run_Enable:  # 需要停止的时候先 复位运行标志 自己发给自己一个消息，进入read，然后关闭socket
            self.sock.close()
            print("udp socket closed")
            print('udp server stop')
        else:
            try:
                # 读取数据
                data, addr = conn.recvfrom(4096)
                # 检查ip地址是否在监听列表内
                if addr[0] in self.Client_Data.keys():
                    # 更新注册反应器数据字典
                    self.Client_Data[addr[0]]['ip_port'] = addr
                    self.Client_Data[addr[0]]['data'].append((time.time(), data))  # 数据包含接收的时间戳和数据
                    if len(self.Client_Data[addr[0]]['data']) > self.data_buffer_size:  # 如果缓存大小到达10个
                        self.Client_Data[addr[0]]['data'].pop(0)     # 删除最旧的
                else:
                    # 更新未注册反应器
                    self.unsigned_ip.update({addr[0]: {'ip_port': addr, 'data': (time.time(), data)}})
            except Exception as e:
                print('错误：', e)

    def get_signed_reactor_data(self):
        """以字典形式将已接收的数据返回,返回数据data最老的一个，类似 FIFO，由于 manager 每秒访问一次，反应器一般5秒上传一次数据"""
        try:
            signed_dic = {}
            for cli_ip in self.Client_Data.keys():
                if len(self.Client_Data[cli_ip]['data']):
                    signed_dic[cli_ip] = {'ip_port': self.Client_Data[cli_ip]['ip_port'], 'data': self.Client_Data[cli_ip]['data'].pop(0)}
            return signed_dic  # 返回数字典
        except Exception as e:
            print(e)
            return {}

    def get_unsigned_reactor_data(self):
        return self.unsigned_ip

    def cmd_to_reactor(self, ip, command):
        """如果正常发送返回 True,错误返回 False，没有找到IP 返回 None"""
        # 检查IP是否在客户端列表
        if ip in self.Client_Data.keys():
            # 获取客户端的端口信息
            result = self.sock.sendto(command, self.Client_Data[ip]['ip_port'])
            if result == len(command):
                return True
            else:
                return False
        else:
            print('没有此ip')
            return None

    def run(self):
        self.Run_Enable = True
        print('udp server start')
        try:
            while True:
                if not self.Run_Enable:
                    break
                else:
                    events = self.selector.select()
                    for key, mask in events:   # 有活动对象了
                        callback = key.data    # key.data 是注册时传递的 accept 函数
                        callback(key.fileobj)  # key.fileobj 就是传递的 socket 对象

        except Exception as e:
            print(e)

    def stop(self):
        self.Run_Enable = False
        self.sock.sendto(' '.encode('utf-8'), (self.IP, self.PORT))  # 给自己发一个消息，跳出selector循环

    def research_new_connections(self):
        """重新搜索未注册的连接"""
        self.unsigned_ip.clear()

    def get_unsigned_connections(self):
        """获取未注册的连接"""
        return self.unsigned_ip


if __name__ == "__main__":
    m1 = MomrUdpServer('192.168.1.168', 19876)
    m1.add_Client('192.168.1.169')
    m1.setDaemon(True)
    m1.start()
    time.sleep(10)
    for i in range(3):
        # 向反应器发送数据
        m1.send_to_reactor('192.168.1.169', 'Cmd_ID:{},Rpm_Set:300,'.format(i).encode('utf-8'))
        for k in range(10):
            # 检查command内的发送选项是否为True
            if m1.Client_Data['192.168.1.169']['command'][0]:
                # 等待反馈
                time.sleep(0.5)
                # 如果反馈的数据以Cmd_ID:开头，表示返回的是命令
                if m1.Client_Data['192.168.1.169']['data'][2].decode('utf-8').startswith('Cmd_ID:'):
                    # 检查ID编号
                    print('接收到反馈数据：{}'.format(m1.Client_Data['192.168.1.169']['data'][2].decode('utf-8')))
                    m1.Client_Data['192.168.1.169']['command'][0] = False
            else:
                break
        time.sleep(9)
    print('stoped')