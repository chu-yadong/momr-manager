import os
import queue
import time
import momr_udp_server
from concurrent.futures import ThreadPoolExecutor
import threading
import momr
import configparser
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox


class MomrManager(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        # Momr反应器列表，通过查询data文件夹下有多少个目录来实现，目录内包含一个配置文件，是用来初始化momr模型
        self.get_data_run_enable = False  # 获取数据进程运行标志
        self.send_cmd_to_momr_enable = False  # 发送数据到momr使能标志
        self.manager_config = None  # 读取config.conf文件，内部包含 udpserver配置，guiserver配置等信息
        self.Momr_List = []
        self.Clients_Status = {'signed_clients': [], 'unsigned_clients': []}   # 分为 signed和unsigned反应器两种
        self.get_data_flg = False
        self.udp_server_ip = ''
        self.udp_server_port = 0
        self.udp_server = None
        self.load_config()
        self.load_momrs()
        self.signed_clients_status = []
        self.unsigned_clients_status = []
        self.config_momr_udp_server()

    def load_config(self):
        """载入配置文件"""
        print('loading config')
        try:
            self.manager_config = configparser.ConfigParser()
            self.manager_config.read('config.conf')
        except Exception as e:
            print(e)

    def config_momr_udp_server(self):
        """配置momr_udp_server"""
        try:
            self.udp_server_ip = self.manager_config['momr_udp_server']['ip']
            self.udp_server_port = int(self.manager_config['momr_udp_server']['port'])
            self.udp_server = momr_udp_server.MomrUdpServer(self.udp_server_ip, self.udp_server_port)
            time.sleep(1)
            # 添加客户端列表
            for momr_client in self.Momr_List:
                self.udp_server.add_Client(momr_client.MomrIP)
            # 启动udp_server
            self.udp_server.setDaemon(True)
            self.udp_server.start()
        except Exception as e:
            print(e)

    def load_momrs(self):
        """ 加载momr目录,每个目录对应一个momr反应器"""
        # 目录的个数对应有多少个momr反应器，然后再初始化各个momr模型
        folders = []
        try:
            for item in os.listdir('.\\data'):
                # 判断是否为目录
                item_path = '.\\data' + '\\' + item
                if os.path.isdir(item_path):
                    folders.append(item)
        except Exception as e:
            print(e)
        try:
            for m_folder in folders:
                self.Momr_List.append(momr.Momr(m_folder))
                # print(len(self.Momr_List))
        except Exception as e:
            print(e)

    def get_momr_clients_status(self):
        return self.Clients_Status

    def run(self):
        self.get_data_run_enable = True
        print('get data form udp service: start')
        while self.get_data_run_enable:
            # read data
            signed_dict = self.udp_server.get_signed_reactor_data()
            unsigned_dict = self.udp_server.get_unsigned_reactor_data()
            if len(signed_dict):
                for key in signed_dict.keys():
                    for Momr in self.Momr_List:
                        if key == Momr.MomrIP:
                            Momr.receive_handle(signed_dict[key])

            # check momr status
            # signed clients status  遍历所有模型，检查其在线状态
            signed_clients = []
            for m in self.Momr_List:
                if m.MomrOnline > 1:
                    m.MomrOnline -= 1  # 每次对其在线值-1，如果小于1则超时离线，模型每次收到数据会自己变成10
                    signed_clients.append((m.MomrIP, m.MomrName, True))
                else:
                    signed_clients.append((m.MomrIP, m.MomrName, False))
                self.Clients_Status.update({'signed_clients': signed_clients})

            # unsigned clients status  这里只有在线的非注册反应器会显示，长时间不活动的不保存
            unsigned_clients = []
            if len(unsigned_dict):
                for key in unsigned_dict.keys():
                    if time.time() - unsigned_dict[key]['data'][0] < 100:  # 如果时间差小于100秒，就是活动的，将其加入到列表
                        unsigned_clients.append((key, int(time.time() - unsigned_dict[key]['data'][0]), True))

                self.Clients_Status.update({'unsigned_clients': unsigned_clients})

            # 间隔1秒
            time.sleep(1)
        print('get data form udp service: stop')

    def send_cmd_to_momr(self, server_udp, cmd_dict, momr_name=None, momr_ip=None, ):
        try:
            if momr_name is None and momr_ip is None:
                raise NameError
            else:
                for Momr in self.Momr_List:
                    if momr_name == Momr.MomrName or momr_ip == Momr.MomrIP:
                        Momr.cmd_pre_to_send(cmd_dict)
                        data = Momr.get_cmd_wait()
                        if data:
                            server_udp.cmd_to_reactor(Momr.MomrIP, data)
        except Exception as e:
            print('发送到momr反应器需要指定名称name或ip地址', e)

    def stop(self):
        try:
            self.get_data_run_enable = False
            time.sleep(1)
            self.udp_server.stop()
            time.sleep(1)
            print('momr_manager stop')
        except Exception as e:
            print(e)


# 主窗体
class Application(tk.Tk):
    def __init__(self,  momrmanger=None):  # 顶层窗口master默认为空，应用中为root
        super().__init__()
        self.momrmager = momrmanger
        #self.master = master
        #self.propagate(0)
        #self.pack()  # 放置到屏幕上
        # 已注册反应器标签
        self.signed_lb = tk.Label(self, text='已注册反应器：')
        self.signed_lb.pack()
        # 已注册反应器frame
        self.frame_signed = tk.Frame(master=self)
        self.frame_signed.pack()
        # 已注册反应器y_scrollbar
        self.scrollbar_signed = tk.Scrollbar(self.frame_signed)
        self.scrollbar_signed.pack(side=tk.RIGHT, fill=tk.Y)
        # 已注册反应器的listbox
        self.listbox_signed = tk.Listbox(self.frame_signed, width=35, height=10,
                                         yscrollcommand=self.scrollbar_signed.set)
        self.listbox_signed.pack()
        # 未注册反应器标签
        self.unsigned_lb = tk.Label(self, text='未注册反应器：')
        self.unsigned_lb.pack()
        # 未注册反应器frame
        self.frame_unsigned = tk.Frame(master=self)
        self.frame_unsigned.pack()
        # 未注册反应器y_scrollbar
        self.scrollbar_unsigned = tk.Scrollbar(self.frame_unsigned)
        self.scrollbar_unsigned.pack(side=tk.RIGHT, fill=tk.Y)
        # 未注册反应器的listbox
        self.listbox_unsigned = tk.Listbox(self.frame_unsigned, width=35, height=5,
                                           yscrollcommand=self.scrollbar_unsigned.set)
        self.listbox_unsigned.pack()
        # 设置刷新按钮
        self.refresh_list_btn = tk.Button(self, text='刷新列表', command=self.show_momr_list)
        self.refresh_list_btn.pack()
        # 5s定时刷新列表
        self.auto_refresh_momr_list()

    def show_momr_list(self):
        momr_status = self.momrmager.get_momr_clients_status()  # 获取momr状态的字典 包含{momr_name, [momr_ip, momr_online]}
        signed_clients = momr_status['signed_clients']
        unsigned_clients = momr_status['unsigned_clients']
        # 刷新已经注册的反应器
        self.listbox_signed.delete(0, tk.END)
        if len(signed_clients):
            for s_m in signed_clients:
                self.listbox_signed.insert(tk.END, "IP: {}   名称: {}".format(s_m[0],s_m[1]))
                if s_m[-1]:
                    self.listbox_signed.itemconfig(tk.END, {'fg': 'green'})
                else:
                    self.listbox_signed.itemconfig(tk.END, {'fg': 'red'})

        # 刷新没有注册的反应器
        self.listbox_unsigned.delete(0, tk.END)
        if len(unsigned_clients):
            for uns_m in unsigned_clients:
                self.listbox_unsigned.insert(tk.END, "IP: {}   连接: {}".format(uns_m[0],uns_m[1]))
                if uns_m[-1]:
                    self.listbox_unsigned.itemconfig(tk.END, {'fg': 'green'})
                else:
                    self.listbox_unsigned.itemconfig(tk.END, {'fg': 'green'})

    def auto_refresh_momr_list(self):
        self.show_momr_list()
        self.after(5000, self.auto_refresh_momr_list)


if __name__ == "__main__":
    momrmanager = MomrManager()
    momrmanager.setDaemon(True)
    momrmanager.start()
    #root = tk.Tk()
    # root.geometry('350x230+400+300')
    # root.resizable(False, False)
    #root.title('Momr Manager')
    app = Application(momrmanger=momrmanager)
    app.mainloop()
    #root.mainloop()
    momrmanager.stop()

    # momrmanager = MomrManager()
    # momrmanager.setDaemon(True)
    # momrmanager.start()

    '''print('Start')
    udp_server = momr_udp_server.MomrUdpServer('192.168.1.168', 19876)
    momrmanager = MomrManager()

    # 将Momr客户端添加到 UDP_Server
    for m in momrmanager.Momr_List:
        ip_addr = m.MomrIP
        print(ip_addr)
        udp_server.add_Client(ip_addr)

    # 启动UDP_Server
    udp_server.setDaemon(True)
    udp_server.start()
    time.sleep(5)
    # 启动获取数据的线程
    get_data = threading.Thread(target=momrmanager.get_data_from_udp_service, args=(udp_server,))
    get_data.setDaemon(True)
    get_data.start()
    for i in range(5):
        print(udp_server.get_unsigned_connections())
        #momrmanager.send_cmd_to_momr(udp_server, {'Rpm_Set': 300}, momr_ip='192.168.1.169')
        time.sleep(5)
    momrmanager.get_data_run_enable = False
    udp_server.stop()
    time.sleep(5)'''
