import struct
import time
from datetime import datetime
from collections import OrderedDict
import sqlite3
import pandas
import importlib

com_dict = {}
rec_dict = {}
serial_run_flag = 0

# 数据库存储结构 这个以后会自动生成
Sql_Save_Format = '''DateTime TEXT, Run_Time REAL, T_Reactor REAL, T_Radiator REAL, IR_I INTEGER, 
                    IR_T INTEGER,IR_S INTEGER, OD REAL, ST REAL, RPM INTEGER, AIR_T REAL, P1_T REAL,
                    P2_T REAL, P3_T REAL, ERR INTEGER'''


Get_Sql_Tables_Command = '''select name from sqlite_master where type='table' order by name'''


class Momr(object):  # 定义 微型在线反应器 的串口总线类
    def __init__(self, data_folder):
        self.DataFolder = data_folder  # 数据存放文件夹
        self.RecBufferSize = 100       # 接收数据缓存大小
        self.CmdBufferSize = 100       # 发送数据缓存大小
        self.MomrName = None           # 反应器名称
        self.MomrIP = None             # 反应器IP地址
        self.MomrOnline = 0            # 反应器是否在线 每次接收到数据变成10，manager定期查询并减掉1，变成0，为离线
        self.MomrDatabases = None      # 反应器数据库链顺序接列表
        self.ReactorDataFormat = OrderedDict()  # 接收数据帧的数据格式
        self.recDataBuffer = OrderedDict()      # 接受数据的buffer，这个在初始化的时候按照config.RecDataFormat的格式进行初始化，用于以后数据解析
        self.cmdDataBuffer = OrderedDict()      # 将反应器配置文件中为 w 的值加入这个字典中，作为生成命令的buffer
        self.LocalVariables = None     # 本地变量
        self.commandFormat = None      # 发送数据帧格式
        self.Cmd_ID = 0                # 发送的命令编号，接收数据的时候检查是否收到回应
        self.Rec_ID = 0                # 接受到的数据条的计数
        self.receiveDataStr = ''       # 接收的最后一个数据的字符串缓存
        self.receiveData = []    # 列表内存放字典{'DataID':1234,'dataTimeStamp':124223,'data':{分析后的数据}}key未能int型id分解的数据为value
        self.commandData = []    # 列表内存放字典{'Cmd_ID': self.Cmd_ID, 'Cmd_TimeStamp': time.time(), 'Cmd_Status': 'Wait，Sent，OK，delay，Error，Timeout', 'Cmd_Str': cmd_str}
        self.readConfig()
        '''self.receiveTime = None  # 接收数据的时间
        self.receiveBytesAll = None   # 接收到的全部数据
        self.receiveLength = 0        # 应当接收的数据长度
        self.commandBytesAll = None   # 发送数据的全部数据
        self.commandBytesData = None  # 发送数据的bytes数据
        self.unpackedReceiveData = None  # 解包后的数据帧，需要判断格式是否正确后再传递到receiveData
        self.receiveFlag = 0  # 接收数据成功标志
        self.valuesFlag = 0  # 数据更新成功标志
        self.commandFlag = 0  # 发送数据成功标志
        self.commandID = 0
        self.receiveID = 0
        self.sql_db_name = "./db/"+self.reactorName+".db"
        self.sqlDB = None
        self.sqlCur = None
        self.sqlTableName = None
        self.sqlColumnsFormat = Sql_Save_Format
        self.sqlColumns = []
        self.sqlTableList = []
        self.onlineFlag = 1
'''
    def readConfig(self):
        try:
            module_dir = 'data.'+ self.DataFolder + '.config'
            config = importlib.import_module(module_dir)
            elements = dir(config)
            if 'Name' in elements:
                self.MomrName = config.Name
            else:
                print('配置文件config.py 缺少反应器名称：Name')
            if 'IP' in elements:
                self.MomrIP = config.IP
            else:
                print('配置文件config.py 缺少反应器IP地址：IP')
            if 'DataBases' in elements:
                self.MomrDatabases = config.DataBases
            else:
                print('配置文件config.py 缺少反应器IP地址：IP')
            if 'ReactorDataFormat' in elements:
                temp_dic = eval(config.ReactorDataFormat)
                for key in temp_dic.keys():  # 建立一个接收数据的buffer字典，建立对应的数据格式的字典
                    self.ReactorDataFormat[key] = temp_dic[key][0]
                    self.recDataBuffer[key] = temp_dic[key][-1]
                    if temp_dic[key][1] == 'w':  # 建立命令数据的buffer字典
                        self.cmdDataBuffer[key] = temp_dic[key][-1]
            else:
                print('配置文件config.py 缺少反应器数据格式定义：ReactorDataFormat')
            if 'LocalVariables' in elements:
                self.LocalVariables = config.LocalVariables
            else:
                print('配置文件config.py 缺少本地数据格式定义：LocalVariables')
        except Exception as e:
            print(e)

    def cmd_pre_to_send(self, cmd_dict):  # 接收到的是以字典格式的命令数据 例如 {'RPM_Set': 300}
        self.Cmd_ID += 1
        cmd_str = 'Cmd_ID:{}'.format(self.Cmd_ID)
        for key in cmd_dict.keys():
            if key in self.cmdDataBuffer.keys():
                value = cmd_dict[key]
                if isinstance(value, float):  # 如果是浮点数，防止浮点数过长
                    if len(str(value).split(".")[1]) > 10:  # 如果小数部分超过10位，则最多10位
                        value = '{:.10f}'.format(value)
                cmd_str = "{},{}:{}".format(cmd_str, key, value)
        cmd_str = cmd_str + ','  # 尾部补加一个, 用来作为返回数据时的标记位
        self.commandData.append({'Cmd_ID': self.Cmd_ID, 'Cmd_TimeStamp': time.time(), 'Cmd_Status': 'Wait', 'Cmd_Str': cmd_str})
        return cmd_str

    def get_cmd_wait(self):
        for i in range(len(self.commandData)):  # 遍历所有commandData
            if self.commandData[i]['Cmd_Status'] == 'Wait':  # 如果状态为Wait 等待发送
                self.commandData[i]['Cmd_Status'] = 'Sent'  # 修改状态为 已发送
                return self.commandData[i]['Cmd_Str'].encode('utf-8')  # 将数据utf-8编码后返回
        return None  # 若没有等待发送数据则返回None

    def cmd_response_resolver(self, cmd_response_str, received_time):
        try:
            cmd_id_str = cmd_response_str[:cmd_response_str.index('*')]  # 首先将数据用第一个 ， 把Cmd_ID提取出来
            cmd_id = int(cmd_id_str.split(':')[1])  # 将cmd_id从冒号后取出来
            fond_id_flg = False
            for i in range(len(self.commandData)):  # 检索整个commandData列表
                if cmd_id == self.commandData[i]['Cmd_ID']:  # ID一致
                    resp_list = cmd_response_str.split('*')  # 将数据使用 ， 分割
                    for item in resp_list:
                        if '#' in item or '!' in item:  # 如果有#或者！，证明反应器接收到了数据，但是执行有误，可能为格式错误
                            print('#/!', item)
                    self.commandData[i]['Cmd_Status'] = '命令部有误'
                    # self.save_cmd_to_sql()  # 数据保存等工作
                    self.commandData[i]['Cmd_Status'] = '命令正常'  # 正确执行并返回,等待存入数据库
                    fond_id_flg = True
            if not fond_id_flg:
                print('Cmd_ID不在等待结果的command列表中，这就很奇怪了，Cmd_ID怎么会不对！')
        except Exception as e:
            print(e)

    def save_cmd_to_sql(self):
        print('保存返回的comd到数据库')

    def check_cmd_timeout(self):
        """正常情况下命令是不会超过30秒还没有返回数据，这个时候外部MomrRunManager会定时 5s 检查一次"""
        for i in range(len(self.commandData)):
            if time.time() - self.commandData[i]['Cmd_TimeStamp'] > 15:  # 如果当前时间超过命令发送的登记时间30秒
                print("命令发送超时:{}".format(self.commandData[i]['cmd']))
                self.commandData[i]['status'] = 'TimeOut'
                # 将错误写入记录 后续补充

    def data_resolver(self, data_str, received_time):  # 数据字符串，udpserver接收到数据的时间
        data_list = data_str.split(',')  # 首先将字符串分割
        if len(data_list) == len(self.recDataBuffer):  # 检查分割的字符串个数与数据格式的数量是否一致
            i = 0
            temp_dic = OrderedDict()
            for key in self.recDataBuffer.keys():
                data_type = self.ReactorDataFormat[key]  # 获取这个数据的数据格式
                if data_type in 'BHL':  # 如果数据类型是B unsigned char H short L long
                    temp_dic[key] = int(data_list[i])
                elif data_type == 'f':  # 如果数据类型是 float
                    temp_dic[key] = float(data_list[i])
                else:  # 其他数据类型按照字符串处理
                    temp_dic[key] = data_list[i]
                i += 1
            self.Rec_ID += 1

            self.receiveData.append({'DataID': self.Rec_ID, 'dataTimeStamp': received_time, 'data': temp_dic})
            # 检查是否超过缓冲区大小
            #print(len(self.receiveData))
            if len(self.receiveData) > self.RecBufferSize:
                old_data = self.receiveData.pop(0)  # 删除最老的数据
                #print(old_data)
            # 测试一下从数据获取后发送到 SQL服务端
            # self.MOMR_Sql_Socket.sendto(json.dumps(self.data_buffer_dict).encode('utf-8'), ('127.0.0.1',8080))
        else:
            print("数据格式有误，请检查！")

    def receive_handle(self, received_dict):  # 返回的数据是一个元组{'ip_port': addr_port,'data':(time.time(), data_bytes)}
        if self.MomrIP == received_dict['ip_port'][0]:  # 检查IP是否正确
            self.MomrOnline = 10  # 在线为大于1
            received_time = received_dict['data'][0]  # udp server收到数据的时间
            received_str = received_dict['data'][1].decode('utf-8')
            if received_str.startswith('Cmd_ID:'):  # 命令回应数据开头是Cmd_ID
                self.cmd_response_resolver(received_str, received_time)
            elif received_str.startswith('192.168.'):  # 数据开头是IP地址
                self.data_resolver(received_str, received_time)
            else:
                print("好神奇，竟然不知道收到的是什么数据！")

    def sql_get_tables(self):
        try:
            self.sqlCur.execute(Get_Sql_Tables_Command)
            all_info = self.sqlCur.fetchall()
            self.sqlTableList = [item[0] for item in all_info]
            return True, self.sqlTableList
        except Exception as e:
            print(repr(e))
            return False, None

    def sql_set_working_table(self, table_name):
        try:
            self.sql_get_tables()
            if table_name not in self.sqlTableList:
                print("数据表名称错误！请检查！")
                return False, None
            self.sqlTableName = table_name
            self.sqlCur.execute("PRAGMA table_info({})".format(self.sqlTableName))
            table_info = self.sqlCur.fetchall()
            self.sqlColumns = [info[1] for info in table_info]
            return True, table_info
        except Exception as e:
            print(repr(e))
            return False, None

    def sql_connect(self):
        self.sqlDB = sqlite3.connect(self.sql_db_name)
        self.sqlCur = self.sqlDB.cursor()
        self.sqlDB.commit()
        self.sql_get_tables()  # 更新tables列表

    def sql_creat_table(self):
        try:
            now = datetime.now()
            self.sqlTableName = self.reactorName + now.strftime('%y%m%d%H%M%S')
            sql_str = 'create table ' + self.sqlTableName + '(' + self.sqlColumnsFormat+')'
            self.sqlCur.execute(sql_str)
            self.sqlDB.commit()
            self.sqlCur.execute("PRAGMA table_info({})".format(self.sqlTableName))
            table_info = self.sqlCur.fetchall()
            self.sqlColumns = [info[1] for info in table_info]
            self.sql_get_tables()  # 更新tables列表
            return True
        except Exception as e:
            print(repr(e))
            return False

    def sql_close(self):
        """
        关闭数据库
        """
        self.sqlCur.close()
        self.sqlDB.close()

    def sql_execute(self, sql, param=None):
        """
        执行数据库的增、删、改
        sql：sql语句
        param：数据，可以是list或tuple，亦可是None
        retutn：成功返回True，失败返回False
        """
        try:
            if param is None:
                self.sqlCur.execute(sql)
            else:
                self.sqlCur.execute(sql, param)
            count = self.sqlDB.total_changes
            self.sqlDB.commit()
        except Exception as e:
            print(repr(e))
            return False, e
        if count > 0:
            return True, count
        else:
            return False, 0

    def sql_query(self, sql, param=None):
        """
        查询语句
        sql：sql语句
        param：参数,可为None
        retutn：成功返回数据，失败返回None
        """
        try:
            if param is None:
                self.sqlCur.execute(sql)
            else:
                self.sqlCur.execute(sql, param)
            return self.sqlCur.fetchall()
        except Exception as e:
            print(repr(e))
            return None

    @staticmethod
    def get_data_format(formated_data):  # 获取数据存储结构
        data_format = []
        for item in formated_data:
            data_format.append(formated_data[item][0])
        return '<'+''.join(data_format)


    @staticmethod
    def get_unpack_data(data_format, rec_bytes):  # 获取解包接收的数据
        return struct.unpack(data_format, rec_bytes)


    def get_new_values(self):  # 获取新数据，有新数据返回新数据，若没有返回None，操作会清零receiveFlag
        if self.valuesFlag == 1:
            self.valuesFlag = 0
            return self.receiveData
        else:
            self.valuesFlag = 0
            return None

    def get_values(self):  # 获取接收到的数据，不论新旧，但是会清零receiveFlag
        self.valuesFlag = 0
        return self.receiveData

    def sql_save_values(self):
        value_keys = self.receiveData.keys()
        save_keys = [key for key in self.sqlColumns if key in value_keys]
        sql_command = "INSERT INTO " + self.sqlTableName + ' (' + ','.join(save_keys) + ',DateTime) values ( ?' + ',?' * len(save_keys) + ')'
        save_values = [self.receiveData[key][1] for key in save_keys]
        save_values.append(self.receiveTime)
        self.sql_execute(sql_command, save_values)

    def sql_get_time_range(self, time_min=None, time_max=None):
        if time_max is None and time_min is None:
            return self.sql_query('select * from {}'.format(self.sqlTableName))
        elif time_max is None:
            return self.sql_query("select * from {} where DateTime >= '{}'".format(self.sqlTableName, time_min))
        elif time_min is None:
            return self.sql_query("select * from {} where DateTime <= '{}'".format(self.sqlTableName, time_max))
        else:
            return self.sql_query("select * from {} where DateTime >= '{}' AND DateTime <= '{}'"\
                                  .format(self.sqlTableName, time_min, time_max))

'''
if __name__ == '__main__':
    m1 = Momr('Momr1')
    m1.read_config()
    print(type(m1.reactorName))
    print(type(m1.receiveFormat))
'''