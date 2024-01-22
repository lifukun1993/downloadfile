import subprocess
import os
from configparser import ConfigParser
from time import sleep
from re import findall
import requests
from multiprocessing import Process
import datetime


# 检测最新文件并下载
class DetectDownload():
    def __init__(self, type_identifier):
        self.type_identifier = type_identifier
        self.path = os.getcwd()  # 脚本路径
        self.config = ConfigParser()
        self.config.read(f'{path}/conf.ini', encoding='utf-8')
        self.root_directory = self.config['conf']['root_directory']  # 根目录
        self.webhook = self.config['conf']['webhook']  # webhook地址
        self.new_cloud_url = self.config[self.type_identifier]['new_cloud_url']  # 最新的oss文件路径
        self.full_file_path = self.config[self.type_identifier]['full_file_path']  # 全量文件存储文件夹名称
        self.incremental_file_path = self.config[self.type_identifier]['incremental_file_path']  # 增量文件存储文件夹名称
        self.temporary_file_path = self.config[self.type_identifier]['temporary_file_path']  # 临时存放文件的文件夹
        self.create_folder()  # 创建文件下载后存储的文件夹
        self.oss_path = self.config[self.type_identifier]['oss_path']  # oss 文件路径
        self.serial_number = self.get_serial_number()  # 获取文件序号
        self.all_count = self.get_all_count()  # 初始全量

    # 获取文件序号
    def get_serial_number(self):
        full_path = os.path.join(self.root_directory, self.full_file_path)
        file_list = os.listdir(full_path)  # 获取全量的文件名
        full_path = os.path.join(self.root_directory, self.incremental_file_path)
        file_list.extend(os.listdir(full_path))  # 获取增量 + 全量的文件名
        if file_list:
            return max(file_list).split('-')[0]
        else:
            return '00000001'

    # 创建存储目录
    def create_folder(self):
        full_path = os.path.join(self.root_directory, self.full_file_path)  # 首次运行创建全量文件夹
        if not os.path.exists(full_path):  # 判断文件夹是否存在
            os.makedirs(full_path)  # 不存在创建文件夹
        full_path = os.path.join(self.root_directory, self.incremental_file_path)  # 首次运行创建增量文件夹
        if not os.path.exists(full_path):  # 判断文件夹是否存在
            os.makedirs(full_path)  # 不存在创建文件夹
        full_path = os.path.join(self.root_directory, self.temporary_file_path)  # 首次运行创建临时文件夹
        if not os.path.exists(full_path):  # 判断文件夹是否存在
            os.makedirs(full_path)  # 不存在创建文件夹

    # 获取oss文件路径
    def get_cloud_urls(self):
        cmd = f'cd {self.path} && ./ossutil64 ls -s {self.oss_path}'
        output = subprocess.check_output(cmd, shell=True).decode('utf-8')
        cloud_urls = {path for path in output.split('\n') if '.jsonl' in path and path > self.new_cloud_url}
        return cloud_urls

    # 获取文件大小
    def get_file_size_info(self, cloud_url):
        cmd = f'cd {self.path} && ./ossutil64 du {cloud_url}'
        output = subprocess.check_output(cmd, shell=True).decode('utf-8')
        size_info = findall('total object sum size: .*[0-9]*', output)
        if size_info:
            return size_info
        else:
            return "no size"

    # 检测文件是上传完成的
    def test_file_upload_completed(self, cloud_url):
        date_time = findall('stat_date=\d*', cloud_url)[0].split('=')[-1].strip()
        while True:
            statr_time = str(datetime.datetime.now() + datetime.timedelta(days=-1)).split(' ')[0].replace('-', '')
            if statr_time > date_time:
                return True  # 24小时前文件直接下载
            start_size_info = self.get_file_size_info(cloud_url)  # 记录文件大小信息
            sleep(5)  # 休眠5秒
            end_size_info = self.get_file_size_info(cloud_url)  # 5秒后的文件大小信息
            if end_size_info != 'no size' and start_size_info == end_size_info:  # 有文件大小信息，且5前后大小未发生变化
                return True  # 文件上传完成
            elif end_size_info != 'no size' and start_size_info != end_size_info:  # 5秒前后文件大小发生了变化
                continue   # 循环等待文件上传完成
            else:
                return False  # 没有文件大小信息的文件不作处理

    # 获取文件存储目录
    def get_file_url(self, cloud_url):
        try:
            date_time = findall('stat_date=\d*',cloud_url)[0].split('=')[-1].strip()  # 下载的文件夹日期
        except:
            date_time = str(datetime.datetime.now()).split(' ')[0].replace('-', '')  # 当天日期
        return [
            os.path.join(self.root_directory, self.full_file_path, ""),
            os.path.join(self.root_directory, self.incremental_file_path, ""),
            os.path.join(self.root_directory, self.temporary_file_path,
                         self.serial_number + '-' + date_time + '_' + cloud_url.split('/')[-1])
        ]

    # 获取全量
    def get_all_count(self):
        all_count = 0
        for folder in [self.full_file_path, self.incremental_file_path]:
            try:
                full_path = os.path.join(self.root_directory, folder)
                cmd = f"cd {full_path} && wc -l ./*.*"
                output = subprocess.check_output(cmd, shell=True).decode('utf-8')
                count = max([int(lines) for lines in findall('\d+ ', output.strip())])
            except Exception as e:
                print(e)
                count = 0
            all_count += count
        print(all_count)
        return all_count

    # 文件统计
    def file_statistics(self, count):
        count_msg = f'{self.full_file_path}: {count + self.all_count}\n{self.incremental_file_path}: {count}'
        print(count_msg)
        return [
            [
                {
                    "tag": "text",
                    "text": count_msg
                }
            ]
        ]

    # 获取新增文件数量
    def get_incremental_count(self, temporary_file_path):
        try:

            cmd = f"wc -l {temporary_file_path}"
            output = subprocess.check_output(cmd, shell=True).decode('utf-8')
            count = max([int(lines) for lines in findall('\d+ ', output.strip())])
        except Exception as e:
            print(e)
            count = 0
        return count

    # 下载文件
    def download_file(self, cloud_urls):
        count = 0
        cloud_urls = list(cloud_urls)
        cloud_urls.sort()  # 排序避免多个新文件不按更新顺序下载
        for cloud_url in cloud_urls:
            if self.test_file_upload_completed(cloud_url):  # 检测文件已经上传完成
                file_url = self.get_file_url(cloud_url)  # 获取文件存储目录
                full_file_url, incremental_file_url, temporary_file_path = file_url
                # 下载文件到临时文件夹
                cmd = f"cd {self.path} && ./ossutil64 cp {cloud_url} {temporary_file_path}"
                subprocess.check_output(cmd, shell=True)
                # 获取新增文件数据数量
                count += self.get_incremental_count(temporary_file_path)
                #  拷贝文件到增量库
                cmd = f'mv {os.path.join(temporary_file_path)} {incremental_file_url}'
                subprocess.check_output(cmd, shell=True)
                # 文件序号+1
                self.serial_number = str(int(self.serial_number) + 1).rjust(8, '0')
                print(f'已下载文件：{cloud_url}')
        return count

    # 发送文件统计信息
    def alarm_report(self, messages):
        # webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/86ecc792-3b37-4e0d-8415-5e054f8ee5a2"
        #webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/0bd6d444-a3d2-4036-995d-851882fdb97f"
        headers = {
            "Content-Type": "application/json"
        }
        body = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": f'{self.type_identifier}检测到更新！',
                        "content": messages
                    }
                }
            }
        }
        response = requests.post(url=self.webhook, headers=headers, json=body)
        print(f'{response.status_code} {response.content}')

    # 主函数
    def main(self):
        while True:  # 循环检测更新并下载
            try:
                cloud_urls = self.get_cloud_urls()  # 获取oss文件路径
                if cloud_urls:  # 存在新文件
                    count = self.download_file(cloud_urls)  # 下载文件
                    file_count = self.file_statistics(count)  # 统计文件数量情况
                    self.alarm_report(file_count)  # 发送更新信息
                    self.new_cloud_url = max(cloud_urls)  # 最新的oss文件路径
                sleep(300)  # 休眠300s
            except Exception as e:
                print(e)
                sleep(60)

def process_run(type_identifier):  # 运行不同类型下载
    print(f'start {type_identifier}')
    obj = DetectDownload(type_identifier)
    obj.main()

if __name__ == "__main__":
    path = os.getcwd()
    config = ConfigParser()
    config.read(f'{path}/conf.ini', encoding='utf-8')
    type_identifiers = config.sections()
    type_identifiers.remove('conf')
    processes = [Process(target=process_run, args=(type_identifier,)) for type_identifier in type_identifiers]
    for processe in processes:
        processe.start()
    for processe in processes:
        processe.join()


