
import datetime
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
import threading
import queue
import os
import pandas as pd

"""
2022/11/29 14：00 
作者：刘奇

多线程爬取2020东奥项目信息

关键词：selenium、Product/Consumer、csv_sheet

特别说明：
题目要求将sheet保存在csv文件中，但在实际操作中发现csv无法同时存储多个sheet，
尽管在excel中可以临时创建多个sheet，保存退出再打开仍会出现仅最后一个sheet存在的情况，
所以本案例将所有csv文件在excel中进行sheet分类合并，
应该不算做曲解题意。

"""

# 定义函数(基础爬取2020冬奥会运动大项类别很快故不做改变，main中直接调用执行一遍即可)
def getItem(item_queue):
    option1 = webdriver.ChromeOptions()
    option1.add_argument('headless')
    driver1 = webdriver.Chrome(options=option1)
    url = 'https://2020.cctv.com/schedule/item/'
    driver1.get(url)
    item = driver1.find_elements(By.XPATH, '//*[@id="itemcon"]/li/a')
    item = item[1:len(item) - 1]
    for it in item:
        dic = {
            '比赛项目' : it.text,
            '具体项目url' : it.get_attribute("href")
        }
        item_queue.put(dic)
        # print(dic)
    driver1.quit()

# 定义函数(判断某个路径是否存在，若不存在则创建该路径文件夹)
def path_exists_make(path): # path: 需要判断的路径
    if os.path.exists(path):
        pass
    else:
        os.makedirs(path)

# 定义生产者类
class Productor_getInformation(threading.Thread):
    def __init__(self, item_queue, sport_queue):
        threading.Thread.__init__(self)
        self.item_queue = item_queue
        self.sport_queue = sport_queue
        pass
    def run(self):
        while True:
            if self.item_queue.empty():
                break
            """
            可能导致的结果：进程不结束
            如果有某几个进程同时拿取数据，但数据不足以让这些进程均分，这会导致部分进程进入阻塞状态，需要添加try
            """
            try:
                sport = self.item_queue.get(timeout=5)
                option2 = webdriver.ChromeOptions()
                option2.add_argument('headless')
                driver2 = webdriver.Chrome(options=option2)
                sport_name = sport.get('比赛项目')
                sport_url = sport.get('具体项目url')
                driver2.get(sport_url)
                information_time = driver2.find_elements(By.XPATH, '//*[@id="data_list"]/tr/td[1]')
                information_match = driver2.find_elements(By.XPATH, '//*[@id="data_list"]/tr/td[3]')
                information_stadium = driver2.find_elements(By.XPATH, '//*[@id="data_list"]/tr/td[4]')
                for i in range(len(information_time)):
                    dic = {
                        '时间': information_time[i].text,
                        '大项': sport_name,
                        '比赛': information_match[i].text,
                        '场馆': information_stadium[i].text
                    }
                    sport_queue.put(dic)
                    print(dic)
                driver2.quit()
                # self.get_content(classify_2_url, classify_2_name, classify_1_name)
            except:
                print('small Warning！Productor_getInformation {}超时退出'.format(self.name))
                break
    pass

# 创建消费者类
class Consumer_writeInformation(threading.Thread):
    def __init__(self, sport_queue, filename_list):
        threading.Thread.__init__(self)
        self.sport_queue = sport_queue
        self.filename_list = filename_list
        pass
    def run(self):
        while True:
            if self.sport_queue.empty() and switch == 1:
                break
            try:
                # 这个等待时间要比生产者长一些，否则生产者还没生产消费者直接饿死了...
                data = self.sport_queue.get(timeout=15)
                time = data.get('时间')[:2]
                filename_tmp = path + '同日期比赛csv文件/' + time + '日比赛.csv'
                data2 = pd.DataFrame(data, index=[0])
                data2.to_csv(filename_tmp, index=False, mode='a+', header=False, encoding='gb18030')
                print('写入成功!')

            except:
                print('small Warning！Consumer_writeInformation {}超时退出'.format(self.name))
                break
    pass

if __name__ == '__main__':
    start_time = datetime.datetime.now()

    # 爬取文件存储方案
    # 创建日期分类比赛信息csv文件名称
    path = r'2022东奥爬虫爬取比赛数据文件/'
    index = ['时间', '大项', '比赛', '场馆']
    filename_list = []
    for i in range(11):
        filename = str(21 + i) + '日比赛' + '.csv'
        filename_list.append(filename)
    for i in range(8):
        filename = '0' + str(1 + i) + '日比赛' + '.csv'
        filename_list.append(filename)
    # for i in filename_list:
    #     print(i)

    # 创建根据日期分类比赛信息csv文件
    path_exists_make(path + '同日期比赛csv文件/')
    for i in range(19):
        csvfile = open(path + '同日期比赛csv文件/' + filename_list[i], mode='w', newline='')
        write = csv.DictWriter(csvfile, fieldnames=index)
        # 优先填入索引
        write.writeheader()
        csvfile.close()

    # 多线程准备
    switch = 0
    item_queue = queue.Queue(maxsize=50)
    sport_queue = queue.Queue(maxsize=1000)
    getItem(item_queue)

    # 创建生产者表，便于后期加join阻塞
    Productor_getInformation_list = []
    Consumer_writeInformation_list = []

    # 创建5个生产者执行爬取大项分类下相关信息
    for i in range(5):
        p = Productor_getInformation(item_queue, sport_queue)
        p.start()
        Productor_getInformation_list.append(p)
        pass
    # 创建3个消费者执行存储数据
    for i in range(3):
        c = Consumer_writeInformation(sport_queue, filename_list)
        c.start()
        Consumer_writeInformation_list.append(c)
        pass

    """
    阻塞每一个Productor_getInformation线程
    注意：加上这个阻塞则表明主线程执行完全部Productor_getInformation后，再执行下一步
    """
    for p in Productor_getInformation_list:
        p.join()
        pass

    # 当生产者运行结束，更改switch值
    switch = 1

    for c in Consumer_writeInformation_list:
        c.join()
        pass

    # 最后如程序注释中所说无法以sheet形式合并到csv文件中，只能以sheet形式合并到excel中
    # 可优化为相对路径(已优化)
    foldername = '2022东奥爬虫爬取比赛数据文件/同日期比赛csv文件/'
    csvfile_names = os.listdir(foldername)
    # 创建输出表
    writer = pd.ExcelWriter('2022东奥爬虫爬取比赛数据文件/最终爬取数据(sheet合并版).xlsx')
    for file_name in csvfile_names:
        data = pd.read_csv('2022东奥爬虫爬取比赛数据文件/同日期比赛csv文件/' + file_name, encoding='gb18030')
        data.to_excel(writer, file_name, index=False)
    print('数据输出成功')

    # 使用save函数会使得打开的excel携带编码错误需要重新保存数据
    # writer.save()
    writer.close()

    end_time = datetime.datetime.now()
    total_time = end_time - start_time
    print("单线程爬取时间 %s s" % total_time)