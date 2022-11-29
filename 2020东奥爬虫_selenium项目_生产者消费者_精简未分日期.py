
import datetime
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
import threading
import queue
import os

"""
2022/11/28 19：00 
作者：刘奇

多线程爬取2020东奥项目信息

关键词：selenium、Product/Consumer

"""

# 定义函数(基础爬取新大项类别很快不做改变)
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
            如果有某几个进程同时取数据，但是数据不足以让这些进程同时取，这会导致部分进程进入阻塞状态，需要添加try
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
                data = self.sport_queue.get(timeout=15)
                # csvfile = open(filename, mode='w', newline='')
                # fieldnames = ['时间', '大项', '比赛', '场馆']
                # # 创建 DictWriter 对象
                # write = csv.DictWriter(csvfile, fieldnames=fieldnames)
                # write.writeheader()
                time = data.get('时间')
                write.writerow(data)
                print("写入成功!")

            except:
                print('small Warning！Consumer_writeInformation {}超时退出'.format(self.name))
                break
    pass







if __name__ == '__main__':
    start_time = datetime.datetime.now()

    path = r'csv文件'
    os.makedirs(path)
    filename_list = []
    for i in range(11):
        filename = path + '/' + '7月' + str(21 + i) + '日' + '.csv'
        filename_list.append(filename)
    for i in range(8):
        filename = path + '/' + '8月' + str(1 + i) + '日' + '.csv'
        filename_list.append(filename)

    csvfile = open(filename, mode='w', newline='')
    fieldnames = ['时间', '大项', '比赛', '场馆']
    # 创建 DictWriter 对象
    write = csv.DictWriter(csvfile, fieldnames=fieldnames)
    write.writeheader()

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

    end_time = datetime.datetime.now()
    total_time = end_time - start_time
    print("单线程爬取时间 %s s" % total_time)
