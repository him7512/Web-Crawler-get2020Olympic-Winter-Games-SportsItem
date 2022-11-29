import os
import queue
import datetime
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By

"""
2022/11/28 15：00 
作者：刘奇

单线程爬取2020东奥项目信息

关键词：selenium

"""

# start_time = time.time()
# start_time = time.perf_counter()
start_time=datetime.datetime.now()

# 第一阶段：获取项目名称以及集体项目url，并且保存到队列

# 开启第一阶段Chrome实例获得所有项目子链接
option1 = webdriver.ChromeOptions()
option1.add_argument('headless')
driver1 = webdriver.Chrome(options=option1)
url = 'https://2020.cctv.com/schedule/item/'
driver1.get(url)
# print(driver.page_source)
# 这里注意itcomeID取法得不到url
# item_name = driver.find_elements(By.ID, 'itemcon')
# for it in item_name:
    # print(it.text)

# 准备队列(为之后改生产者消费者做准备)
item_queue = queue.Queue(maxsize=50)
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

# 第二阶段，提取url并获取该项目全部比赛信息
for i in range(len(item)):
    option2 = webdriver.ChromeOptions()
    option2.add_argument('headless')
    driver2 = webdriver.Chrome(options=option2)
    sport = item_queue.get()
    # print(sport)
    sport_name = sport.get('比赛项目')
    sport_url = sport.get('具体项目url')
    # print(sport_name)
    # print(sport_url)
    driver2.get(sport_url)
    # information = driver2.find_elements(By.XPATH, '//*[@id="data_list"]/tr/td[position()<=4]')
    # information = driver2.find_elements(By.XPATH, '//*[@id="data_list"]/tr/td')
    # information = driver2.find_elements(By.ID, 'data_list')
    information_time = driver2.find_elements(By.XPATH, '//*[@id="data_list"]/tr/td[1]')
    # information_sport = driver2.find_elements(By.XPATH, '//*[@id="data_list"]/tr/td[2]')
    information_match = driver2.find_elements(By.XPATH, '//*[@id="data_list"]/tr/td[3]')
    information_stadium = driver2.find_elements(By.XPATH, '//*[@id="data_list"]/tr/td[4]')
    sport_queue = queue.Queue(maxsize=1000)
    # for inf in information_time:
    #     print(inf.text)
    # print("========")
    # # for inf in information_sport:
    # #     print(inf.text)
    # # print("========")
    # for inf in information_match:
    #     print(inf.text)
    # print("========")
    # for inf in information_stadium:
    #     print(inf.text)
    for i in range(len(information_time)):
        # x = information_time[i].text
        # print(x)
        # y = information_match[i].text
        # print(y)
        # z = information_stadium[i].text
        dic = {
            '时间' : information_time[i].text,
            '大项' : sport_name,
            '比赛' : information_match[i].text,
            '场馆' : information_stadium[i].text
        }
        print(dic)
        sport_queue.put(dic)
    driver2.quit()




# 将数据写入csv
path = r'csv文件'
os.makedirs(path)

# data = sport_queue.get()
# time = data.get('时间')
# print(time)



"""
相较于下面哪种写法，该循环with方法会导致csv文件中出现空行
"""
# with open(filename,'w') as csvfile:
#     fieldnames = fieldnames = ['时间', '大项', '比赛', '场馆']
#     writer=csv.DictWriter(csvfile,fieldnames=fieldnames)
#     writer.writeheader()
#     for i in range(10):
#         data = sport_queue.get()
#         writer.writerow(data)
#     # csvfile.close()

# 2020冬奥会时间7/21-8/8
filename_list = []
for i in range(11):
    filename = path + '/' + '7月' + str(21 + i) + '日' + '.csv'
    filename_list.append(filename)
for i in range(8):
    filename = path + '/' + '8月' + str(1 + i) + '日' + '.csv'
    filename_list.append(filename)
# for i in filename_list:
#     print(i)


csvfile = open(filename, mode='w', newline='')
fieldnames = ['时间', '大项', '比赛', '场馆']


# time = data.get('时间')


# 创建 DictWriter 对象
write = csv.DictWriter(csvfile, fieldnames=fieldnames)
# 写入表头
write.writeheader()
while True:
    if sport_queue.empty():
        break
    data = sport_queue.get()
    time = data.get('时间')
    write.writerow(data)


# time.sleep(1)
# end_time = time.time()
end_time = datetime.datetime.now()
total_time = end_time - start_time
# total_time = 100 - start_time
print("单线程爬取时间 %s s" % total_time)


