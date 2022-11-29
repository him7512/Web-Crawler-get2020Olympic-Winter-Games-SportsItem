# Web-Crawler-get2020Olympic-Winter-Games-SportsItem
爬取2020东京奥运会所有比赛信息，并将信息按时间分类以sheet形式存储于Excel

## 小tips：
  2020冬奥会这个网址好像是不限爬的，对新手及其友好
  
  文件中路径均采用相对路径
  
  采用selenium爬取方法，pyCharm貌似可以直接下载，其它则需要网络另行下载运行资源
  
## 文件介绍：
  A 普通单线程爬虫 数据合并一个csv文件
  
  B 生产者消费者模型多线程爬虫 数据合并一个csv文件
  
  C 生产者消费者模型多线程爬虫 将结果按日期分类，并以sheet形式合并一个xlsx文件
