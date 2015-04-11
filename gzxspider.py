# -*- coding: utf-8 -*-
import threadmanager
import miniweb
import re
import sqlite3
import time
import logging
import sys
import math
from urllib.parse import urljoin
from queue import Queue
from bs4 import BeautifulSoup
from threading import Thread
from datetime import datetime
from datetime import timedelta

class GzxSpider(object):
	'''爬虫类，负责分析抓取网页
		以起始URL（startUrl）开始，爬取指定的深度（depth），支持线程池，支持检索关键字
	'''
	def __init__(self, startUrl, depth, threadNum, dbFile = 'htmldb.db3', keys = ''):
		super(GzxSpider, self).__init__()
		self.logger = logging.getLogger('GzxSpider')
		# 起始URL
		self.startUrl = startUrl
		# 需要检查分析处理的URL队列
		self.urlQueue = Queue()
		self.urlQueue.put(startUrl)
		# 需要保存的HTML队列
		self.saveHtmlQueue = Queue()
		# 已经处理过的URL队列
		self.appearedUrls = []
		# 下一层需要分析处理的URL临时存储
		self.nextLevelUrls = []
		# 爬取深度
		self.depth = depth
		# 当前深度
		self.currentDepth = 1
		# 线程池线程数
		self.threadNum = threadNum
		# 检索关键字
		self.keys = keys
		# 创建线程池
		self.threadPool = threadmanager.ThreadManager(self.threadNum)
		# 满足关键字页面个数
		self.hasKeysUrlNum = 0
		# 已经处理的URL个数
		self.processUrlNum = 0
		# 爬虫状态，未启动为False
		self.status = False
		# 整个爬取过程消耗的时间
		self.timeElapsed = 0
		self.database = SaveHtmlToDB(dbFile, self.saveHtmlQueue)
		# 后续考虑支持传入方法或类，使用者决定如何处理满足条件的页面
		self.save = self.database.saveHtml

	def start(self):
		'''启动爬虫'''
		self.starttime = datetime.now()
		self.status = True
		self.logger.info('GzxSpider Start. [StartUrl: %s, ThreadNum: %d, Depth: %d, Keys: %s]' % (self.startUrl, self.threadNum, self.depth, self.keys))
		self.appearedUrls.append(self.startUrl)
		tempDepth = self.currentDepth
		while tempDepth <= self.depth:
			while not self.urlQueue.empty():
				url = self.urlQueue.get()
				self.threadPool.addTask(self.processUrl, url, tempDepth == self.depth)

			while tempDepth != self.depth:
				if self.threadPool.isAllThreadNoTask():
					for url in self.nextLevelUrls:
						self.urlQueue.put(url)
					self.nextLevelUrls.clear()
					self.currentDepth += 1
					break
			self.save()
			tempDepth += 1

		while True:
			if self.threadPool.isAllThreadNoTask():
				self.save()
				self.threadPool.stopThreads(len(self.threadPool.threadPool))
				self.database.close()
				break
		self.status = False
		self.timeElapsed = convertSecond((datetime.now() - self.starttime).total_seconds())
		self.logger.info('GzxSpider Stop. [Process Url Number: %d, Has Keys Url Number: %d, Keys: %s, Depth: %d, Time Elapsed: %d hours %d minutes %d seconds]' %
			(self.processUrlNum, self.hasKeysUrlNum, self.keys, self.currentDepth, self.timeElapsed['days'] * 24 + self.timeElapsed['hours'],
				self.timeElapsed['minutes'], self.timeElapsed['seconds']))

	def processUrl(self, *urlTuple):
		'''URL处理方法，获取当前页面URL，检索当前页面是否有关键字'''
		self.processUrlNum += 1
		urlStr, isEndLevel = urlTuple
		respData = miniweb.getResponseData(urlStr)
		if not isEndLevel:
			soup = BeautifulSoup(respData)
			addressList = soup.find_all('a', href = re.compile('^http|^/'))
			for address in addressList:
				if address['href'].startswith('/'):
					address['href'] = urljoin(urlStr, address['href'])
				if address['href'] not in self.appearedUrls:
					self.appearedUrls.append(address['href'])
					self.nextLevelUrls.append(address['href'])
		self.findHasKeysHtml(urlStr, respData)

	def findHasKeysHtml(self, urlStr, htmlData):
		'''检索页面是否有关键字，用户未设定关键字，直接将页面放入待保存队列'''
		if self.keys:
			patternKey = re.compile('|'.join(self.keys.split()))
			soup = BeautifulSoup(htmlData)
			if soup.find('meta', content = patternKey):
				self.saveHtmlQueue.put((urlStr, self.keys, htmlData.decode()))
				self.hasKeysUrlNum += 1
		else:
			self.saveHtmlQueue.put((urlStr, '', htmlData.decode()))
			self.hasKeysUrlNum += 1


class SaveHtmlToDB(object):
	'''保存页面到数据的列'''
	def __init__(self, dbFile, htmlQueue):
		super(SaveHtmlToDB, self).__init__()
		self.htmlQueue = htmlQueue
		#数据库创建链接
		self.conn = sqlite3.connect(dbFile)
		self.cmd = self.conn.cursor()
		self.cmd.execute('''
			create table if not exists htmls(
				id integer primary key autoincrement,
				url text,
				keys text,
				html text
				)
		''')
		self.conn.commit()

	def saveHtml(self):
		'''保存页面方法'''
		while not self.htmlQueue.empty():
			url, keys, html = self.htmlQueue.get()
			self.cmd.execute("insert into htmls (url,keys,html) values (?,?,?)",(url,keys,html))
			self.conn.commit()
	
	def close(self):
		'''关闭数据库连接方法'''
		self.conn.close()


class ShowRuningInfo(Thread):
	'''显示爬虫爬取状态信息列'''
	def __init__(self, spider, sleepTime = 10):
		super(ShowRuningInfo, self).__init__()
		self.spider = spider
		self.sleepTime = sleepTime
		self.isShow = True

	def run(self):
		while self.spider.status or self.isShow:
			self.showInfo()
			time.sleep(self.sleepTime)
		self.showInfo()
		print('【Time Elapsed : %d H %d M %d S】' % 
				(self.spider.timeElapsed['days'] * 24 + self.spider.timeElapsed['hours'],
				self.spider.timeElapsed['minutes'], self.spider.timeElapsed['seconds']))

	def showInfo(self):
		'''打印信息方法'''
		currentDepthLine = 'Current Depth       : %d' % self.spider.currentDepth
		threadNumLine = 'Thread Number       : %d' % len(self.spider.threadPool.threadPool)
		searchKeysLine = 'Search Keys         : %s' % self.spider.keys
		hasKeysUrlNumLine = 'Has Keys Url Number : %d' % self.spider.hasKeysUrlNum
		processUrlLine = 'Process Url Number  : %d' % self.spider.processUrlNum
		searchKeysLineByteLength = len(searchKeysLine.encode('utf-8'))
		lineLength = max(len(currentDepthLine), len(threadNumLine), searchKeysLineByteLength, 
					len(hasKeysUrlNumLine), len(processUrlLine)) + 1
		print('*'.ljust(lineLength + 4, '*'))
		print(' ', currentDepthLine.ljust(lineLength))
		print(' ', threadNumLine.ljust(lineLength))
		print(' ', searchKeysLine.ljust(lineLength))
		print(' ', hasKeysUrlNumLine.ljust(lineLength))
		print(' ', processUrlLine.ljust(lineLength))
		print('*'.ljust(lineLength + 4, '*'))

def initMyLogger(logname = 'spider.log'):
	'''初始化日志方法'''
	logger = logging.getLogger('GzxSpider')
	logger.setLevel(logging.INFO)
	fileFormatter = logging.Formatter('%(asctime)s <%(levelname)s> [%(module)s] %(message)s.') 
	fileHandler = logging.FileHandler(logname)
	fileHandler.setFormatter(fileFormatter)
	logger.addHandler(fileHandler)

def convertSecond(second):
	'''将秒数转换为日，时，分，秒'''
	timeValDict = {'days' : 0, 'hours' : 0, 'minutes' : 0, 'seconds' : 0}
	second = math.floor(second)
	if second > 0:
		minuteSecond = 60
		hourSecond = minuteSecond * 60
		daySecond = hourSecond * 24
		if second >= daySecond:
			timeValDict['days'] = math.floor(second / daySecond)
			second = second % daySecond
		if second >= hourSecond:
			timeValDict['hours'] = math.floor(second / hourSecond)
			second = second % hourSecond
		if second >= minuteSecond:
			timeValDict['minutes'] = math.floor(second / minuteSecond)
			second = second % minuteSecond
		timeValDict['seconds'] = second
	return timeValDict


if __name__ == '__main__':
	patternUrl = re.compile(r'^http[s]?://([a-zA-Z0-9.\-]+/?)+([^/].)*')
	# 起始URL输入
	while True:
		startUrl = input('Input Start Url>>')
		startUrl = startUrl.strip()
		if len(startUrl) > 2048:
			print('Url Length Must Less Than 2048')
		tempMatch = re.match(patternUrl, startUrl)
		if tempMatch is not None and len(startUrl) == len(tempMatch.group()):
			break
		else:
			print('You Input String Is Not Url.')

	# 爬取深度输入
	while True:
		depth = input('Input Scan Depth>>')
		depth = depth.strip()
		if not depth.startswith('0') and depth.isdecimal():
			depth = int(depth)
			break
		print('Please Input An Integer And Greater Than Zero')

	# 日志名称输入
	patternFileName = re.compile(r'[a-zA-Z0-9_\-.]+')
	MAX_FILE_NAME = 32
	while True:
		logfile = input('Input Log File Name(Default Is spider.log)>>')
		logfile = logfile.strip()
		if len(logfile) == 0:
			logfile = 'spider.log'
			break
		elif len(logfile) > MAX_FILE_NAME:
			print('Max Log File Name Is %d' % MAX_FILE_NAME)
			continue
		tempMatch = re.match(patternFileName, logfile)
		if tempMatch is not None and len(logfile) == len(tempMatch.group()):
			break
		else:
			print('You Input Log File Name Has Illegal Character')

	# 线程数输入
	MAX_THREAD = 500
	while True:
		threadNum = input('Input Thread Number(Default Is 10, Max Is %d)>>' % MAX_THREAD)
		threadNum = threadNum.strip()
		if len(threadNum) == 0:
			threadNum = 10
			break
		elif not threadNum.startswith('0') and threadNum.isdecimal():
			threadNum = int(threadNum)
			if threadNum > MAX_THREAD:
				print('Max Thread Number Is %d' % MAX_THREAD)
				continue
			else:
				break
		else:
			print('Please Input An Integer And Greater Than Zero')

	# 数据库文件名输入
	while True:
		dbfile = input('Input Database File Name(Default Is htmldb.db3, Max Length Is %d)>>' % MAX_FILE_NAME)
		dbfile = dbfile.strip()
		if len(dbfile) == 0:
			dbfile = 'htmldb.db3'
			break
		elif len(dbfile) > MAX_FILE_NAME:
			print('Max File Name Length Is %d' % MAX_FILE_NAME)
			continue
		tempMatch = re.match(patternFileName, dbfile)
		if tempMatch is not None and len(dbfile) == len(tempMatch.group()):
			break
		else:
			print('You Input Database File Name Has Illegal Character')

	# 关键字输入
	MAX_KEY = 64
	while True:
		keys = input('Input Keys(Default No Keys, Key Separated By Space)>>')
		keys = keys.strip()
		if len(keys) == 0:
			break
		elif len(keys) > MAX_KEY:
			print('Inpurt String Length Is %d' % MAX_KEY)
			continue

		patternKeys = re.compile(r'[\w ]+')
		tempMatch = re.match(patternKeys, keys)
		if tempMatch is not None and len(keys) == len(tempMatch.group()):
			break
		else:
			print('You Input Keys Has Illegal Character')

	# 是否启动爬虫
	while True:
		isRun = input('Start GzxSpider?(y/n)>>')
		isRun = isRun.strip()
		if isRun == 'y':
			break
		elif isRun == 'n':
			try:
				sys.exit(0)
			except SystemExit:
				pass
			finally:
				break
	if isRun == 'y':
		initMyLogger(logfile)
		myspider = GzxSpider(startUrl, depth, threadNum, dbfile, keys)
		runingInfo = ShowRuningInfo(myspider, 5)
		runingInfo.start()
		try:
			myspider.start()
		except Exception as e:
			myspider.logger.critical(repr(e))
		finally:
			myspider.status = False
			runingInfo.isShow = False
			runingInfo.join()
			print("***Scan Finished***")
