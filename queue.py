#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2 as pg2
import re
import time
import ConfigParser
pg2.extensions.register_type(pg2.extensions.UNICODE)
pg2.extensions.register_type(pg2.extensions.UNICODEARRAY)

#~ Прочитать конфигурационный файл и присвоить переменные
conf = ConfigParser.ConfigParser()
conf.read('config.cfg')
host = conf.get('database', 'host')
dbname = conf.get('database', 'dbname')
user = conf.get('database', 'user')
password = conf.get('database', 'password')
connect_params = "host=%s dbname=%s user=%s password=%s" % (host, dbname, user, password)

def GetInHMS(seconds):
	"""
	Получает секунды и преобразовывает это в формат HH:MM:SS
	"""
	hours = seconds/3600
	seconds -= hours*3600
	minutes = seconds/60
	seconds -= minutes*60
	if hours == 0:
		return "%02d:%02d" % (minutes, seconds)
	return "%02d:%02d:%02d" % (hours, minutes, seconds)

def WaitTime(timer):
	"""
	Вычисляет разницу между локальным unixtime и приходящим
	значением unixtime (предположительно из БД). Вывод форматируется
	с помощью функции GetInHMS.
	ПыСы: Рассчитано на то, что будет работать прямо на хосте с БД,
	так что будет показывать реальную разницу в секундах.
	"""
	current_time = int(time.time())
	wait_time = current_time - int(timer)
	return GetInHMS(wait_time)

con = pg2.connect(connect_params)
cur = con.cursor()

SQL = """
SELECT data, time FROM
	(SELECT * FROM telephony_queuelog AS t1
		WHERE t1.event = 'ENTERQUEUE'
		AND t1.queuename = 'internet'
		AND t1.callid NOT IN (
			SELECT t2.callid FROM telephony_queuelog AS t2
				WHERE t2.event <> 'ENTERQUEUE'
				AND t2.queuename = 'internet'
			ORDER BY t2.id DESC
			LIMIT 1000
	)
	ORDER BY t1.id DESC
	LIMIT 1000) AS tt
		WHERE tt.time::INT > EXTRACT(epoch FROM NOW())-3600 ORDER BY tt.callid ASC;
"""

cur.execute(SQL)
results = cur.fetchall()
regex = re.compile('^\|')
pos = 1
for i in range(len(results)):
	dbtime = results[i][1]
	timer = WaitTime(dbtime)
	number = regex.sub('', results[i][0])
	print "%s: %s (wait %s)" % (pos, number, timer)
	pos += 1