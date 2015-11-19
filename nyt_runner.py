""" runs nyt_worker from specified date through 9 days prior (10 total) """

import os
import time
import sys

date = sys.argv[1]
date = time.strptime(date, '%Y%m%d')
date = time.mktime(date)

subtractor = 0
for i in range(10):
	run_date = time.gmtime(date - subtractor)
	run_date = time.strftime('%Y%m%d', run_date)
	subtractor += 86400 #seconds in a day
	print "running nyt_worker for ", run_date
	os.system("python nyt_worker.py " + run_date)