# sequential runner for worker
"""
Runs all workers in sequence for yesterday
"""

import os
import time

today = time.time()
yesterday = time.localtime(today - 86400)
yesterday = time.strftime('%Y%m%d', yesterday)

print "yesterday is: ", yesterday

os.system("python nyt_worker.py " + yesterday + " -d")
os.system("python twitter_worker.py")
os.system("python facebook_worker.py")