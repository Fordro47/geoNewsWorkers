# Facebook Worker

# Facebook worker for shares, likes, comments and clicks 

import requests
import datetime
import json
import logging

logging.basicConfig(filename='')

logger = logging.getLogger('facebook_logger')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

# logging.basicConfig(filename='facebook_worker_log.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
# logging.debug('Facebook Worker Logging File')
# logging.info('This file will keep track of the workers status')

articleListSize = 0
updatedArticleListSize = 0

#cc-nebula.cc.gatech.edu/geonewsapi/articles/?date>=    [datetime.datetime.now()-timedelta(days=7)]


# This function takes in a primary key and a URL from an article, 
# calls facebook's data on that data and then adds all the information
# for that article in the database fields. 
def formatLoggerMessage(msg):
	message = str(msg)
	return str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + ' - ' + message

def populateFacebookCounts(pk, url, article):

	# Get Query in correct Format for pulling data

	append = "https://api.facebook.com/method/fql.query?query=select%20total_count,like_count,comment_count,share_count,click_count%20from%20link_stat%20where%20url=%27"
	appendEnd = "%27&format=json"
	query = append + url + appendEnd

	response = requests.get(query).json()[0]
	logger.debug(formatLoggerMessage(response))

	shareCount = response['share_count']
	likeCount = response['like_count']
	commentCount = response['comment_count']
	clickCount = response['click_count']

	article['facebookcounts'].append({'sharecount': shareCount, 'likecount' : likeCount, 'commentcount' : commentCount, 'clickcount': clickCount})
	article['sharecount'] = shareCount
	# article['likecounts'].append({'likecount': likeCount})
	# article['commentcounts'].append({'commentcount': commentCount})
	# article['clickcounts'].append({'clickcount': clickCount})

def getUrlsAndPk(articles):

	for article in articles:
		populateFacebookCounts(article['pk'], article['url'], article)
		r = requests.put('http://cc-nebula.cc.gatech.edu/geonewsapi/articles/' + str(article['pk'])+'/' , data = json.dumps(article), headers={'content-type':'application/json', 'accept':'application/json'})
		if (r.status_code >= 300 || r.status_code < 200):
			print("put failed\nr.content\n")
			logger.error(formatLoggerMessage(r.status_code + ' Error: Put failed at \nr.content\n'))
		else:
			updatedArticleListSize++

	logger.debug(formatLoggerMessage(updatedArticleListSize + ' articles updated'))
	logger.info(formatLoggerMessage('Finish updating Database'))

# //Add start run

logger.info(formatLoggerMessage('Start updating Database'))

#get back the date 7 days ago in the format specified
date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

#articles will be an array of article
articles = requests.get('http://cc-nebula.cc.gatech.edu/geonewsapi/articles/?format=json&enddate=' + date).json()
articleListSize = len(articles)
logger.debug(formatLoggerMessage(articleListSize + ' articles retrieved from Database'))
getUrlsAndPk(articles)


# https://api.facebook.com/method/fql.query?query=select%20total_count,like_count,
# comment_count,share_count,click_count%20from%20link_stat%20where%20url=%27
# http://www.nytimes.com/2015/11/06/us/louisiana-police-shooting-marksville.html       -> this is the address appended to the param
# %27&format=json
