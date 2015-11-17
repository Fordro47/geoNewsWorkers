'''Twitter Worker'''

'''
urls.api.twitter.com/1/urls/count.json?url=http://www.nytimes.com
/reuters/2015/09/01/world/asia/01reuters-ww2-anniversary-taiwan.html
'''
import requests
import datetime
import json
import logging


#cc-nebula.cc.gatech.edu/geonewsapi/articles/?date>=    [datetime.datetime.now()-timedelta(days=7)]

logging.basicConfig(filename='twitter_logger_worker',level=logging.DEBUG)

logger = logging.getLogger('twitter_logger_worker')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

articleListSize = 0
updatedArticleListSize = 0

def formatLoggerMessage(msg):
	message = str(msg)
	return str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + ' - ' + message

def getTweetCount(pk, url):

	append = "http://urls.api.twitter.com/1/urls/count.json?url="

	param = append + url
	retweetCount = requests.get(param).json()['count'] #urllib2.urlopen(param))['count']
	logger.debug(formatLoggerMessage(retweetCount))
	#post back to the database with pk, retweet combo
	#json.load(urllib2.urlopen)

	#POST to retweetcounts in the database
	# print(retweetCount)
	return retweetCount

def getUrlsAndPk(articles):
	global updatedArticleListSize

	for article in articles:
		#article = articles[104]
		count = getTweetCount(article['pk'], article['url'])
		article['retweetcount'] = count
		article['retweetcounts'].append({'retweetcount': count})
		# print json.dumps(article)
		r = requests.put('http://cc-nebula.cc.gatech.edu/geonewsapi/articles/' + str(article['pk'])+'/' , data = json.dumps(article), headers={'content-type':'application/json', 'accept':'application/json'})
		if (r.status_code >= 300 or r.status_code < 200):
			logger.error(formatLoggerMessage(str(r.status_code) + ' Error: Put failed at: ' + r.content))#' \nr.content\n'))
		else:
			updatedArticleListSize = updatedArticleListSize + 1

	logger.debug(formatLoggerMessage(str(updatedArticleListSize) + ' articles updated'))
	logger.info(formatLoggerMessage('Finish updating Database'))

logger.info(formatLoggerMessage('Start updating Database'))

#get back the date 7 days ago in the format specified
date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

#articles will be an array of 
articles = requests.get('http://cc-nebula.cc.gatech.edu/geonewsapi/articles/?format=json&startdate=' + date).json()
articleListSize = len(articles)
logger.debug(formatLoggerMessage(str(articleListSize) + ' articles retrieved from Database'))
getUrlsAndPk(articles)




'''
get all the articles from the database

get the urls from the articles

go through and add that article url to the twitter GET call

store that count with the primary key

POST the pk, count

'''
