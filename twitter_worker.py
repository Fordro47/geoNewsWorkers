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

logger = logging.getLogger('twitter_worker')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s :: %(message)s')

handler = logging.FileHandler('logs/twitter_worker.log')
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

articleListSize = 0
updatedArticleListSize = 0

def getTweetCount(pk, url):

	append = "http://urls.api.twitter.com/1/urls/count.json?url="

	param = append + url
	retweetCount = requests.get(param).json()['count'] #urllib2.urlopen(param))['count']
	logger.debug(url + ':' + str(retweetCount))
	#post back to the database with pk, retweet combo
	#json.load(urllib2.urlopen)

	#POST to retweetcounts in the database
	# print(retweetCount)
	return retweetCount

def getUrlsAndPk(articles):
	global updatedArticleListSize = 0

	for article in articles:
		#article = articles[104]
		count = getTweetCount(article['pk'], article['url'])
		article['retweetcount'] = count
		article['retweetcounts'].append({'retweetcount': count})
		# print json.dumps(article)
		r = requests.put('http://localhost/geonewsapi/articles/' + str(article['pk'])+'/' , data = json.dumps(article), headers={'content-type':'application/json', 'accept':'application/json'})
		if (r.status_code >= 300 or r.status_code < 200):
			logger.error(formatLoggerMessage(str(r.status_code) + ' Error: Put failed at: ' + r.content))#' \nr.content\n'))
		else:
			updatedArticleListSize += 1

	logger.info(str(updatedArticleListSize) + ' articles successfully updated')
	logger.info('Finish updating Database')

logger.info('Start updating Database')

#get back the date 7 days ago in the format specified
date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

#articles will be an array of 
articles = requests.get('http://localhost/geonewsapi/articles/?format=json&startdate=' + date).json()
articleListSize = len(articles)
logger.info(str(articleListSize) + ' articles retrieved from Database')
getUrlsAndPk(articles)




'''
get all the articles from the database

get the urls from the articles

go through and add that article url to the twitter GET call

store that count with the primary key

POST the pk, count

'''
