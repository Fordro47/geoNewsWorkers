'''Twitter Worker'''

'''
urls.api.twitter.com/1/urls/count.json?url=http://www.nytimes.com
/reuters/2015/09/01/world/asia/01reuters-ww2-anniversary-taiwan.html
'''
import requests
import datetime
import json
import logging
import traceback


#cc-nebula.cc.gatech.edu/geonewsapi/articles/?date>=    [datetime.datetime.now()-timedelta(days=7)]

logger = logging.getLogger('twitter_worker')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s :: %(message)s')

handler = logging.FileHandler('logs/twitter_worker.log')
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

def getTweetCount(pk, url):
	append = "http://urls.api.twitter.com/1/urls/count.json?url="
	query = append + url
	try:
		response = requests.get(query)
		try:
			responseJSON = response.json()
			try:
				retweetCount = responseJSON['count'] #urllib2.urlopen(query))['count']
				logger.debug(url + ':' + str(retweetCount))
				return retweetCount
			except Exception, e:
				logger.error('Problem getting count from twitter response json\nurl: ' + query + '\nresponse status code: ' + str(response.status_code) + '\nresponse content: ' + response.content)
				logger.exception(e)
				return None
		except Exception, e:
			logger.error('Problem getting json from twitter response\nurl: ' + query + '\nresponse status code: ' + str(response.status_code) + '\nresponse content: ' + response.content)
			logger.exception(e)
			return None
	except Exception, e:
		logger.error('Problem getting response from twitter\nurl: ' + query)
		logger.exception(e)
		return None
	return None

def getUrlsAndPk(articles):
	updatedArticleListSize = 0

	for article in articles:
		try:
			retweetCount = getTweetCount(article['pk'], article['url'])
			if (retweetCount is None):
				logger.error('Problem retrieving retweetcount for article ' + article['url'] + ', skipping article with id ' + str(article['pk']))
				continue
			article['retweetcount'] = retweetCount
			article['retweetcounts'].append({'retweetcount': retweetCount})
			try:
				r = requests.put('http://localhost/geonewsapi/articles/' + str(article['pk'])+'/' , data = json.dumps(article), headers={'content-type':'application/json', 'accept':'application/json'})
				if (r.status_code >= 300 or r.status_code < 200):
					logger.error('Error on Put\n-----------\n--Request--\n-----------\n' + 'http://localhost/geonewsapi/articles/' + str(article['pk'])+'/\n' + json.dumps(article) + '\n------------\n--Response--\n-----------\n' + str(r.status_code) + r.content)#' \nr.content\n'))
				else:
					updatedArticleListSize += 1
			except Exception, e:
				logger.error('Problem getting a response from the backend for url: http://localhost/geonewsapi/articles/' + str(article['pk']))
		except Exception, e:
			traceback.print_exc()

	logger.info(str(updatedArticleListSize) + ' articles successfully updated')
	logger.info('Finish updating Database')

logger.info('Start updating Database')

#get back the date 7 days ago in the format specified
date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
#articles will be an array of 
articles = requests.get('http://localhost/geonewsapi/articles/?format=json&start_date=' + date).json()
logger.info(str(len(articles)) + ' articles retrieved from Database')
getUrlsAndPk(articles)

'''
get all the articles from the database

get the urls from the articles

go through and add that article url to the twitter GET call

store that count with the primary key

POST the pk, count

'''
