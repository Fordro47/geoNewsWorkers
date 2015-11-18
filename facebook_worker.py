# Facebook Worker

# Facebook worker for shares, likes, comments and clicks 

import requests
import datetime
import json
import logging
import traceback

logger = logging.getLogger('facebook_worker')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s :: %(message)s')

handler = logging.FileHandler('logs/facebook_worker.log')
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

#cc-nebula.cc.gatech.edu/geonewsapi/articles/?date>=    [datetime.datetime.now()-timedelta(days=7)]


# This function takes in a primary key and a URL from an article, 
# calls facebook's data on that data and then adds all the information
# for that article in the database fields.
def getFacebookCounts(pk, url):

	# Get Query in correct Format for pulling data

	append = "http://api.facebook.com/method/fql.query?query=select%20total_count,like_count,comment_count,share_count,click_count%20from%20link_stat%20where%20url=%27"
	appendEnd = "%27&format=json"
	query = append + url + appendEnd

	try:
		response = requests.get(query)
		try:
			responseJSON = response.json()
			try:
				facebookCounts = {}
				responseCounts = responseJSON[0]
				facebookCounts['share_count'] = responseCounts['share_count']
				facebookCounts['like_count'] = responseCounts['like_count']
				facebookCounts['comment_count'] = responseCounts['comment_count']
				facebookCounts['click_count'] = responseCounts['click_count']
				logger.debug(url + ': share-' + str(facebookCounts['share_count']) + 'like-' + str(facebookCounts['like_count']) + 'comment-' + str(facebookCounts['comment_count']) + 'click-' + str(facebookCounts['click_count']))
				return facebookCounts
			except Exception, e:
				logger.error('Problem getting counts from facebook response json\nurl: ' + query + '\nresponse status code: ' + response.status_code + '\nresponse content: ' + response.content)
				logger.exception(e)
				return None
		except Exception, e:
			logger.error('Problem getting json from facebook response\nurl: ' + query + '\nresponse status code: ' + response.status_code + '\nresponse content: ' + response.content)
			logger.exception(e)
			return None
	except Exception, e:
		logger.error('Problem getting response from facebook\nurl: ' + query)
		logger.exception(e)
		return None
	return None

def getUrlsAndPk(articles):
	updatedArticleListSize = 0

	for article in articles:
		try:
			facebookCounts = getFacebookCounts(article['pk'], article['url'])
			if (facebookCounts is None):
				logger.error('Problem retrieving facebookcounts for article ' + article['url'] + ', skipping article with id ' + str(article['pk']))
				continue
			article['facebookcounts'].append({'sharecount': facebookCounts['share_count'], 'likecount' : facebookCounts['like_count'], 'commentcount' : facebookCounts['comment_count'], 'clickcount': facebookCounts['click_count']})
			article['sharecount'] = facebookCounts['share_count']
			try:
				r = requests.put('http://localhost/geonewsapi/articles/' + str(article['pk'])+'/' , data = json.dumps(article), headers={'content-type':'application/json', 'accept':'application/json'})
				if (r.status_code >= 300 or r.status_code < 200):
					logger.error('Error on Put\n-----------\n--Request--\n-----------\n' + 'http://localhost/geonewsapi/articles/' + str(article['pk'])+'/\n' + json.dumps(article) + '\n------------\n--Response--\n-----------\n' + str(r.status_code) + r.content)
				else:
					updatedArticleListSize +=1
			except Exception, e:
				logger.error('Problem getting a response from the backend for url: http://localhost/geonewsapi/articles/' + str(article['pk']))
		except Exception, e:
			traceback.print_exc()

	logger.info(str(updatedArticleListSize) + ' articles successfully updated')
	logger.info('Finish updating Database')

# //Add start run

logger.info('Start updating Database')

#get back the date 7 days ago in the format specified
date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

#articles will be an array of article
articles = requests.get('http://localhost/geonewsapi/articles/?format=json&start_date=' + date).json()
logger.info(str(len(articles)) + ' articles retrieved from Database')
getUrlsAndPk(articles)


# https://api.facebook.com/method/fql.query?query=select%20total_count,like_count,
# comment_count,share_count,click_count%20from%20link_stat%20where%20url=%27
# http://www.nytimes.com/2015/11/06/us/louisiana-police-shooting-marksville.html       -> this is the address appended to the param
# %27&format=json
