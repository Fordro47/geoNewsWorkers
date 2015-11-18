# Facebook Worker

# Facebook worker for shares, likes, comments and clicks 

import requests
import datetime
import json
import logging
import sys

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
def populateFacebookCounts(pk, url, article):

	# Get Query in correct Format for pulling data

	append = "http://api.facebook.com/method/fql.query?query=select%20total_count,like_count,comment_count,share_count,click_count%20from%20link_stat%20where%20url=%27"
	appendEnd = "%27&format=json"
	query = append + url + appendEnd

	response = requests.get(query).json()[0]
	# logger.debug(formatLoggerMessage(response))

	shareCount = response['share_count']
	likeCount = response['like_count']
	commentCount = response['comment_count']
	clickCount = response['click_count']

	logger.debug(url + ': share-' + str(shareCount) + 'like-' + str(likeCount) + 'comment-' + str(commentCount) + 'click-' + str(clickCount))

	article['facebookcounts'].append({'sharecount': shareCount, 'likecount' : likeCount, 'commentcount' : commentCount, 'clickcount': clickCount})
	article['sharecount'] = shareCount
	# article['likecounts'].append({'likecount': likeCount})
	# article['commentcounts'].append({'commentcount': commentCount})
	# article['clickcounts'].append({'clickcount': clickCount})
	return response

def getUrlsAndPk(articles):
	updatedArticleListSize = 0

	for article in articles:
		try:
			fb_req = populateFacebookCounts(article['pk'], article['url'], article)
			r = requests.put('http://cc-nebula.cc.gatech.edu/geonewsapi/articles/' + str(article['pk'])+'/' , data = json.dumps(article), headers={'content-type':'application/json', 'accept':'application/json'})
			if (r.status_code >= 300 or r.status_code < 200):
				logger.error('Error on Put\n-----------\n--Request--\n-----------\n' + 'http://localhost/geonewsapi/articles/\n' + str(article['pk'])+'/' + json.dumps(article) + '\n------------\n--Response--\n-----------\n' + str(r.status_code) + r.content)
			else:
				updatedArticleListSize +=1
		except:
			print(sys.exc_info()[0])

	logger.info(str(updatedArticleListSize) + ' articles successfully updated')
	logger.info('Finish updating Database')

# //Add start run

logger.info('Start updating Database')

#get back the date 7 days ago in the format specified
date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

#articles will be an array of article
articles = requests.get('http://cc-nebula.cc.gatech.edu/geonewsapi/articles/?format=json&start_date=' + date).json()
logger.info(str(len(articles)) + ' articles retrieved from Database')
getUrlsAndPk(articles)


# https://api.facebook.com/method/fql.query?query=select%20total_count,like_count,
# comment_count,share_count,click_count%20from%20link_stat%20where%20url=%27
# http://www.nytimes.com/2015/11/06/us/louisiana-police-shooting-marksville.html       -> this is the address appended to the param
# %27&format=json
