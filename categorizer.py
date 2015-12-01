import json
import requests
import datetime	
import logging
import traceback
import time

log = logging.getLogger('catogorizer')
log.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s :: %(message)s')

fileHandler = logging.FileHandler('logs/categorizer.log')
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(formatter)
log.addHandler(fileHandler)
# streamHandler = logging.StreamHandler()
# streamHandler.setLevel(logging.INFO)
# streamHandler.setFormatter(formatter)
# log.addHandler(streamHandler)

with open('categories.json') as categoriesData:
	categories = json.load(categoriesData)

def getArticles():
	query = 'http://cc-nebula.cc.gatech.edu/geonewsapi/articles/?limit=1000&format=json&ordering=-date'
	offset = 1000
	articles = []
	while(query != None):
		try:
			response = requests.get(query)
			response.encoding = 'utf-8'
			try:
				responseJSON = response.json()
				try:
					articles += responseJSON['results']
					query = responseJSON['next']
					log.info('Loaded through ' + str(offset) + ' of ' + str(responseJSON['count']))
					offset += 1000
				except Exception, e:
					log.error('Problem getting content from server response json\nurl: ' + query + '\nresponse status code: ' + str(response.status_code) + '\nresponse content: ' + response.content.decode('utf-8'))
					log.exception(e)
					return None
			except Exception, e:
				log.error('Problem getting json from server response\nurl: ' + query + '\nresponse status code: ' + str(response.status_code) + '\nresponse content: ' + response.content.decode('utf-8'))
				log.exception(e)
				return None
		except Exception, e:
			log.error('Problem getting response from server\nurl: ' + query)
			log.exception(e)
			return None
	return articles

def categorize(article):
	for category,keywords in categories.iteritems():
		for keyword in keywords:
			if(keyword in article['keywords']):
				return category
	return 'world'

def updateArticles(articles):
	""" Attempts to categorize the articles """
	updatedArticles = []
	
	for article in articles:
		category = categorize(article)
		article["category"] = category
		updatedArticles.append(article)
		log.debug(article["url"]+' : '+category)
	return updatedArticles

def updateDB(articles):
	""" Posts updated articles to db """
	updateCount = 0
	
	for article in articles:
		r = requests.put('http://localhost/geonewsapi/articles/' + str(article['pk'])+'/' , data = json.dumps(article), headers={'content-type':'application/json', 'accept':'application/json'})
		if (200 <= r.status_code <= 299):
			updateCount += 1
		else:
			timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
			log.error('Error on Put\n-----------\n--Request--\n-----------\n' + 'http://localhost/geonewsapi/articles/' + str(article['pk'])+'/\n' + json.dumps(article) + '\n Relevant html file: logs/' + timestamp + ' - ' + article['pk'] + '.html')
			with open('logs/' + timestamp + ' - ' + article['pk'] + '.html') as errorFile:
				errorFile.write(r.content)

articles = getArticles()
if(articles != None):
	log.info("articles pulled: %d\n" % len(articles))

	updatedArticles = updateArticles(articles)
	log.info("articles to update: %d\n" % (len(updatedArticles)))

	updateCount = updateDB(updatedArticles)
	log.info("articles updated: %d\n" % (updateCount))