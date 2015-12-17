import sys
import json
import requests
import time
import logging
import traceback
import pprint
from nytimesarticle import articleAPI
from pygeocoder import Geocoder

#System arguments #######################################################
date = sys.argv[1]
debugging = False

#Variables #############################################################
api = articleAPI("af0ead0b339871714bd8718ac007283b:11:73169680")

submitted = 0
duplicates = 0
coordCount = 0
multimediaCount = 0
log = logging.getLogger('nyt_worker')
log.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s :: %(message)s')

fileHandler = logging.FileHandler('logs/nyt_log.log')
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(formatter)
log.addHandler(fileHandler)

if (len(sys.argv) == 3 and str(sys.argv[2]).lower() == "-d"):
    debugging = true
    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(logging.DEBUG)
    streamHandler.setFormatter(formatter)
    log.addHandler(streamHandler)
    log.debug("debugging enabled")

log.info("nyt_worker called with: " + date);

with open('categories.json') as categoriesData:
	categories = json.load(categoriesData)

#initializes end date by taking date and adding 1 day (86400 seconds)
endDateStruct = time.strptime(date, '%Y%m%d')
timeInSeconds = time.mktime(endDateStruct)
endDateTuple = time.gmtime(timeInSeconds + 86400)
endDate = time.strftime('%Y%m%d', endDateTuple)
####################################################################

def getArticles(date):
	"""
	This function returns a list of 100 dictionaries containing
	NYT articles from a specified date
	"""
	articleList = []

	#NYT by default only returns 1 of 100 pages containing the first 10 results
	for i in range(100):
		articleList.append(api.search(begin_date = date, end_date = endDate, sort = "newest", fl = ["byline", "headline", "keywords", "web_url", "pub_date", "lead_paragraph", "news_desk", "_id", "source", "multimedia"], facet_field = ["section_name"], page = i + 1))
		time.sleep(.1)
	return (articleList)

def parseArticles(articles):
    """
    This function takes in a response to the NYT api and stores
    the articles into a list of dictionaries

    modified from: http://dlab.berkeley.edu/blog/scraping-new-york-times-articles-python-tutorial
    """
    news = []
    for i in articles['response']['docs']:
        dic = {}
        dic['author'] = i['byline']
        dic['headline'] = i['headline']
        dic['lead_paragraph'] = i['lead_paragraph']
        dic['keywords'] = i['keywords']
        dic['url'] = i['web_url']
        dic['date'] = i['pub_date']
        dic['news_desk'] = i['news_desk']
        dic['id'] = i['_id']
        dic['source'] = i['source']
        dic['multimedia'] = i['multimedia']

        log.debug("adding news from: " + i['web_url'])

        news.append(dic)

    log.debug("articles in page: " + len(news))
    return(news)

def parseArticleList(articleList):
	"""
	This function takes in a list of responses from the NYT api
	and returns a list of dictionaries
	"""
	news = []
	i = 0
	while i < len(articleList):

		log.debug("adding articles from page: " + str(i + 1))

		news.extend(parseArticles(articleList[i]))
		i += 1
	return(news)

def parseByline(author):
	"""
	Takes author key from dictionary and
	returns what comes after "BY:"
	"""
	if (author != None and 'original' in author and len(author['original']) > 0):
		original = author['original']
		return original[3:]
	else:
		return None

def parseAuthors(author):
	"""
	Takes author key and
	returns a list of dictionaries of first and last names
	"""
	authorList = []
	i = 0
	if (author != None and 'person' in author):
		while (i < len(author['person'])):
			authorDict = {}
			if ('firstname' in author['person'][i]):
				authorDict['first'] = author['person'][i]['firstname']
			else:
				authorDict['first'] = None
			if ('lastname' in author['person'][i]):
				authorDict['last'] = author['person'][i]['lastname']
			else:
				authorDict['last'] = None
			authorList.append(authorDict)
			i+=1
		return authorList
	else:
		return None

def parseTitle(headline):
	"""
	Takes title key from dictionary and
	returns the title
	"""
	if 'main' in headline:
		return (headline['main'])
	elif 'name' in headline:
		return (headline['name'])
	else:
		return ""

def parseLeadParagraph(lead_paragraph):
	"""
	Takes lead_paragraph key from dictionary and
	returns the lead_paragraph
	"""
	if (lead_paragraph != None):
		return (lead_paragraph)
	else:
		return ""

def parseLocation(address):
	"""
	takes in an address, decodes it with Geocoder,
	and returns a set of coordinates
	"""
	coords = {}
	coords['type'] = "Point"
	log.debug("parseLocation() received: " + address)
	if (address != None):
		latitude = 0
		longitude = 0
		try:
			coordinates = Geocoder.geocode(address)[0]
			latitude = coordinates.latitude
			longitude = coordinates.longitude
		except:
			pass
		coords['coordinates'] = [latitude,longitude]
		global coordCount
		coordCount += 1
		log.debug("coordinates: [" + str(latitude) + ", " + str(longitude) + "%d]")
	else:
		coords['coordinates'] = [0,0]
		log.debug("coordinates: [0, 0]")
	return coords

def parseKeywords(keywords):
	"""
	Take in a list of keywords and parse out keyword values and coordinates
	"""
	keywordList = []
	log.debug("parseKeywords called with:" + pprint.pformat(keywords));
	i = 0
	address = None
	while (i < len(keywords)):
		keywordDict = {}
		#limit to length 79 to match DB requirements
		keywordDict['keyword'] = keywords[i]['value'][0:78]
		if ('name' in keywords[i] and keywords[i]['name'] == "glocations"):
			newAddress = keywords[i]['value']
			log.debug("new address is: " + newAddress)
			if (newAddress != None):
				address = newAddress
		if keywordDict not in keywordList:
			keywordList.append(keywordDict)
		i+=1
	log.debug("sending " + newAddress + " to parseLocation")
	coords = parseLocation(address)
	log.debug("parsed keywords: " + pprint.pformat(keywordList) + "coordinates: " + pprint.pformat(coords))
	return [keywordList, coords]

def parseMultimedia(multimedia):
	"""
	Take multimedia and return image urls that are NOT thumbnails"
	"""
	multimediaList = []
	for media in multimedia:
		multimediaDict = {}
		if ("type" in media and "subtype" in media and "url" in media and media["type"] == "image" and media["subtype"] != "thumbnail"):
			url = "http://graphics8.nytimes.com/" + media["url"]
			if (url not in multimediaList):
				multimediaDict["url"] = url
				if multimediaDict not in multimediaList:
					multimediaList.append(multimediaDict)
					global multimediaCount
					multimediaCount += 1
			else:
				log.debug("scrapping: " + pprint.pformat(media))
		else:
			log.debug("scrapping: " + pprint.pformat(media))
	return multimediaList

def parseCategory(article):
	for category,keywords in categories.iteritems():
		for keyword in keywords:
			if(keyword in article['keywords']):
				return category
	return 'world'

def jsonArticle(article):
	"""
	Take an article and parses it into a JSON object
	"""
	data = {}
	#parsing required
	byline = parseByline(article['author'])
	if ((byline == None)):
		data['byline'] = "New York Times"
	else: data['byline'] = byline
	data['headline'] = parseTitle(article['headline'])[0:199]
	data['authors'] = parseAuthors(article['author'])
	if (article['lead_paragraph'] != None):
		data['abstract'] = article['lead_paragraph'][0:499]
	#DB doesn't accept null abstract
	else:
		data['abstract'] = ""
	#coordinates are derived from parsed keywords
	parsedKeywords = parseKeywords(article['keywords'])
	if (len(parsedKeywords[0]) > 0 and parsedKeywords[0][0]['keyword'] != ""):
		data['keywords'] = parsedKeywords[0]
	else:
		data['keywords'] = []
	data['coords'] = parsedKeywords[1]
	data['images'] = parseMultimedia(article['multimedia'])
	#no parsing required
	data['url'] = article['url']
	data['date'] = article['date']
	data['sourceid'] = 'NYT_' + article['id']
	data['sectionname'] = 'miscellaneous' if article['news_desk'] == None else article['news_desk']
    data['category'] = parseCategory(data) if len(data['keywords']) > 0 else 'world'
	#defaults until resolved ########
	data['retweetcount'] = 0
	data['retweetcounts'] = []
	data['sharecount'] = 0
	data['facebookcounts'] = []
	#######################
	json_object = json.dumps(data)
	return (json_object)

def convertToJSONArray(news):
	"""
	Take a list of articles as dictionaries, and convert them
	to an array of JSON
	objects for posting to database
	"""
	jsonArray = []
	for article in news:
		log.debug("trying to json:" + pprint.pformat(article))
		jsonObject = jsonArticle(article)
		if jsonObject != None:
			jsonArray.append(jsonObject)
	return(jsonArray)

def updateDB(jsonObject):
		"""
		Takes in a json string and attempts to retrieve corresponding article from database
		once retrieved, all fields are updated except for twitter and facebook counts'
		"""
		#old json to fix
		updatedJson = json.loads(jsonObject)
		log.debug("post failed, trying to update existing")
		dbJson = requests.get("http://localhost/geonewsapi/articles/?format=json&sourceid=" +  updatedJson['sourceid']).json()[0]
		log.debug("trying to get from http://localhost/geonewsapi/articles/?format=json&sourceid=" + updatedJson['sourceid'])
		log.debug("This is dbJson\n")
		log.debug(bJson)
		for key in updatedJson:
			log.debug("adding " + str(key) + " to dbJson")
			log.debug("dbJson is a " + str(type(dbJson)))
			log.debug("updatedJson is a " + str(type(updatedJson)))
			dbJson[key] = updatedJson[key]
		final = json.dumps(dbJson)
		log.debug("trying to put: " + final)
		log.debug("trying to put to http://localhost/geonewsapi/articles/" + str(dbJson['pk']))
		x = requests.put('http://localhost/geonewsapi/articles/' + str(dbJson['pk'])+'/' , data = final, headers={'content-type':'application/json', 'accept':'application/json'})
		log.debug("Status Code:" + str(x.status_code) + " Reason: " + x.reason)
		if (200 <= x.status_code <= 299):
			return 1
		else:
			log.error("Attempted to submit: " + final)
			timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
			log.error("Status Code:" + str(x.status_code) + " Reason: " + x.reason + " Relevant html file: " + (str(dbJson['pk']) + "_" + timestamp + ".html"))
            serverErrorFile = open("logs/html/nyt_" + str(dbJson['pk']) + "_" + timestamp + ".html", "w")
            serverErrorFile.write(x.content)
			serverErrorFile.close()
			return 0

def postToDB(jsonArray):
	""" take in an array of json strings and attempt to post to database """
	for jsonObject in jsonArray:
		log.debug("trying to post:" + jsonObject)
		r = requests.post("http://localhost/geonewsapi/articles/", data=jsonObject, headers={'content-type':'application/json', 'accept':'application/json'} )
		log.debug("Status Code:" + str(x.status_code) + " Reason: " + x.reason)
		if (200 <= r.status_code <= 299):
			global submitted
			submitted += 1
		if (r.status_code == 400):
			if not ("sourceid" in r.content):
                log.error("Attempted to submit: " + jsonObject)
    			timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    			log.error("Status Code:" + str(x.status_code) + " Reason: " + x.reason + " Relevant html file: " + (str(dbJson['pk']) + "_" + timestamp + ".html"))
    			serverErrorFile = open("logs/html/nyt_" + str(dbJson['pk']) + "_" + timestamp + ".html", "w")
    			serverErrorFile.write(x.content)
    			serverErrorFile.close()
			else:
				global duplicates
				duplicates += 1
				if (updateDB(jsonObject)):
					submitted += 1

articles = getArticles(date)
articleList = parseArticleList(articles)
jsonArray = convertToJSONArray(articleList)
if debugging:
	for article in (jsonArray):
		log.debug(article)

log.debug("multimedia returned: " + str(multimediaCount))

#posts all of the articles to the DB. Uncomment when ready.
postToDB(jsonArray)

stats = "finished, stats:\n\t\t\tarticles pulled: " + str(len(articleList))
stats += "\n\t\t\tarticles to submit: " + str(len(jsonArray))
stats += "\n\t\t\tarticles successfully submitted: " + str(submitted)
stats += "\n\t\t\tduplicate articles found: " + str(duplicates)
stats += "\n\t\t\tgeolocation coordinates returned: " + str(coordCount)
stats += 80*"#"
log.info(stats)

################################################################

#TEST PRINT STATEMENTS:
#i = 0
#while ( i < len(articleList)):
#	id = articleList[i]['source']
#	print id
#	i += 1
#print(parseTitle(((parseArticleList(getArticles(date))))[0]['keywords']))
#print(((((parseArticleList(getArticles(date))))[0]['keywords'][0]['value'])))
#print(((((parseArticleList(getArticles(date))))[0]['keywords'][0]['value'])))
#r = requests.post("http://localhost/geonewsapi/articles/",(convertToJSONArray(parseArticleList(getArticles(date))))[0])
#print(r.status_code, r.reason)
#print(Geocoder.geocode("New York City, New York")[0].coordinates)
#
#	print("trying to post:\n")
#	print(jsonArray[0])
#	r = requests.post("http://localhost/geonewsapi/articles/", data=jsonArray[0], headers={'content-type':'application/json', 'accept':'application/json'} )
#	print(r.status_code, r.reason)
