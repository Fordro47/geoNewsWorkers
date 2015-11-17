import sys
import json
import requests
import time
from nytimesarticle import articleAPI
from pygeocoder import Geocoder

#System arguments #######################################################
date = sys.argv[1]
debugging = False
if (len(sys.argv) == 3 and str(sys.argv[2]).lower() == "-d"):
    debugging = True
    print("debugging enabled")
	
#Variables #############################################################	
api = articleAPI("af0ead0b339871714bd8718ac007283b:11:73169680")

submitted = 0
coordCount = 0
logFile = open('nyt_log.txt', 'a')

logFile.write(time.utc() + " nyt_worker called for: " + date)

#initializes end date by taking date and adding 1 day (86400 seconds)
endDateStruct = time.strptime(date, '%Y%m%d')
timeInSeconds = time.mktime(endDateStruct)
endDateTuple = time.gmtime(timeInSeconds + 86400)
endDate = time.strftime('%Y%m%d', endDateTuple)
####################################################################

def getArticles(date):
	'''
	This function returns a list of 100 dictionaries containing
	NYT articles from a specified date
	'''
	articleList = []

	#NYT by default only returns 1 of 100 pages containing the first 10 results 
	for i in range(100):
		articleList.append(api.search(begin_date = date, end_date = endDate, sort = "newest", fl = ["byline", "headline", "keywords", "web_url", "pub_date", "lead_paragraph", "news_desk", "_id", "source"], facet_field = ["section_name"], page = i + 1))
		time.sleep(.1)
	return (articleList)	

def parseArticles(articles):
    '''
    This function takes in a response to the NYT api and stores
    the articles into a list of dictionaries

    modified from: http://dlab.berkeley.edu/blog/scraping-new-york-times-articles-python-tutorial
    '''
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
		
        if debugging: print("adding news from: %s\n") % (i['web_url']) 
		
        news.append(dic)
		
    if debugging: print("articles in page: %d\n\n") % (len(news))
	
    return(news)

def parseArticleList(articleList):
	'''
	This function takes in a list of responses from the NYT api
	and returns a list of dictionaries
	'''	
	news = []
	i = 0
	while i < len(articleList):
	
		if debugging: print(80*"#" + "\n" + "adding articles from page: %d") % (i + 1)
		
		news.extend(parseArticles(articleList[i]))
		i += 1
	return(news)

def parseByline(author):
	'''
	Takes author key from dictionary and 
	returns what comes after "BY:"
	'''
	if (author != None and 'original' in author and len(author['original']) > 0):
		original = author['original']
		return original[3:] 
	else:
		return None

def parseAuthors(author):
	'''
	Takes author key and 
	returns a list of dictionaries of first and last names
	'''
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
	'''
	Takes title key from dictionary and 
	returns the title
	'''
	if 'main' in headline:
		return (headline['main'])
	elif 'name' in headline:
		return (headline['name'])
	else:
		return ""

def parseLeadParagraph(lead_paragraph):
	'''
	Takes lead_paragraph key from dictionary and 
	returns the lead_paragraph
	'''
	if (lead_paragraph != None): 
		return (lead_paragraph)
	else: 
		return ""

def parseLocation(address):
	'''
	takes in an address, decodes it with Geocoder,
	and returns a set of coordinates
	'''
	coords = {}
	coords['type'] = "Point"	
	if debugging: 
		print("parseLocation() received: " + str(address))
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
		if debugging: print("coordinates: [%d, %d]" % (latitude, longitude))
	else:
		coords['coordinates'] = [0,0]
		if debugging: print("coordinates: [0, 0]")
	return coords
		
def parseKeywords(keywords):
	'''
	Take in a list of keywords and parse out keyword values and coordinates
	'''
	keywordList = []
	if debugging: print("parseKeywords called with:")
	if debugging: print(keywords)

	i = 0
	address = None
	while (i < len(keywords)):
		keywordDict = {}
		#limit to length 79 to match DB requirements
		keywordDict['keyword'] = keywords[i]['value'][0:78]
		if ('name' in keywords[i] and keywords[i]['name'] == "glocations"):
			newAddress = keywords[i]['value']
			if debugging: print("new address is: %s" % (newAddress))
			if (newAddress != None):
				address = newAddress
		keywordList.append(keywordDict)
		i+=1
		
	if debugging: print("sending %s to parseLocation" % (address))
	
	coords = parseLocation(address)
	
	if debugging:
		print("parsed keywords: ")
		print(keywordList)
		print("coordinates: ")
		print(coords)
		print("\n")
		
	return [keywordList, coords]

def jsonArticle(article):
	'''
	Take an article and parses it into a JSON object
	'''
	data = {}
	#parsing required
	byline = parseByline(article['author'])
	if ((byline == None)):
		data['byline'] = "New York Times"
	else: data['byline'] = byline
	data['headline'] = parseTitle(article['headline'])
	data['authors'] = parseAuthors(article['author'])	
	if (article['lead_paragraph'] != None):
		data['abstract'] = article['lead_paragraph']
		
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

	#no parsing required
	data['url'] = article['url']
	data['date'] = article['date']
	data['sourceid'] = 'NYT_' + article['id']
	category = article['news_desk']
	if (category != None):
		data['category'] = category
	else:
		data['category'] = 'miscellaneous'
	
	#defaults until resolved ########	
	data['retweetcount'] = 0
	data['retweetcounts'] = []
	
	data['sharecount'] = 0
	data['facebookcounts'] = []
	#######################
	
	json_object = json.dumps(data)
	return (json_object)

def convertToJSONArray(news):
	'''
	Take a list of articles as dictionaries, and convert them
	to an array of JSON 
	objects for posting to database
	'''
	jsonArray = []
	for article in news:
		if debugging: print("trying to json:")
		if debugging: print(article)
		jsonObject = jsonArticle(article)
		if jsonObject != None:
			jsonArray.append(jsonObject)
	return(jsonArray)
	
def updateDB(jsonObject):
		''' 
		Takes in a json string and attempts to retrieve corresponding article from database
		once retrieved, all fields are updated except for twitter and facebook counts'
		'''

		#old json to fix
		oldJson = json.loads(jsonObject)

		if debugging: print("\npost failed, trying to update existing")
		g = requests.get("http://cc-nebula.cc.gatech.edu/geonewsapi/articles/?format=json&sourceid=" +  oldJson['sourceid'])
		if debugging: print ("trying to get from http://cc-nebula.cc.gatech.edu/geonewsapi/articles/?format=json&sourceid=" + oldJson['sourceid'])

		newJson = json.loads(g.content)
		
		oldJson['retweetcount'] = newJson[0]['retweetcount']
		oldJson['retweetcounts'] = newJson[0]['retweetcounts']
		oldJson['sharecount'] = newJson[0]['sharecount']
		oldJson['facebookcounts'] = newJson[0]['facebookcounts']
		oldJson['pk'] = newJson[0]['pk']

		testDict = {}
		keyList = oldJson.keys()
		for key in keyList:
			testValue = oldJson[key]
			if (type(testValue) == 'unicode'):
				testValue.encode('utf-8')
			testDict[key.encode('utf-8')] = testValue
		
		final = json.dumps(testDict)
		print ("trying to put:")
		print (final)
		print ("\n")
		
		print ("trying to put to http://cc-nebula.cc.gatech.edu/geonewsapi/articles/" + str(newJson[0]['pk']))
		x = requests.put("http://cc-nebula.cc.gatech.edu/geonewsapi/articles/" + str(newJson[0]['pk']), data=final, headers={'content-type':'application/json', 'accept':'application/json'})
		print (x.status_code, x.reason, x.content)
		if (200 <= x.status_code <= 299):
			return 1
		else:
			return 0

def postToDB(jsonArray):
	""" take in an array of json strings and attempt to post to database """
	for jsonObject in jsonArray:
		if debugging: 
			print("trying to post:")
			print(jsonObject)
			print("\n")
		r = requests.post("http://cc-nebula.cc.gatech.edu/geonewsapi/articles/", data=jsonObject, headers={'content-type':'application/json', 'accept':'application/json'} )
		
		if debugging: print(r.status_code, r.reason, r.content)
		if debugging: print("\n")
		
		if (200 <= r.status_code <= 299):
			global submitted
			submitted += 1
			
		if (r.status_code == 400):
			logFile.write(r.status_code, r.reason, r.content)
			#if (updateDB(oldJson)):
				#submitted += 1

articles = getArticles(date)
articleList = parseArticleList(articles)
jsonArray = convertToJSONArray(articleList)
if debugging:
	for article in (jsonArray):
		print(article)
		print("\n")
		
#posts all of the articles to the DB. Uncomment when ready.		
#postToDB(jsonArray)

logFile.write("articles pulled: 1000\n")
logFile.write("articles to submit: %d\n" % (len(jsonArray)))
logFile.write("articles successfully submitted: %d\n" %  (submitted))
logFile.write("geolocation coordinates returned: %d\n" % (coordCount))
logFile.write(time.time() + " END")

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
#r = requests.post("http://cc-nebula.cc.gatech.edu/geonewsapi/articles/",(convertToJSONArray(parseArticleList(getArticles(date))))[0])
#print(r.status_code, r.reason)
#print(Geocoder.geocode("New York City, New York")[0].coordinates)
#
#	print("trying to post:\n")
#	print(jsonArray[0])
#	r = requests.post("http://cc-nebula.cc.gatech.edu/geonewsapi/articles/", data=jsonArray[0], headers={'content-type':'application/json', 'accept':'application/json'} )
#	print(r.status_code, r.reason) 
