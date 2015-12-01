import json
import requests
import datetime	

articles = requests.get('http://localhost/geonewsapi/articles/?format=json).json()
logFile = open("logs/nyt_locator.txt", "a")

def geoLocate(article):
	""" 
	takes in article and returns a set of coordinates if possible
	else: returns [0,0]
	"""
	# TODO use places.py
	return [0,0]

def updateArticles(articles):
	""" Attempts to update non-geolocated articles """
	updatedArticles = []
	
	for article in articles:
		if article["coords"]["coordinates"] == [0,0]:
			coordinates = geoLocate(article)
			if coordinates != [0,0]:
				article["coords"]["coordiates"] = coordinates
				updatedArticles.append(article)
	return updatedArticles

def updateDB(articles):
	""" Posts updated articles to db """
	global logFile
	updateCount = 0
	
	for article in articles:
		r = requests.put('http://localhost/geonewsapi/articles/' + str(article['pk'])+'/' , data = json.dumps(article), headers={'content-type':'application/json', 'accept':'application/json'})
		
		print (r.status_code, r.reason, r.content)
		if (200 <= r.status_code <= 299):
			updateCount += 1
		else:
			timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
			logFile.write(timestamp + "\n ")
			logFile.write(str(r.status_code) + ", ")
			logFile.write(r.reason + ", ")
			if (r.status_code != 500):
				logFile.write(r.content + "\n\n    ")
			else:
				logFile.write("Relevant html file: " + (str(dbJson['pk']) + ".html\n"))
				serverErrorFile = file.open("logs/html/nyt_locator_" + article["pk"], "a")
				serverErrorFile.write(r.content)
				serverErrorFile.close()
			logFile.write(str(jsonObject) + "\n\n")

updatedArticles = updateArticles(articles)
updateCount = updateDB(updated=Articles)

logFile.write("articles pulled: %d\n" % (len(articles)))
logFile.write("articles to update: %d\n" % (len(updatedArticles)))
logFile.write("articles updated: %d\n" % (updateCount))


