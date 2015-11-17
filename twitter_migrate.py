'''Twitter Worker'''

'''
urls.api.twitter.com/1/urls/count.json?url=http://www.nytimes.com
/reuters/2015/09/01/world/asia/01reuters-ww2-anniversary-taiwan.html
'''
import requests
import datetime
import json


#cc-nebula.cc.gatech.edu/geonewsapi/articles/?date>=    [datetime.datetime.now()-timedelta(days=7)]




def getTweetCount(pk, url):

	append = "http://urls.api.twitter.com/1/urls/count.json?url="

	param = append + url
	retweetCount = requests.get(param).json()['count'] #urllib2.urlopen(param))['count']
	
	#post back to the database with pk, retweet combo
	#json.load(urllib2.urlopen)

	#POST to retweetcounts in the database
	# print(retweetCount)
	return retweetCount

def getUrlsAndPk(articles):

	for article in articles:
		#article = articles[104]
		#count = getTweetCount(article['pk'], article['url'])
		#article['retweetcounts'].append({'retweetcount': count})
		article['retweetcount'] = article['retweetcounts'][0]['retweetcount']
		print json.dumps(article)
		print requests.put('http://cc-nebula.cc.gatech.edu/geonewsapi/articles/' + str(article['pk'])+'/' , data = json.dumps(article), headers={'content-type':'application/json', 'accept':'application/json'})


#get back the date 7 days ago in the format specified
date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

#articles will be an array of 
articles = requests.get('http://cc-nebula.cc.gatech.edu/geonewsapi/articles/?format=json&startdate=' + date).json()

getUrlsAndPk(articles)




'''
get all the articles from the database

get the urls from the articles

go through and add that article url to the twitter GET call

store that count with the primary key

POST the pk, count

'''
