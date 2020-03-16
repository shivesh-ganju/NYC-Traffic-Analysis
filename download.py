import urllib2
for i in range(2,13):
	url=""
	if i <10 :
		url = "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-0"+str(i)+".csv"
	else:
		url = "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-"+str(i)+".csv"
	print("Starting Download for file  " + str(i))
	file = urllib2.urlopen(url)
	writer = file.read()
	path = "/home/sg6148/yellowTaxi"+str(i)+".csv"
	with open(path,'wa') as a:
        a.write(writer)