import urllib2
from subprocess import PIPE, Popen
import shutil
import os
year_yellow=[i for i in range(2009,2020)]
year_green = [i for i in range(2013,2020)]
year_fhv = [i for i in range(2015,2020)]
folder_yellow = "project/yellowTaxi/"
folder_green = "project/folder_green/"
folder_fhv = "project/folder_fhv/"
print("Downloading yellow taxi data on server")
for year in year_yellow:
	os.makedirs("/home/sg6148/"+str(folder_yellow))
	for i in range(1,13):
		url=""
		if i <10 :
			url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_"+str(year)"-0"+str(i)+".csv"
		else:
			url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_"+str(year)"-"+str(i)+".csv"
		file = urllib2.urlopen(url)
		writer = file.read()
		path = "/home/sg6148/"+str(folder_yellow)+str(year)+"_"+str(i)+".csv"
		with open(path,'wa') as a:
	        a.write(writer)
		put = Popen(["hdfs", "dfs", "-put",path, folder_yellow], stdin=PIPE, bufsize=-1)
		put.communicate()
	print("Year " + str(year) + "put in hdfs")    
	shutil.rmtree("/home/sg6148/"+str(folder_yellow))

print("Downloading Green taxi data on server")
for year in year_green:
	os.makedirs("/home/sg6148/"+str(folder_green))
	for i in range(1,13):
		url=""
		if i <10 :
			url = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_"+str(year)"-0"+str(i)+".csv"
		else:
			url = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_"+str(year)"-"+str(i)+".csv"
		file = urllib2.urlopen(url)
		writer = file.read()
		path = "/home/sg6148/"+str(folder_green)+str(year)+"_"+str(i)+".csv"
		with open(path,'wa') as a:
	        a.write(writer)
		put = Popen(["hdfs", "dfs", "-put",path, folder_green], stdin=PIPE, bufsize=-1)
		put.communicate()
	print("Year " + str(year) + "put in hdfs")    
	shutil.rmtree("/home/sg6148/"+str(folder_green))

print("Downloading fhv taxi data on server")
for year in year_yellow:
	os.makedirs("/home/sg6148/"+str(folder_fhv))
	for i in range(1,13):
		url=""
		if i <10 :
			url = "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_"+str(year)"-0"+str(i)+".csv"
		else:
			url = "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_"+str(year)"-"+str(i)+".csv"
		file = urllib2.urlopen(url)
		writer = file.read()
		path = "/home/sg6148/"+str(folder_fhv)+str(year)+"_"+str(i)+".csv"
		with open(path,'wa') as a:
	        a.write(writer)
		put = Popen(["hdfs", "dfs", "-put",path, folder_fhv], stdin=PIPE, bufsize=-1)
		put.communicate()
	print("Year " + str(year) + "put in hdfs")    
	shutil.rmtree("/home/sg6148/"+str(folder_fhv))