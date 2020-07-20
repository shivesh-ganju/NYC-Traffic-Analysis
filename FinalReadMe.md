Read Me file
This project uses the NYC taxi dataset to derive insights as mentioned in the project paper. The read me file describes the contents of the project, setting it up and how to run it

Team members
Shivesh Ganju (sg6148)
Ravi Shankar (rs6980)

Tools used
Spark,scala,sbt, SparkSQL, Mllib, Tableau

Dataset
The dataset has been provided by NYC Taxi and Limousine commission. The dataset comprises of 360 different files which has a size of around 1.2GB each. Overall dataset size is around 300GBs. You can either download it manually or you can run the script in the data_ingest directory. Please make sure to change the folder names in the script
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

NYC shape file for Taxi zones
https://geo.nyu.edu/catalog/nyu-2451-36743

Weather Dataset
https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00094728/detail

FHV Base mapping
https://data.cityofnewyork.us/Transportation/FHV-Base-Aggregate-Report/2v9c-2k7f

Project Directories and structure
The project structure was built using sbt. However the folders have been renamed for grading purposes.
File name : NYCTaxiAnalyzer_grading_only

/app_code : This folder contains the Jar file which has the compiled code and will be used to run the application. Apart from that it contains the Tableau workbooks which has the visualizations. This also has the necessary sbt files.

/data_ingest : It has the download.py script which will download the dataset and put it into your dumbo cluster.
Run the script by using python download.py

/etl_code : This directory contains all the logic for polling data from the HDFS and then cleaning it and saving it as a RDD

/profiling_code : This directory has the code for profiling the Green taxi, yellow taxi and FHV taxi datasets

/analytics_code : This directory has the code which does the analytics

/main : The directory which has the main funciton and runs the application

/screenshots : Images of the application running 

Note : This project folder will not be compiled because of absence of sbt files and because of a different project structure

There is another zip folder in the submission which you can check if you want to compile the code (NYCTaxiAnalyzer_compilable.zip)

Project structure :

/src/main/scala/Pollers : Pollers for polling the data
/src/main/scala/Preprocessors : Cleaning the datasets
/src/main/scala/Analyzers : Code for carrying out analytics
/src/main/scala/initApp.scala : Main funciton. Runs the application

Compiling the code
Please use the NYCTaxiAnalyzer_compilable.zip folder if you want to compile the code from the beginning
Steps:
1)For each of the file in poller directory, change the location of the file where the data is available to your location.
Files - FHVTaxiPoller,GreenTaxiPoller,TaxiLocationPoller,WeatherPoller,YellowTaxiPoller
2)In analyzer change the location where you want to save the data. Find the line "saveAsTable" in the analyzer directory and change the table name so that you can view it. You'll have to do this for all the files
CountAnalyzer,PriceAnalyzer,Regressor,WeatherAnalyzer,ZoneAnalyzer
After doing the above steps you can compile the code using
sbt clean compile test doc assembly
save the table as <userid.tablename> so that you can view it in your dumbo account

Running the code
You can use the built jar file to directory run the code. Or if you prefer to compile the code then you can find the jar file in the target folder of the application. Transfer the jar file to your dumbo account using scp. Then run the following command
spark2-submit --class initApp --name "Taxi" --master yarn --deploy-mode cluster --driver-memory 5G --executor-memory 4G --num-executors 40 NYTaxiAnalyzer-assembly-1.0.jar
This should take around 30 minutes to run the complete job

Results:
The results are stored in form of hive tables. You can simply view the results from these hive tables
sg6148.Analysis1_1
sg6148.Analysis1_2
sg6148.Analysis2_1
rs6980.Analysis1_1
rs6980.Analysis1_2
rs6980.Analysis2_1
sg6148.Analysis2_2
sg6148.Analysis2_3
sg6148.AnalysisML_Linear
sg6148.AnalysisML_Forest
sg6148.zoneAnalysis

For visualizations, you can refer to the tableau workbooks and screenshots. We have provided the result dataset in the /app_code folder for your reference

Acknowledgements
We would like to thank NYU HPC for providing access to dumbo and providing spark ecosystem. We would also like to thank NYU Spatial Data Repository for providing taxi zone shapefile
https://geo.nyu.edu/catalog/nyu-2451-36743
Lastly we would like to thank KoddiDev for providing an open source geocoding library for scala which was a wrapper over google maps API for scala
https://github.com/KoddiDev/geocoder

Contribution
There has been an equal contribution. Every code file had an input from each team member