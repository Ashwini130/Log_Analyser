from pyspark.sql import SparkSession

#create spark session
spark = SparkSession\
        .builder\
        .appName("logfiles")\
        .getOrCreate()
sc = spark.sparkContext

#read log directory into spark rdd
logfiles = sc.textFile("file:///C:/Users/Ashwini/Desktop/logs/*.log")

#perform transformations on spark rdd
table = logfiles.filter(lambda row:"milliseconds" in row).map(lambda line: line.split('BMXAA6720W')).filter(lambda tuple: len(tuple) == 2)
table = table.map(lambda line: (line[0],line[1]))
table = table.map(lambda line: (line[0],line[1].split(":")))
tabledf1 = table.map(lambda line : (line[0],line[1][0],line[1][1]))
tabledf1 = tabledf1.map(lambda line:(line[0],line[1],line[2].split("(execution took"))).filter(lambda line: len(line[2])==2)
tabledf2 = tabledf1.map(lambda line:(line[0],line[1],line[2][0],line[2][1])).filter(lambda line: len(line)==4)

#Call an action to execute the job
print('The total number of rows is:',tabledf2.count())
#print(tabledf2.count())

#tableDFnew = spark.createDataFrame(tabledf2)

#tableDFnew.write.format('csv').option('header',True).mode('overwrite').save('file:///F:/Hadoop/sparkjobtest.csv')
