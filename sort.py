

import os
import sys

# use brew install pyspark, open pyspark from terminal, import sys, print sys.path, copy py4j part and paste below
# os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/1.4.1"
# sys.path.append("/usr/local/Cellar/apache-spark/1.4.1/libexec/python/")
# sys.path.append("/usr/local/Cellar/apache-spark/1.4.1/libexec/python/lib/py4j-0.8.2.1-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from operator import add
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)

import datetime


def main(arglist):

    with open("log_file_x.txt", "a") as f:
        f.write("Start time of sort...... %s\n" % datetime.datetime.now())

    print("Start time...... %s" % datetime.datetime.now())

    # mapreduce params
    path = arglist[0]
    output = arglist[1]
    minPartitions = int(arglist[2])

    # initialize
    conf = SparkConf()
    conf = conf.setMaster('local').setAppName("PythonSort").set("spark.driver.memory", "10g").set("spark.driver-maxResultSize", "3g")
    sc = SparkContext(conf=conf)

    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(path)
    counts = lines.flatMap(lambda x: x.split('\n')) \
                  .map(lambda x: (x, 1)) \
                  .sortByKey(lambda x: x)
    counts.saveAsTextFile(output)
    # # print(rdd)
    # f = open(output, 'w')
    # f.writelines('\n'.join(rdd))
    # f.close()

    # # write to one single file
    # single_output = open('single_output', 'w')
    # for i in range(minPartitions):
    #     file_name = 'part-000' + ('0'+str(i) if i < 10 else str(i))
    #     file_path = os.path.join(output, file_name)
    #     file = open(file_path, 'r')
    #
    #     single_output.write(''.join(file))
    # single_output.close()
    sc.stop()

    print("End time of sort...... %s" % datetime.datetime.now())
    with open("log_file_x.txt", "a") as f:
        f.write("End time of sort...... %s\n" % datetime.datetime.now())


if __name__ == "__main__":

    main(sys.argv[1:])
