from pyspark import SparkContext
sc = SparkContext.getOrCreate()
print(sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
