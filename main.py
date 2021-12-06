from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col


spark_con=SparkContext.getOrCreate()
spark_con.setLogLevel('OFF')
ssc = StreamingContext(spark_con,1)
spark=SparkSession(spark_con)

try:
        record = ssc.socketTextStream('localhost',6100)
except Exception as e:
        print(e)
        
def readstream(rdd):
        if(len(rdd.collect())>0):
          df=spark.read.json(rdd)
          df.show()
               
record.foreachRDD(lambda x:readstream(x))

ssc.start()
ssc.awaitTermination()
