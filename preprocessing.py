import sys
import re
import json
import string
from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel, StreamingKMeans
from pyspark.sql.functions import *

from pyspark.sql import functions as F
from pyspark.sql import types as T
import operator
import numpy as np
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
from nltk.stem import PorterStemmer
#from nltk.tokenize import word_tokenize



#sqlContext = SQLContext(sc)

#model_inputs = sys.argv[1]


def main():
	spark_con = SparkContext(appName="test")
	ssc = StreamingContext(spark_con, 2)
	spark=SparkSession(spark_con)
	#ssc.checkpoint("BIGDATA")
	record=ssc.socketTextStream("localhost",6100)
	st=stopwords.words('english')
	spl='[@_!#$%^&*()<>?/\|}{~:]'
	
	
	def readMyStream(rdd):
		if not rdd.isEmpty():
			rdd1=rdd.map(lambda x: json.loads(x))
			rdd2=rdd1.flatMap(lambda d:list(d[k] for k in d))
			rdd3=rdd2.map(lambda x:tuple(x[k] for k in x))
			#print(rdd3.take(3))
			columns=['subject','message','class']
			df=rdd3.toDF(columns)
			
			#df.printSchema()
			#df.show()
			#df = df.replace(to_replace ='\n', value = '', regex = True)
			a=df.select('message').collect()
		    
			ps=PorterStemmer()
			df=df.withColumn("pre",lit(0))
		    
			#remover = StopWordsRemover(stopWords=["a","the","is",""])
		    
			#a.to_numpy()
		    
			#df["transform"]=df.apply(preprosses,axis='message')
			#df.printSchema()
			#df.show()
			np.array(a)
		    
			
			for i in a:
				upper_l=[]
				for j in i:
					j=j.lower()
					j=j.split()
					#print(j)
					for l in j:
						
						#print(l,type(l))
						if (l not in st) and (l.isalpha()) and (l not in spl) and (len(l)>2):
						
							#print(l)
							a=ps.stem(l)
							#print(a)
							upper_l.append(a)
				line=" ".join(upper_l)
				#newDf = df.withColumn("pre", when(col("message")== i, line).otherwise(1))
				
				print(line)
				#words = line.split(" ")
				#rdd10 = spark.sparkContext.parallelize(words)
				#wordCounts = rdd10.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
				#print(wordCounts)
			
			
		
		
		    
	    
	record.foreachRDD( lambda rdd: readMyStream(rdd) )
	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
    main()
