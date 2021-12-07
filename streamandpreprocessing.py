import sys
import re
import json
import string
from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel, StreamingKMeans
from pyspark.sql.functions import *
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
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
	sparkc = SparkContext(appName="test")
	ssc = StreamingContext(sparkc, 2)
	spark=SparkSession(sparkc)
	record=ssc.socketTextStream("localhost",6100)
	
	
	
	def readMyStream(rdd):
		if not rdd.isEmpty():
			rdd_a=rdd.map(lambda x: json.loads(x))
			rdd_b=rdd_a.flatMap(lambda h:list(h[k] for k in h))
			rdd_c=rdd_b.map(lambda x:tuple(x[k] for k in x))
			 
			columns=['subject','message','class']
			df=rdd_c.toDF(columns)
			
			 
			mes=df.select('message').collect()
		    
			ps=PorterStemmer()
			df=df.withColumn("pre",lit(0))
		    
			
			st=stopwords.words('english')
			spl='[@_!#$%^&*()<>?/\|}{~:]'
			np.array(mes)
		    
			 
			for i in mes:
				updatedl=[]
				for j in i:
					j=j.lower()
					j=j.split()
					for k in j:
						if (k not in st) and (k.isalpha()) and (k not in spl) and (len(k)>2):
							a=ps.stem(k)
							updatedl.append(a)
				line=" ".join(updatedl)
				print(line)
				 
			
		
		
		    
	    
	record.foreachRDD( lambda rdd: readMyStream(rdd) )
	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
    main()
    
    
