from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('testudf').master("yarn").enableHiveSupport().getOrCreate()

rows = [Row(injury='1998-02-02 09:00:00.000000', birth_date='1991-06-18')]

df = spark.createDataFrame(rows)

udf_date_minus = udf(lambda end_date,from_date: "" if end_date == None or from_date == None or from_date == "00:00:00.0000000" or not end_date or not from_date or end_date == "" or from_date == ""  else relativedelta(datetime.strptime(end_date.split()[0],'%Y-%m-%d'),datetime.strptime(from_date,'%Y-%m-%d')).years,IntegerType())


out=df.withColumn("output_udf",udf_date_minus('injury','birth_date'))
out.show()
