from pyspark.sql import *

#start session
spark = SparkSession.builder.master('local[2]').appName('orc_reading').getOrCreate()

#load the data
df  = spark.read.orc('userdata1_orc')

#show
df.show()

#to overwrite the files
df.write.format('orc').mode('overwrite').save('./output_orc/userdata_saved_orc')

#to write it into parquet format
df.write.format('parquet').mode('append').save('./output_orc/userdata_saved_parquet')

#to write it in csv
df.write.format('csv').mode('append').save('./output_orc/userdata_saved_csv')

#to write it in json
df.write.format('json').mode('append').save('./output_orc/userdata_saved_json')

#to load the files
df_orc = spark.read.format('orc').load('./output_orc/userdata_saved')


df_json = spark.read.format('json').load('./output_orc/userdata_saved_json')

df_json.show()
print(df_json.count() , len(df_json.columns))
