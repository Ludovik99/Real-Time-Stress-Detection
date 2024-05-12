#!/usr/bin/env python
# coding: utf-8

# ## Using pre-trained models best_model_hrv and best_model_eda (defined in notebook "Stress Alert 1 - Stress detection ML models" to make predictions on new unlabeled data

# ### Initialize SparkSession for Spark Structured Streaming and import needed module

# In[2]:


import findspark
findspark.init()


# In[4]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Combined Streaming Pipeline") \
    .getOrCreate()


# In[5]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline


# ### Pre-processing incoming streaming data from the two sources

# In[ ]:


# Preprocess unlabeled data for df_streaming_hrv
indexer_hrv = StringIndexer(inputCol="label", outputCol="indexedLabel")
assembler_hrv = VectorAssembler(inputCols=df_streaming_hrv.columns[:-2], outputCol="features")
pipeline_hrv = Pipeline(stages=[indexer_hrv, assembler_hrv])
preprocessed_data_hrv = pipeline_hrv.fit(df_streaming_hrv).transform(df_streaming_hrv)


# In[ ]:


# Preprocess unlabeled data for df_streaming_eda
indexer_eda = StringIndexer(inputCol="label", outputCol="indexedLabel")
assembler_eda = VectorAssembler(inputCols=df_streaming_eda.columns[:-2], outputCol="features")
pipeline_eda = Pipeline(stages=[indexer_eda, assembler_eda])
preprocessed_data_eda = pipeline_eda.fit(df_streaming_eda).transform(df_streaming_eda)


# ### Generate predictions on incoming data

# In[ ]:


# Make predictions using the best DecisionTreeClassifier model for df_streaming_hrv
predictions_hrv = best_model_hrv.transform(preprocessed_data_hrv)

# Make predictions using the best RandomForestClassifier model for df_streaming_eda
predictions_eda = best_model_eda.transform(preprocessed_data_eda)

# Combine predictions from both models
combined_predictions = predictions_hrv.join(predictions_eda, on="id") \
    .withColumn("predicted_label", when((col("prediction") == 1) | (col("prediction_eda") == 1), 1).otherwise(0)) \
    .select("id", "predicted_label")


# ### Implement a watermark to allow reception of late data

# In[ ]:


watermarked_predictions = combined_predictions \
    .withWatermark("timestamp", "5 seconds")


# ### Write the combined predictions to an output sink (e.g., console) using Spark Structured Streaming

# In[ ]:


query = watermarked_predictions.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='1 seconds') \  # Trigger to update every second
    .start()

query.awaitTermination()

