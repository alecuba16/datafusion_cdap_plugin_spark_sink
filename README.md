# datafusion_cdap_spark_sink_plugin
Complete example project to create a custom Google cloud datafusion (CDAP) spark sink plugin. Sourced and adapted from the documentation where there is no quickstart project.

## Spark Sink Plugin
A SparkSink plugin is used to perform computations on a collection of input records and optionally write output data. It can only be used in batch data pipelines. A SparkSink is similar to a SparkCompute plugin except that it has no output. In a SparkSink, you are given access to anything you would be able to do in a Spark program. For example, one common use case is to train a machine-learning model in this plugin.

In order to implement a Spark Sink Plugin, you extend the SparkSink class.

## Methods
### configurePipeline():
Used to perform any validation on the application configuration that is required by this plugin or to create any datasets if the fieldName for a dataset is not a macro.
### run():
This method is given a Spark RDD (Resilient Distributed Dataset) containing every object that is received from the previous stage. Then this method performs Spark operations on the input, and usually saves the result to a dataset.
