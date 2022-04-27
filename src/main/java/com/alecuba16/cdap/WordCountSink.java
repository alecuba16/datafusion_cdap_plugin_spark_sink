package com.alecuba16.cdap;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.*;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;

/**
 * Spark Sink Plugin
 * A SparkSink plugin is used to perform computations on a collection of input records and optionally write output data. It can only be used in batch data pipelines. A SparkSink is similar to a SparkCompute plugin except that it has no output. In a SparkSink, you are given access to anything you would be able to do in a Spark program. For example, one common use case is to train a machine-learning model in this plugin.
 *
 * In order to implement a Spark Sink Plugin, you extend the SparkSink class.
 *
 * ## Methods
 * ### configurePipeline():
 * Used to perform any validation on the application configuration that is required by this plugin or to create any datasets if the fieldName for a dataset is not a macro.
 * ### run():
 * This method is given a Spark RDD (Resilient Distributed Dataset) containing every object that is received from the previous stage. Then this method performs Spark operations on the input, and usually saves the result to a dataset.
 */

/**
 * SparkSink plugin that counts how many times each word appears in records input to it and stores the result in
 * a KeyValueTable.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(WordCountSink.NAME)
@Description("Counts how many times each word appears in all records input to the aggregator.")
public class WordCountSink extends SparkSink<StructuredRecord> {
    public static final String NAME = "WordCount";
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
        @Description("The field from the input records containing the words to count.")
        private String field;

        @Description("The name of the KeyValueTable to write to.")
        private String tableName;
    }

    public WordCountSink(Conf config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        // any static configuration validation should happen here.
        // We will check that the field is in the input schema and is of type string.
        Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        if (inputSchema != null) {
            WordCount wordCount = new WordCount(config.field);
            wordCount.validateSchema(inputSchema);
        }
        pipelineConfigurer.createDataset(config.tableName, KeyValueTable.class, DatasetProperties.EMPTY);
    }

    @Override
    public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
        // no-op
    }

    @Override
    public void run(SparkExecutionPluginContext sparkExecutionPluginContext,
                    JavaRDD<StructuredRecord> javaRDD) throws Exception {
        WordCount wordCount = new WordCount(config.field);
        JavaPairRDD outputRDD = wordCount.countWords(javaRDD)
                .mapToPair(new PairFunction<Tuple2<String, Long>, byte[], byte[]>() {
                    @Override
                    public Tuple2<byte[], byte[]> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return new Tuple2<>(Bytes.toBytes(stringLongTuple2._1()), Bytes.toBytes(stringLongTuple2._2()));
                    }
                });
        sparkExecutionPluginContext.saveAsDataset(outputRDD, config.tableName);
    }
}