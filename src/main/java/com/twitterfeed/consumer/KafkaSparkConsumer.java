/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitterfeed.consumer;

import com.google.common.collect.Lists;

import java.util.*;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;
import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more Kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.KafkaWordCount broker1-host:port,broker2-host:port topic1,topic2
 */

public final class KafkaSparkConsumer {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {
    String brokers = "localhost:9092";
    String topics = "tw";

    // Create context with 2 second batch interval
    SparkConf sparkConf = new SparkConf().set(
            "spark.streaming.receiver.writeAheadLog.enable", "false").setMaster("local[*]")
            .setAppName("twitterSparkKafka")
            .set("spark.driver.memory", "2g")
            .set("spark.executor.memory", "2g");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

    HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", brokers);

    // Create direct kafka stream with brokers and topics
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
        jssc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topicsSet
    );

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map((Function<Tuple2<String, String>, String>) tuple2 -> tuple2._2());
    JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>) x -> Lists.newArrayList(SPACE.split(x)));

    words.persist();

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
            (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)).reduceByKey(
            (Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

    wordCounts = wordCounts.filter((Function<Tuple2<String, Integer>, Boolean>) v1 ->  CheckCharacter.isCharacter(v1._1) == false );

    //swap counts to be able to sort it
    JavaPairDStream<Integer, String> swappedCounts = wordCounts.mapToPair
            ((PairFunction<Tuple2<String, Integer>, Integer, String>) in -> in.swap() );

    //sort based on count of earch query string.
    JavaPairDStream<Integer, String> sortedCounts = swappedCounts
            .transformToPair((Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>) in -> in.sortByKey(false));

    sortedCounts.print(20);


    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}