package com.qa.sparkStreaming;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.qa.Util.PriorityHashTags;
import com.qa.Util.SendTrendningMail;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

public class SelfLearningExecutor {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		String accessToken = "273347143-r6KLzwCkGODLO7TOrawqcbkMGLk2lJVI1GLuOjGw";
		String secretToken = "a948FKzNoPQMYj6Xt71aE9kGZaXVOJZM8Pcj276XY5uSK";
		String username = "ZOFwIf6de4GBdwxGMEMN5sw2v";
		String password = "vKBWVUsjuwGEJyj7YYAfFYI1CYkLzkTXhfCiThQmJytPT8ezre";

		ConfigurationBuilder builder = new ConfigurationBuilder();
		builder.setOAuthAccessToken(accessToken);
		builder.setOAuthAccessTokenSecret(secretToken);
		builder.setOAuthConsumerKey(username);
		builder.setOAuthConsumerSecret(password);

		SparkConf conf = new SparkConf().setMaster("spark://dllu0003:7077").setAppName("TwitterUtils");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(30));

		JavaDStream<Status> dtweets = TwitterUtils.createStream(sc, new OAuthAuthorization(builder.build()));

		JavaDStream<Status> filtered = dtweets.filter(x -> PriorityHashTags.testForPriorityHashTags(x));

		filtered.foreachRDD(new Function<JavaRDD<Status>, Void>() {
			@Override
			public Void call(JavaRDD<Status> v1) throws Exception {
				for(Status s : v1.collect()) {
					PriorityHashTags.writePriorityHashTags(Arrays.asList(s.getText().split(" ")));
				} 
				return null;
			}
		});

		JavaPairDStream<Status, Long> topUsers = filtered
				.mapToPair(s -> new Tuple2<Status, Long>(s, new Long(s.getUser().getFollowersCount())));

		JavaPairDStream<Status, Long> topUsersAgg = topUsers.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
			public Long call(Long i, Long j) {
				return i;
			}
		}, new Duration(60 * 1 * 1000), new Duration(60 * 1 * 1000));

		JavaPairDStream<Status, Long> topUsersFinal = topUsersAgg.filter(s -> s._2 > 10);
		topUsersFinal.print(10);

		topUsersFinal.foreachRDD(new Function<JavaPairRDD<Status, Long>, Void>() {
			@Override
			public Void call(JavaPairRDD<Status, Long> v1) throws Exception {
				Map<Status, Long> mp = new HashMap<Status, Long>();
				mp = v1.collectAsMap();
				if (!mp.isEmpty()) {
					SendTrendningMail.sendMail(mp);
				}
				return null;
			}
		});

		sc.start();
		sc.awaitTermination();

	}

}
