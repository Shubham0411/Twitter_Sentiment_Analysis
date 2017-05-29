package com.example.java.twittersentiment;

import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Created by shubham on 6/4/17.
 */

@ApplicationAnnotation(name="TweetsSentimentAnalysis")
public class Application implements StreamingApplication
{
  public static final Logger LOG = LoggerFactory.getLogger(Application.class);
  public SortedMap<String, Integer> afinnSentimentMap = null;
  @Override
  public void populateDAG(DAG dag, org.apache.hadoop.conf.Configuration conf)
  {

    TweetInput Input =dag.addOperator("Input",TweetInput.class);
    TweetFilter Filter = dag.addOperator("Filter", TweetFilter.class);
    TweetCleanser TweetCleanser = dag.addOperator("Cleanser", TweetCleanser.class);

    SentimentAnalyzer SentimentAnalysis = dag.addOperator("Analyser", SentimentAnalyzer.class);
    SentimentDisplay SentimentOutput = dag.addOperator("SentimentOutput", SentimentDisplay.class);

    //sending tweets from TweetInput to TweetFilter
    dag.addStream("sendilng inputs",Input.text,Filter.orginaltweet);

    //sending filtered out tweets from TweetFilter to TweetCleanser
    dag.addStream("sending filtered tweets", Filter.filteredtweet, TweetCleanser.originaltweet1);

     //sending original and cleaned tweet from TweetCleanser to SentimentAnalyzer
    dag.addStream("sending orignal tweet", TweetCleanser.originaltweet, SentimentAnalysis.originaltweet_2);
    dag.addStream("sending cleaned tweet", TweetCleanser.cleanedtweet, SentimentAnalysis.inputtweet1);


    //sending original tweet  and its sentiment value  from SentimentAnalyzer to SentimentDisplay
    dag.addStream("sending original tweet", SentimentAnalysis.sendOriginaltweet_2, SentimentOutput.input);
    dag.addStream("analysed tweet value", SentimentAnalysis.sentimentscore_2, SentimentOutput.sentimentscore1);

  }


}
