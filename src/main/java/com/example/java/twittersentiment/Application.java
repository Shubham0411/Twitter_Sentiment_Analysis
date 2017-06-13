package com.example.java.twittersentiment;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Created by shubham on 6/4/17.
 */

@ApplicationAnnotation(name="TweetsSentimentAnalysis")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, org.apache.hadoop.conf.Configuration conf)
  {

    TweetInput Input =dag.addOperator("Input",TweetInput.class);
    TweetFilter Filter = dag.addOperator("Filter", TweetFilter.class);
    TweetCleanser TweetCleanser = dag.addOperator("Cleanser", TweetCleanser.class);

    SentimentAnalyzer SentimentAnalysis = dag.addOperator("Analyser", SentimentAnalyzer.class);
    //  SentimentDisplay SentimentOutput = dag.addOperator("SentimentOutput", SentimentDisplay.class);

    GenericFileOutputOperator.StringFileOutputOperator positiveOutput = dag.addOperator("PositiveOutput", GenericFileOutputOperator.StringFileOutputOperator.class);
    GenericFileOutputOperator.StringFileOutputOperator negativeOutput = dag.addOperator("NegativeOutput", GenericFileOutputOperator.StringFileOutputOperator.class);

    //sending tweets from TweetInput to TweetFilter
    dag.addStream("sendilng inputs",Input.text,Filter.orginalTweetAtFilter);

    //sending filtered out tweets from TweetFilter to TweetCleanser
    dag.addStream("sending filtered tweets", Filter.filteredOriginalTweetOutput, TweetCleanser.originalTweetInputAtCleanser);

    //sending original and cleaned tweet from TweetCleanser to SentimentAnalyzer
    // dag.addStream("sending orignal tweet", TweetCleanser.originalTweetFromCleanser, SentimentAnalysis.originalTweetInputAtAnalyzer);
    // dag.addStream("sending cleaned tweet", TweetCleanser.cleanedTweetFromCleanser, SentimentAnalysis.cleanTweetInputAtAnalyzer);
    dag.addStream("sending data", TweetCleanser.output, SentimentAnalysis.input);


    //sending original tweet  and its sentiment value  from SentimentAnalyzer to SentimentDisplay
    //  dag.addStream("sending original tweet", SentimentAnalysis.originalTweetOutputFromAnalyzer, SentimentOutput.input);
    //  dag.addStream("analysed tweet value", SentimentAnalysis.sentimentValueFromAnalyzer, SentimentOutput.sentimentValue);

    dag.addStream("positiveSentiment", SentimentAnalysis.positiveTweetOutputFromAnalyzer, positiveOutput.input);
    dag.addStream("negativeSentiment", SentimentAnalysis.negativeTweetOutputFromAnalyzer, negativeOutput.input);


  }


}
