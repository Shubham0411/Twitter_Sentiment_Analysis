package com.example.java.twittersentiment;
/*
This class receives the tweet from TweetFilter
and cleans the tweet by removing unwanted signs and
urls from the tweet and then forwards both
original (for reference)
as well as cleaned tweet as POJO for further analysis.
 */
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;


/**
 * Created by shubham on 25/3/17.
 */

public class TweetCleanser extends BaseOperator
{
  Data data = new Data();
  public static final Logger LOG = LoggerFactory.getLogger(TweetCleanser.class);


  public final transient DefaultOutputPort<Data> output = new DefaultOutputPort<>();
  public final transient DefaultInputPort<String> originalTweetInputAtCleanser = new DefaultInputPort<String>()
  {
    @Override
    public void process(String receivedTweet)
    {
      LOG.info("Current tweet" + receivedTweet);
      data.originalTweetInCleanser = receivedTweet;

      // it will first call removeUrl than puntuationRemover.
      String urlRemovedTweet = removeUrl(receivedTweet);
      punctuationRemover(urlRemovedTweet);
    }
  };

  // will remove all the punctuations from a tweet
  public void punctuationRemover(String currTweet)
  {
    //Remove all punctuation and new line chars in the tweet and converting into lowercase.
    String tweet = currTweet.replaceAll("\\p{Punct}|\\n", " ").toLowerCase();
    LOG.info("cleaned tweet  is --> " +tweet);

    Data d = new Data();
    d.cleanedTweetInCleanser = tweet;
    output.emit(d);
  }


  public String removeUrl(String filteredTweet)
  {
    String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
    Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(filteredTweet);
    int i = 0;
    while (m.find()) {
      filteredTweet = filteredTweet.replaceAll(m.group(i),"").trim();
      i++;
    }
    LOG.info("cleaned from url tweet  is --> " +filteredTweet);
    return filteredTweet;
  }
}

class Data
{
  String cleanedTweetInCleanser;
  String originalTweetInCleanser;
}
