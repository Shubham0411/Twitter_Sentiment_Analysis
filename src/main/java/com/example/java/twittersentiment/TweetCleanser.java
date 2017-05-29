package com.example.java.twittersentiment;
/*
This class receives the tweet from TweetFilter
and cleans the tweet by removing unwanted signs and
urls from the tweet and then forwards both
original (for reference)
as well as cleaned tweet for further analysis.
 */
import java.util.SortedMap;
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
  public static final Logger LOG = LoggerFactory.getLogger(TweetCleanser.class);

  public static String getFILENAME()
  {
    return FILENAME;
  }

  public static void setFILENAME(String FILENAME)
  {
    TweetCleanser.FILENAME = FILENAME;
  }

  protected static String FILENAME ;

  public final transient DefaultOutputPort<String> cleanedtweet = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<String> originaltweet = new DefaultOutputPort<>();
  public SortedMap<String, Integer> afinnSentimentMap = null;
  public TweetCleanser()
  {

  }


  public final transient DefaultInputPort<String> originaltweet1 = new DefaultInputPort<String>()
  {
    @Override
    public void process(String receivedtweet)
    {
        LOG.info("Current tweet" + receivedtweet);
        originaltweet.emit(receivedtweet);
        //the original tweet is passed to the function to remove the url
        removeUrl(receivedtweet);
    }
  };

  // will remove all the punctuations from a tweet
  public void punctuationRemover(String curr_tweet)
  {

    //Remove all punctuation and new line chars in the tweet and converting into lowercase.
    String tweet = curr_tweet.replaceAll("\\p{Punct}|\\n", " ").toLowerCase();
    LOG.info("cleaned tweet  is --> " +tweet);
    cleanedtweet.emit(tweet);

  }


  public void removeUrl(String filteredtweet)
  {
    String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
    Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(filteredtweet);
    int i = 0;
    while (m.find()) {
      filteredtweet = filteredtweet.replaceAll(m.group(i),"").trim();
      i++;
    }
    LOG.info("cleaned from url tweet  is --> " +filteredtweet);
    // passes the cleaned tweet from here to the given function
    punctuationRemover(filteredtweet);
  }
}
