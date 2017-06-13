package com.example.java.twittersentiment;



/*
This class takes input from the TweetInput and
then searches for a particular word in that
tweet and if the word is found the respective tweet will
be forwarded to the next operator otherwise its rejected.
*/

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by shubham on 14/5/17.
 */
public class TweetFilter extends BaseOperator
{


  public static final Logger LOG = LoggerFactory.getLogger(TweetFilter.class);


  public final transient DefaultInputPort<String> orginalTweetAtFilter = new DefaultInputPort<String>()
  {
    @Override
    public void process(String unfilteredTweetAtFilter)
    {
      LOG.info("Unfiltered Tweet =>" + unfilteredTweetAtFilter);
      filter(unfilteredTweetAtFilter);
    }
  } ;



  public final transient DefaultOutputPort<String> filteredOriginalTweetOutput = new DefaultOutputPort<>();

  public String getWordsToFilterTweet()
  {
    return wordsToFilterTweet;
  }
  public void setWordsToFilterTweet(String wordsToFilterTweet)
  {
    this.wordsToFilterTweet = wordsToFilterTweet;
  }


  // Enter the particular word you want to search for in the tweet
  private String wordsToFilterTweet;


  public void filter(String unfilteredTweet)
  {    String unfilteredTweet_LowerCase = unfilteredTweet.toLowerCase();
    String [] unfilTweetInArray = unfilteredTweet_LowerCase.split(" ");
    boolean contains = false;
    String[] wordsToFindArray = wordsToFilterTweet.split(",");

//iterate the String array
    for(int i=0; i < unfilTweetInArray.length; i++) {

      for (int j = 0; j < wordsToFindArray.length; j++) {
        //check if string array contains the string
        if (unfilTweetInArray[i].equals(wordsToFindArray[j])) {

          //string found
          contains = true;
          break;

        }
      }
    }


    //emits if the particular word is found in the tweet.
    if(contains == true ){
      // will be displayed in LOGS in GUI
      String filterTweet = Arrays.toString(unfilTweetInArray);
      LOG.info("Filtered Tweet =>" + filterTweet);
      filteredOriginalTweetOutput.emit(unfilteredTweet);
    }
  }

}

