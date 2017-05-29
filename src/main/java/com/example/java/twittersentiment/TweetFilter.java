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
// Enter the particular word you want to search for in the tweet
  String strFind1 = "bad";

  public static final Logger LOG = LoggerFactory.getLogger(TweetFilter.class);

  public final transient DefaultInputPort<String> orginaltweet = new DefaultInputPort<String>()
  {
    @Override
    public void process(String unfilteredtweet)
    {
      LOG.info("Unfiltered Tweet =>" + unfilteredtweet);
      filter(unfilteredtweet);
    }
  } ;

  public final transient DefaultOutputPort<String> filteredtweet = new DefaultOutputPort<>();

  public void filter(String unfilteredtweet)
  {    String unfilteredtweet1 = unfilteredtweet.toLowerCase();
    String [] unfiltweet = unfilteredtweet1.split(" ");
    boolean contains = false;

//iterate the String array
    for(int i=0; i < unfiltweet.length; i++){

      //check if string array contains the string
      if(unfiltweet[i].equals(strFind1)){

        //string found
        contains = true;
        break;

      }
    }


    //emits if the particular word is found in the tweet.
    if(contains == true ){
      // will be displayed in LOGS in GUI
      String filtertweet = Arrays.toString(unfiltweet);
      LOG.info("Filtered Tweet =>" + filtertweet);
      filteredtweet.emit(unfilteredtweet);
    }
  }

}

