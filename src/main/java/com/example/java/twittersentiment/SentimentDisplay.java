package com.example.java.twittersentiment;


/*
This class here receives the original tweet
and the sentiment value from the previous operator
and stores them in the file for the user to read it.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Created by shubham on 6/4/17.
 */
public class SentimentDisplay extends AbstractFileOutputOperator<String>
{
  int flag = 0;

  byte[] stringFile;
  public static final Logger LOG = LoggerFactory.getLogger(SentimentDisplay.class);

  //int sentimentvalue2;
  String sentimentvalue2;
  public final transient DefaultInputPort<String> sentimentscore1 = new DefaultInputPort<String>()
  {
    @Override
    public void process(String sentimentvalue1)
    {
      sentimentoftweet(sentimentvalue1);
    }
  };


//  public int sentimentoftweet(String sentimentvalue1)
  public String sentimentoftweet(String sentimentvalue1)

  {
     sentimentvalue2 = sentimentvalue1;
    //sentimentvalue2 = Integer.parseInt(sentimentvalue1);
    return sentimentvalue2;
  }

  public void importedtweet(String originaltweet)
  {
    LOG.debug("Tweet : Sentiment --> {} ==> {}", originaltweet, sentimentvalue2);
    LOG.info("Tweet : Sentiment --> {} ==> {}", originaltweet, sentimentvalue2);
    //sentimentdisp(originaltweet, sentimentvalue2);
    sentimentdisp(originaltweet, sentimentvalue2);

  }

  public byte[] sentimentdisp(String originaltweet, String sentimentvalue2)
  {


    File file=null;
      try{
        // the path here defines where the file is stored to be accessed by the user

        file = new File("/home/shubham/Documents/MyFile.txt");

        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(originaltweet +"==>" + sentimentvalue2 );
      bw.newLine();
      bw.close();
      stringFile = FileUtils.readFileToByteArray(file);
    } catch (IOException e) {
      e.printStackTrace();
    }

      return stringFile;
  }

  @Override
  protected String getFileName(String s)
  {
      return "MyText.txt";
  }

  @Override
  protected byte[] getBytesForTuple(String s)
  {
    LOG.info("Tweet : Sentiment {} ==> {}", s, sentimentvalue2);
    return sentimentdisp(s,sentimentvalue2);
  }
}
