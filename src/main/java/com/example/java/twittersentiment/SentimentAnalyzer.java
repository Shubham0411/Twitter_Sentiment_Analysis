package com.example.java.twittersentiment;

/*
This class analyses the sentiment of a
particular tweet using Naive Bayes Algorithm
and forwards it to the SentimentDisplay class
 */


/**
 * Created by shubham on 16/5/17.

 */

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.doccat.DocumentSampleStream;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;

  public class SentimentAnalyzer extends BaseOperator {
    public static final Logger LOG = LoggerFactory.getLogger(SentimentAnalyzer.class);

    public final transient DefaultOutputPort<String> sendOriginaltweet_2 = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<String> sentimentscore_2 = new DefaultOutputPort<>();

    public final transient DefaultInputPort<String> originaltweet_2 = new DefaultInputPort<String>()
    {
      @Override
      public void process(String originaltweet_2)
      {
        sendOriginaltweet_2.emit(originaltweet_2);
      }
    };


    public final transient DefaultInputPort<String> inputtweet1 = new DefaultInputPort<String>()
    {
      @Override
      public void process(String refinedtweet_2)
      {
        classifyNewTweet(refinedtweet_2);
      }
    };

    DoccatModel model;


    public void trainModel() {
      InputStream dataIn = null;
      try {

        LOG.info("I have been called here.");

        //this define where the file used for training the model is stored.
        dataIn = new FileInputStream("/home/shubham/myapps/twittersentiment-03/mydtapp/src/main/resources/META-INF/tweets");
        ObjectStream lineStream = new PlainTextByLineStream(dataIn, "UTF-8");
        ObjectStream sampleStream = new DocumentSampleStream(lineStream);
        // Specifies the minimum number of times a feature must be seen
        int cutoff = 2;
        int trainingIterations = 30;
        model = DocumentCategorizerME.train("en", sampleStream, cutoff,
          trainingIterations);
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (dataIn != null) {
          try {
            dataIn.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }



    public void classifyNewTweet(String tweet) {
      DocumentCategorizerME myCategorizer = new DocumentCategorizerME(model);
      double[] outcomes = myCategorizer.categorize(tweet);
      String category = myCategorizer.getBestCategory(outcomes);

      if (category.equalsIgnoreCase("1")) {
        System.out.println("The tweet is positive :) ");
        String result = "positive";
        sentimentscore_2.emit(result);
      } else {
        System.out.println("The tweet is negative :( ");
        String result = "negative";
        sentimentscore_2.emit(result);
      }
    }
    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      trainModel();
      //    prepare1();
    }
  }

