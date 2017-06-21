package com.example.java.twittersentiment;

/*
This class analyses the sentiment of a
particular tweet using Naive Bayes Algorithm
by training the model using the given DATA SET
and stores the output in HADOOP using GenericOutputFileWriter */


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

  DataInAnalyzer dataInAnalyzer = new DataInAnalyzer();


  public static final Logger LOG = LoggerFactory.getLogger(SentimentAnalyzer.class);



  public final transient DefaultOutputPort<String> positiveTweetOutputFromAnalyzer = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<String> negativeTweetOutputFromAnalyzer = new DefaultOutputPort<>();


  public final transient DefaultInputPort<Data> input = new DefaultInputPort<Data>()
  {
    @Override
    public void process(Data data)
    {
      classifyNewTweet(data);
    }
  };


  DoccatModel model;


  protected String filePath;

  public String getFilePath()
  {
    return filePath;
  }
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }


  public void trainModel() {
    InputStream dataIn = null;
    try {

      LOG.info("I have been called here.");
      //this define where the file used for training the model is stored.
      dataIn = new FileInputStream(getFilePath());
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


  public void classifyNewTweet(Data data) {
    dataInAnalyzer.originalTweetInAnalyzer = data.originalTweetInCleanser;
    dataInAnalyzer.cleanedTweetInAnalyzer = data.cleanedTweetInCleanser;
    DocumentCategorizerME myCategorizer = new DocumentCategorizerME(model);
    double[] outcomes = myCategorizer.categorize(dataInAnalyzer.cleanedTweetInAnalyzer);
    String category = myCategorizer.getBestCategory(outcomes);

    if (category.equalsIgnoreCase("1")) {

      String result = "positive";
      LOG.info("Tweet : Sentiment {} ==> {}", dataInAnalyzer.originalTweetInAnalyzer, result);
      positiveAnalyzedOutputTweet(dataInAnalyzer.originalTweetInAnalyzer,result);

    } else {

      String result = "negative";
      LOG.info("Tweet : Sentiment {} ==> {}", dataInAnalyzer.originalTweetInAnalyzer, result);


      if(result=="positive")
        positiveAnalyzedOutputTweet(dataInAnalyzer.originalTweetInAnalyzer,result);
      else
      negativeAnalyzedOutputTweet(dataInAnalyzer.originalTweetInAnalyzer,result);
    }
  }


  public void positiveAnalyzedOutputTweet(String originalTweetInAnalyzer,String result)
  {
    String positiveTweetFromAnalyzer = originalTweetInAnalyzer + " ==>  " + result ;
    positiveTweetOutputFromAnalyzer.emit(positiveTweetFromAnalyzer);
  }


  public void negativeAnalyzedOutputTweet(String originalTweetInAnalyzer,String result)
  {
    String negativeTweetFromAnalyzer = originalTweetInAnalyzer + " ==>  " + result ;
    negativeTweetOutputFromAnalyzer.emit(negativeTweetFromAnalyzer);
  }


  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    trainModel();
  }
}


class DataInAnalyzer
{
  String originalTweetInAnalyzer;
  String cleanedTweetInAnalyzer;
}