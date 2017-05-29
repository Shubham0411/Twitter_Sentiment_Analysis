package com.example.java.twittersentiment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by shubham on 30/3/17.
 */
public class tweetsentimentanalysis extends BaseOperator
{


  public  String AFINN_SENTIMENT_FILE_NAME = "AFINN-111.txt";
  public static final Logger LOG = LoggerFactory.getLogger(tweetsentimentanalysis.class);
  public final transient DefaultOutputPort<String> sendOriginaltweet = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<String> sentimentscore = new DefaultOutputPort<>();
  public final transient DefaultInputPort<String> originaltweet1 = new DefaultInputPort<String>()
  {
    @Override
    public void process(String originaltweet)
    {
      sendOriginaltweet.emit(originaltweet);
    }
  };
  public SortedMap<String, Integer> afinnSentimentMap = null;

  /*public final void execute(String refinedtweet) {


    final Status status = (Status) input.getValueByField("tweet");
    final int sentimentOfCurrentTweet = getsentimentoftweet( , status);
   //int stateId = Constants.MAP_STATE_CODE_ID.get(state);
    //_outputCollector.emit(new Values(state, sentimentOfCurrentTweet));
//        LOGGER.info("{}:{}", state, previousSentiment);
  }*/
  public final transient DefaultInputPort<String> cleanedtweet1 = new DefaultInputPort<String>()
  {
    @Override
    public void process(String refinedtweet)
    {
      getsentimentoftweet(refinedtweet);
    }
  };
  protected transient BufferedReader br;


 public void prepare(final Map<String, Integer> map)
  {  String text;
    afinnSentimentMap = Maps.newTreeMap();

    //this module will read from the AFINN Sentiment file [which is in the classpath] and stores the key, value pairs to a Map.
    try {

      //InputStream resourceAsStream = this.getClass().getResourceAsStream("/META-INF/AFINN-111.txt" + AFINN_SENTIMENT_FILE_NAME);
      Path filePath = new Path("file:///home/shubham/myapps/twittersentiment-02/mydtapp/src/main/resources/META-INF/AFINN-111");
      FileSystem fs = FileSystem.newInstance(filePath.toUri(), new Configuration());
      InputStream resourceAsStream = fs.open(filePath);
      //InputStream resourceAsStream = this.getClass().getResourceAsStream("META-INF/AFINN-111.txt" );

      BufferedReader reader =  new BufferedReader(new InputStreamReader(resourceAsStream));
           //if(resourceAsStream != null) {
             while ((text = reader.readLine()) != null) {
               LOG.info("Text is ", text);

               text = text.trim();
               if (text.length() == 0) {
                 continue;
               }
               ArrayList<String> tabSplit = Lists.newArrayList(Splitter.on("\t").trimResults().omitEmptyStrings().split(text));
               LOG.info("Text in tabspplit is for", tabSplit);

               afinnSentimentMap.put(tabSplit.get(0), Integer.parseInt(tabSplit.get(1)));
             }

           //}     //final URL url = Resources.getResource(AFINN_SENTIMENT_FILE_NAME);
      //final String text = Resources.toString(resourceAsStream, Charsets.UTF_8);0
      //final Iterable<String> lineSplit = Splitter.on("\n").trimResults().omitEmptyStrings().split(text);
      //List<String> tabSplit;
      //for (final String str : lineSplit) {
        //tabSplit = Lists.newArrayList(Splitter.on("\t").trimResults().omitEmptyStrings().split(str));
        //afinnSentimentMap.put(tabSplit.get(0), Integer.parseInt(tabSplit.get(1)));
      //}
    } catch (final IOException ioException) {

      LOG.error(ioException.getMessage(), ioException);
      ioException.printStackTrace();
      //Should not occur. If it occurs, we cant continue. So, exiting at this point itself.
      System.exit(1);
    }
  }

  public void getsentimentoftweet(String refinedtweet)
  {
    Iterable<String> words = Splitter.on(' ').trimResults().omitEmptyStrings().split(refinedtweet);
    // tweetcleanser tweetfilter = new tweetcleanser();
    int sentimentOfCurrentTweet = 0;
   // getAfinnSentimentMap();
    //Loop through all the words and find the sentiment of the tweet.
    for (final String word : words) {
      LOG.info("Word is ", word);

      if (afinnSentimentMap.containsKey(word)) {
        sentimentOfCurrentTweet += afinnSentimentMap.get(word);
      }
    }
    //LOG.debug("Tweet : Sentiment {} ==> {}", tweet, sentimentOfCurrentTweet);

    String sentimentvalue = String.valueOf(sentimentOfCurrentTweet);
    sentimentscore.emit(sentimentvalue);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    prepare(afinnSentimentMap);
   //    prepare1();
  }

  /*public void setAfinnSentimentMap(SortedMap<String, Integer> afinnSentimentMap)
  {
    this.afinnSentimentMap = afinnSentimentMap;
  }

  public SortedMap<String, Integer> getAfinnSentimentMap()
  {
    return afinnSentimentMap;
  }
*/
}
