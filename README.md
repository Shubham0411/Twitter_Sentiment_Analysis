#Twitter Sentiment Analysis

Objective
We will be doing sentiment analysis on data collected from the Twitter streaming API. In this we will search for tweets matching certain keywords using Filter Operator. Sentiment analysis lets us analyze what Twitter users think about the topics of their tweets

Example
If  there are two tweets like-
1) The day was not good, all the bad things just happened with me.
2) Today was the worst day of my life, i lost someone very close to me.

In this I will configure the words in properties.xml than "FILTER" operator will search for that particular word/s in the received tweet and forward to the next operator and not the other tweets. 
In this situation I will describe that only the tweet which contains the word "bad" should be forwarded and so in this case 1st tweet will be forwarded as only it contains the word "bad" and not the 2nd tweet.

The steps to run this application
 
Step 1: Build the code.
       shell> mvn clean install

Upload the target/twitter-sentiment-1.0-SNAPSHOT.apa to the UI console or launch it from command line using apexcli.

Step 2: During launch use src/main/resources/META-INF/properties.xml as a custom configuration file.
