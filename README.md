Here is an alternative version of the GitHub ReadMe with slight variations in wording:

---

# Big Data Sentiment Analysis of Online Food Reviews

## Team Members
- **Jessica Yang** (zy1216)
- **Cathy Liu** (cyl4949)

## Project Overview

This project focuses on performing sentiment analysis on a large collection of online food reviews to gain insights into customer satisfaction and preferences. The goal is to leverage big data tools and techniques to process extensive datasets, developing a robust system capable of accurately categorizing reviews into various sentiment classes.

## Motivation

The competitive nature of the food industry makes customer feedback essential for businesses. Understanding the sentiments in online reviews enables businesses to fine-tune their services and products, thereby enhancing their competitive edge and profitability. This project aims to transform raw review data into actionable business intelligence.

## Data Sources

We used two main datasets for this project:
- **Yelp Open Dataset**: Approximately 6 million reviews related to over 188,000 businesses.
- **Amazon Fine Food Reviews Dataset**: Over 500,000 food product reviews.

## Methodology

Our approach involved several key stages:

1. **Data Ingestion**: Collecting data from Yelp and Amazon and storing it using Apache Hadoop's HDFS.
2. **Data Cleaning and Profiling**: Removing malformed entries, converting timestamps, and generating summary statistics using Apache Spark.
3. **Sentiment Analysis**: Utilizing NLP techniques and machine learning algorithms within the Apache Spark framework to classify reviews into positive, neutral, or negative sentiments.

### Tools and Technologies
- **Apache Hadoop**: For distributed data storage and processing.
- **Apache Spark**: For fast big data analytics.
- **MLlib**: Machine learning library in Apache Spark.
- **Natural Language Processing (NLP)**: Techniques for text preprocessing and sentiment analysis.

## Key Findings

- Yelp reviews tend to show a higher frequency of positive sentiments compared to Amazon reviews.
- Temporal analysis indicates that positive sentiments peak during holiday seasons on both platforms.
- Service quality and food quality are the main factors driving positive reviews on Yelp.
- Product quality and delivery experience are significant for Amazon reviews.

## Benefits

The insights derived from this project benefit both business owners and consumers. For businesses, the analysis helps them understand customer preferences and areas needing improvement. For consumers, it offers a summarized understanding of a restaurant's strengths and weaknesses, aiding them in making more informed dining choices.

## Conclusion

This project showcases the potential of big data technologies to drive strategic business decisions and enhance service offerings in the competitive food industry. By providing businesses with precise, data-driven insights into customer feedback, our sentiment analysis helps prioritize areas for improvement and boosts customer satisfaction and loyalty.

## Code Structure

Below is an overview of the code files used in this project:

### Data Exploration
- `UniqueRecs.java`: Main driver for that finds out exploratory information of the Yelp dataset. 
- `UniqueRecsMapper.java`: Mapper class that finds out exploratory information of the Yelp daataset. 
- `UniqueRecsReducer.java`: Reducer class that finds out exploratory information of the Yelp dataset. 
- `FirstCode.scala`: Scala file that contains code on calculating mean, median, mode, standard deviation, and some data cleaning. 

### Data Cleaning

- `Clean.java`: Original Main driver for data cleaning on Yelp reviews.
- `CleanMapper.java`: Original Mapper class for cleaning Yelp reviews.
- `CleanReducer.java`: Original Reducer class for cleaning Yelp reviews.
- `YelpClean.java`: New Main driver for data cleaning on Yelp reviews.
- `YelpCleanMapper.java`: New Mapper class for cleaning Yelp reviews.
- `YelpCleanReducer.java`: New Reducer class for cleaning Yelp reviews.

### Sentiment Analysis

- `SentimentAnalysis.java`: Main driver for sentiment analysis on Yelp reviews.
- `SentimentMapper.java`: Mapper class for sentiment analysis on Yelp reviews.
- `SentimentReducer.java`: Reducer class for sentiment analysis on Yelp reviews.
### License

This project is licensed under the MIT License - see the LICENSE file for details.

