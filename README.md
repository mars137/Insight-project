# Ad-Streams - "Every click matters"
Repository for my data engineering project done during Insight fellows program

## tl;dr

 * [Slides](bit.ly/ads1989)
 * [Live Demo](adtstreams.info)


## 1. Introduction

Google recently announced a new product called digital attribution which called for data-driven attribution and considering all responses of a user instead of just the last click.

Digital marketing advertisements are continuously affecting target users to varying degrees and these users in turn may respond by showing interest through clicking, researching the event, looking for deals or buying the product. All of this is happening simultaneously for all users. Each of these users can then be thought to be in a different position on the conversion funnel - henceforth referred to as propensity for conversion or simply propensity .Based on users current propensity the most productive next digital marketing move can be highly customized and optimized for that particular user. This type of highly reactive digital marketing platform would require continuously evaluating each user's current propensity.


## 1.1 Project Details

This project demonstrates both the engineering, business and human aspects of solving this problem. A logistic regression scoring based approach to computing propensity score based on user history, implemented this approach on a streaming infrastructure, separates the business logic from the details on underlying streaming infrastructure through schema formats The overall end to end technique demonstrated here could be applied in a straightforward manner to any of the other specific problem involving digital marketing analytics.


For this project , I am considering four kinds of user events - clicks(CL) , impressions(IM) , paid search(PS) and  past conversions(CN). As shown in the picture the idea is to sequence the user stream though a  data streaming paradigm. The specific solution for continuous propensity scoring is based on the notion of a sequence of digital marketing treatments and user actions being continuously maintained and providing the based data on which a number of features are then computed based on the type of the event and the time elapsed since the event happened.
 
![User activity for sequencing](https://github.com/mars137/Insight-project/blob/master/images/Sequencing.png)


Below image shows the feature based on impression events.

![Feature engineering](https://github.com/mars137/Insight-project/blob/master/images/feature_engineering_1.png)


All features follow different variation of this template of type of event and time based aggregation. These features are then fed to a logistic regression scoring model to compute the propensity score. Below image shows the features based on all events for feeding to logistic scoring scoring model.

![Feature engineering](https://github.com/mars137/Insight-project/blob/master/images/feature_engineering_2.png)

The entire pipeline of gathering sequence to computation of features to computation of propensity score is built on a streaming infrastructure of Kafka as storage and messaging layer, kafka-streams for business logic processing layer, avro as schema and data representation, serializing and deserializing format, Cassandra  as the distributed columnar database to store propensity over time and python  flask as the front end for visualization. In the pipeline data in motion is depicted through pipes and data at rest (cassandra sink) is depicted by a rectangles. 


![Pipeline](https://github.com/mars137/Insight-project/blob/master/images/pipeline.png)


The data at rest a.k.a table requirement (Cassandra), say at the front end data presentation layer and table creation(KTable) when data is in motion is achieved through interpreting kafka streaming message queue as a change log and a table as a materialized view on this changelog. This cleanly resolves any contradiction between a continuous flowing stream of data and "at rest" batch processed table of data. 

Throough Avro schema formats , I have demomstrated that it is possible to separate out the implementation of the business logic from the details of streaming infrastructure. Avro is robust to schema changes and additional business logic computation can be developed and composed by incrementing the existing solution. 







