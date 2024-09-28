import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json , col
from pyspark.sql.types import StructType , StructField , TimestampType , IntegerType ,StringType
from pyspark.sql.functions import sum as _sum

if __name__ == '_main_' :
    spark = ( SparkSession.builder
              .appName("RealTimeVotingEngineering")
              .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3')
              .config('spark.jars','C:\\Users\\pc\\PycharmProjects\\RealTimeVotingProject\\postgresql-42.7.4.jar')
              .config('spark.sql.adaptive.enable','false')
                .getOrCreate())

    vote_schema = StructType([
        StructField ('voter_id' , StringType() ,True),
        StructField('candidate_id', StringType(), True),
        StructField('voting_time', TimestampType(), True),
        StructField('voter_name', StringType(), True),
        StructField('party_affiliation', StringType(), True),
        StructField('biography', StringType(), True),
        StructField('campaign_platform', StringType(), True),
        StructField('photo_url', StringType(), True),
        StructField('candidate_name', StringType(), True),
        StructField('date_of_birth', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('nationality', StringType(), True),
        StructField('registration_number', StringType(), True),
        StructField('address', StructType([
            StructField('street', StringType(), True),
            StructField('city', StringType(), True),
            StructField('state', StringType(), True),
            StructField('country', StringType(), True),
            StructField('postcode', StringType(), True),

        ]),True),
        StructField('voter_id', StringType(), True),
        StructField('phone_number', StringType(), True),
        StructField('picture', StringType(), True),
        StructField('registred_age', IntegerType(), True),
        StructField('vote', IntegerType(), True),

    ])

    # Read streaming data from Kafka
    votes_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "votes_topic")
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) as json_str")
                .select(from_json(col("json_str"), vote_schema).alias("data"))
                .select("data.*"))

    # Data preprocessing : typecasting and watermarking

    votes_df = votes_df.withColumn('voting_time',col('voting_time').cast(TimestampType())) \
                .withColumn('vote',col('vote').cast(IntegerType()))
    enriched_votes_df = votes_df.withWatermark('voting_time','1 minute')

    # Aggregate votes per candidate and turnout by location

    votes_per_candidate = enriched_votes_df.groupBy('candidate_id','candidate_name','party_affiliation','photo_url').agg(_sum('votes').alias('total_votes'))
    turnout_by_location = enriched_votes_df.groupBy('address.state').count().alias('total_votes')
    votes_per_candidate_to_kafka = (votes_per_candidate.selectExpr('to_json(struct(*)) AS value')
    .writeStream
    .format('kafka')
    .option('kafka.bootstrap.servers','localhost:9092')
    .option('topic','aggregated_votes_per_candidate')
    .option('checkpointLocation','C:\\Users\\pc\\PycharmProjects\\RealTimeVotingProject\\checkpoints\\checkpoints1')
    .outputMode('update')
    .start()
    )


    turnout_by_location_to_kafka = (votes_per_candidate.selectExpr('to_json(struct(*)) AS value')
    .writeStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'localhost:9092')
    .option('topic', 'aggregated_turnout_by_location')
    .option('checkpointLocation', 'C:\\Users\\pc\\PycharmProjects\\RealTimeVotingProject\\checkpoints\\checkpoints2')
    .outputMode('update')
    .start()
    )

    # Await termination for the streaming queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()