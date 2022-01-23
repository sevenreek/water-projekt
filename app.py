from flask import Flask, render_template
import requests as rq
from webscrap_api import scrap
from pyspark.sql.functions import udf
import hashlib
from pyspark.sql import SparkSession
from pyspark.ml import feature, classification, evaluation
from pyspark.ml.recommendation import ALS

spark = SparkSession.builder.appName("ml").getOrCreate()
ratings = spark.read.csv("mini.csv", sep=",", header=True, inferSchema=True)
train,test = ratings.randomSplit([0.8,0.2], 42)

idx = feature.StringIndexer(inputCol="steamid", outputCol="steamid_unique")
idx.setHandleInvalid('skip')
train = train.dropna()
indexer = idx.fit(train)
train = indexer.transform(train)

#train = train.withColumn("steamid_hash", hash_udf(train["steamid"]).cast('int'))
#train = train.withColumn("appid_hash", hash_udf(train["appid"]).cast('int'))


als = ALS(maxIter=5, regParam=0.01, userCol="steamid_unique", itemCol="appid", ratingCol="playtime_minutes", seed=420, coldStartStrategy="drop")
als_model = als.fit(train)

app = Flask(__name__)

@app.route('/')
def index():
    return "Hello world!"

@app.route('/recommend/<string:steam_id>')
def get_recommendation(steam_id):
    steamid, played_games = scrap(steam_id)
    tag_df = spark.createDataFrame([(steamid, *g) for g in played_games]).toDF(*train.columns[:3])
    

    als_model.recommendForUserSubset
    pred = model.transform(df)
    pred.show(5)
    games = [{'appid':420}]
    return render_template('index.jinja', games=games, prediction=pred)

    
if __name__ == "__main__":
    app.run()