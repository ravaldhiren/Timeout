package com.scalaprec

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import spark.implicits._


object TimeOuttask {


//Read JSON File
val spark = SparkSession.builder().appName("TimeOutTask").config("spark.some.config.option", "timeout").getOrCreate()
import spark.implicits._
val df1 = spark.read.json("C:/ZTest/Timeout/users.json")

//Making View for JSON file
df1.createOrReplaceTempView("users")

val df2 = spark.read.json("C:/ZTest/Timeout/venues.json")

//Making View for JSON file
df2.createOrReplaceTempView("venues")


val placetogo = spark.sql("select venues.Name from venue where venue.Name not in (select venues.name from users,venues where users.Wont_eat = venue.Food);")

placetogo.show

val placetoavoid = spark.sql("select venues.Name from venue where venue.Name in (select venues.name from users,venues where users.Wont_eat = venue.Food);")

placetoavoid.show

}