package com.hongstudy.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.functions._


object TotalSpentByCustomerDataset {

  case class CustomerOrder(customer_id:Int, item_id:Int, amount_spent:Double)

  def main(arg: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("TotalSpentByCustomer")
      .master("local[*]")
      .getOrCreate()


    val customerOrdersSchema = new StructType()
      .add("customer_id", IntegerType, nullable = true)
      .add("item_id", IntegerType, nullable = true)
      .add("amount_spent", DoubleType, nullable = true)

    import spark.implicits._
    val customerDS = spark.read
      .schema(customerOrdersSchema)
      .csv("data/customer-orders.csv")
      .as[CustomerOrder]

    val totalByCustomer = customerDS
      .groupBy("customer_id")
      .agg(round(sum("amount_spent"), 2).alias("total_spent"))

    val totalByCustomerSorted = totalByCustomer.sort("total_spent")

    totalByCustomerSorted.show(totalByCustomer.count.toInt)



  }

}
