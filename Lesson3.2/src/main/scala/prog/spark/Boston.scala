package prog.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Boston {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Необходимо Boston <file1> <file2> <folderToSave>")
      sys.exit(-1)
    }

    val spark = SparkSession.builder.appName("Boston").getOrCreate()

    var crime = spark.read.option("header", "true").option("inferShema", "true").csv(args(0))
    var offense_codes = spark.read.option("header", "true").option("inferShema", "true").csv(args(1))

    var windowSpec = Window.partitionBy("DISTRICT").orderBy(desc("count"))

    var freq_crime = crime
      .groupBy("DISTRICT", "OFFENSE_CODE_GROUP")
      .count
      .sort("DISTRICT", "count")
      .withColumn("row_number",row_number.over(windowSpec))
      .filter($"row_number" < 4)
      .drop("count", "row_number")
      .groupBy("DISTRICT")
      .agg(concat_ws(", ", collect_list($"OFFENSE_CODE_GROUP")).alias("frequent_crime_types"))
      .withColumnRenamed("DISTRICT", "DISTRICT_")


    var district_latlong = crime
      .groupBy("DISTRICT")
      .agg(
        avg("Lat").alias("lat"),
        avg("Long").alias("lng"))
      .withColumnRenamed("DISTRICT", "DISTRICT_")


    var data = crime
      .groupBy("DISTRICT","MONTH")
      .count
      .withColumnRenamed("count", "crimes_total")
      .groupBy("DISTRICT")
      .agg(
        sum("crimes_total").alias("crimes_total"),
        callUDF("percentile_approx", col("crimes_total"), lit(0.5)).alias("crimes_monthly"))
      .join(freq_crime, freq_crime("DISTRICT_") === crime("DISTRICT"))
      .drop("DISTRICT_")
      .join(district_latlong, district_latlong("DISTRICT_") === crime("DISTRICT"))
      .drop("DISTRICT_")


    data.coalesce(1).write.format("parquet").mode("append").save(args(2))

    spark.stop()
  }
}
