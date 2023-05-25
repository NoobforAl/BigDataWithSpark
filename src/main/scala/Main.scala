import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.ip2location._

import java.time._

object Main extends App {
  val loc = new IP2Location
  val useMMF = true
  loc.Open("/home/cppte/ScalaTest/DB.BIN", useMMF)

  val spark = SparkSession.builder
    .appName("TestFinal")
    .master("local[*]")
    .getOrCreate()

  def intToIp(ipInt: Int): String = {
    val octet1: Int = (ipInt >> 24) & 0xff
    val octet2: Int = (ipInt >> 16) & 0xff
    val octet3: Int = (ipInt >> 8) & 0xff
    val octet4: Int = ipInt & 0xff
    s"$octet1.$octet2.$octet3.$octet4"
  }

  def fc(start: Int, end: Int): List[String] = {
    var Country: List[String] = List()
    var i = 0

    // get 60 countryName from ip range
    for (ip <- start to end) {
      if (i == 60) return Country

      try {
        val rec = loc.IPQuery(intToIp(ip))
        if ("OK" == rec.getStatus) {
          val countryName: String = rec.getCountryShort()
          if ("-" != countryName) {
            Country = Country :+ countryName
            i += 1
          }
        }
      } catch {
        case e: Exception => {}
      }
    }
    return Country
  }

  val findCountryName = udf((start: Int, end: Int) => fc(start, end))
  spark.udf.register("findCountryName", findCountryName)

  var df = spark.read
    .option("header", "true")
    .schema("start INT,end INT")
    .csv("/home/cppte/ScalaTest/range_ips.csv")

  df = df
    .withColumn("Country", findCountryName(col("start"), col("end")))
    .drop("start", "end")
    .select(col("Country"), explode(col("Country")))
    .drop("Country")

  loc.Close()

  df = df
    .groupBy(col("col"))
    .agg(
      count(col("col"))
        .alias("count")
    )
    .sort(col("count"))
    .cache()

  df.write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("/home/cppte/ScalaTest/out")

  df.unpersist()
  df.show(5)
  Thread.sleep(10000L)
}
