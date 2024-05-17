import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types.IntegerType 

object FirstCode {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder() //Initialize a Spark session.
      .appName("Amazon and Yelp Data Analysis") //Name the Spark application.
      .getOrCreate() //Create a Spark session instance if it does not exist.

    import spark.implicits._ 

    //Load the Amazon dataset and cast numeric columns.
    val amazonDf = spark.read.option("header", "true").csv("Reviews.csv")
      .withColumn("HelpfulnessNumerator", $"HelpfulnessNumerator".cast(IntegerType))
      .withColumn("HelpfulnessDenominator", $"HelpfulnessDenominator".cast(IntegerType))
      .withColumn("Score", $"Score".cast(IntegerType))

    //Calculate mean of numerical columns in the Amazon dataset.
    val amazonMean = amazonDf.select(
      mean($"HelpfulnessNumerator").as("mean_helpfulnessnumerator"), 
      mean($"HelpfulnessDenominator").as("mean_helpfulnessdenominator"), 
      mean($"Score").as("mean_score"),
    )
    amazonMean.show()

    //Calculate standard deviation for the Score column in the Amazon dataset.
    val stddevAmazon = amazonDf.select(
      stddev($"score").as("stddev_score")
    )
    stddevAmazon.show()

    //Calculate median using approximate percentile for numerical columns in the Amazon dataset.
    val amazonMedian = amazonDf.select(
      expr("percentile_approx(HelpfulnessNumerator, 0.5)").as("median_helpfulnessnumerator"),
      expr("percentile_approx(HelpfulnessDenominator, 0.5)").as("median_helpfulnessdenominator"),
      expr("percentile_approx(Score, 0.5)").as("median_score"),
    )
    amazonMedian.show()

    //Define a function to calculate mode for a given column in a DataFrame.
    def calculateMode(df: org.apache.spark.sql.DataFrame, columnName: String): Int = {
      val modeRow = df.groupBy(columnName).count()
        .orderBy($"count".desc)
        .first()

      modeRow.getAs[Int](columnName)
    }

    //Calculate mode for numerical columns in the Amazon dataset.
    val modeHelpfulnessNumerator = calculateMode(amazonDf, "HelpfulnessNumerator")
    val modeHelpfulnessDenominator = calculateMode(amazonDf, "HelpfulnessDenominator")
    val modeScore = calculateMode(amazonDf, "Score")

    println(s"Mode of Helpfulness Numerator: $modeHelpfulnessNumerator")
    println(s"Mode of Helpfulness Denominator: $modeHelpfulnessDenominator")
    println(s"Mode of Score: $modeScore")

    //Perform text and date formatting on Amazon dataset.
    val amazonPreprocessed = amazonDf
      .withColumn("UserId", lower(trim($"UserId"))) //Normalize UserId by making it lowercase and trimming spaces.
      .withColumn("Time", from_unixtime($"Time", "MM-dd-yyyy")) //Convert UNIX timestamp to readable date format.

    //Create a binary column based on the Score condition in the Amazon dataset.
    val amazonFinal = amazonPreprocessed.withColumn("high_score", when($"Score" >= 4, 1).otherwise(0))
    amazonFinal.show()

    //Load the Yelp dataset and cast numeric columns.
    val df = spark.read.option("header", "true").csv("yelp.csv")
      .withColumn("stars", $"stars".cast("int"))
      .withColumn("cool", $"cool".cast("int"))
      .withColumn("useful", $"useful".cast("int"))
      .withColumn("funny", $"funny".cast("int"))

    //Calculate mean for numerical columns in the Yelp dataset.
    val descriptiveStats = df.select(
      mean($"stars").as("mean_stars"),
      mean($"cool").as("mean_cool"),
      mean($"useful").as("mean_useful"),
      mean($"funny").as("mean_funny")
    )
    descriptiveStats.show()

    //Calculate standard deviation for the stars column in the Yelp dataset.
    val stddevStats = df.select(
      stddev($"stars").as("stddev_stars")
    )
    stddevStats.show()

    //Calculate median using approximate percentile for numerical columns in the Yelp dataset.
    val medianDf = df.select(
      expr("percentile_approx(stars, 0.5)").as("median_stars"),
      expr("percentile_approx(cool, 0.5)").as("median_cool"),
      expr("percentile_approx(useful, 0.5)").as("median_useful"),
      expr("percentile_approx(funny, 0.5)").as("median_funny")
    )
    medianDf.show()

    //Calculate mode for numerical columns in the Yelp dataset.
    val modeStars = calculateMode(df, "stars")
    val modeCool = calculateMode(df, "cool")
    val modeUseful = calculateMode(df, "useful")
    val modeFunny = calculateMode(df, "funny")

    println(s"Mode of Stars: $modeStars")
    println(s"Mode of Cool: $modeCool")
    println(s"Mode of Useful: $modeUseful")
    println(s"Mode of Funny: $modeFunny")

    //Perform date and text formatting on Yelp dataset.
    val cleanedDf = df
      .withColumn("date", date_format(to_date($"date", "yyyy-MM-dd"), "MM-dd-yyyy")) //Convert date to MM-dd-yyyy format.
      .withColumn("user_id", lower(trim($"user_id"))) //Normalize user_id by making it lowercase and trimming spaces.

    //Create a binary column based on the stars condition in the Yelp dataset.
    val finalData = cleanedDf.withColumn("high_rating", when($"stars" >= 4, 1).otherwise(0))
    finalData.show()

    spark.stop() //Stop the Spark session.
  }
  
}
