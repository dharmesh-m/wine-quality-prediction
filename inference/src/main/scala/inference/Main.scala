import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.types._
import java.io.PrintWriter
import Constants._

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Wine Quality Prediction")
      .master("local[*]")
      .getOrCreate()

    val modelPath = "../training/output/wine-quality-prediction-model-20230730_143015"
    val validationDataPath = "../training/data/ValidationDataset.csv"
    val model = PipelineModel.load(modelPath)

    // Define your custom schema based on your dataset
    val customSchema = StructType(Seq(
      StructField(FIXED_ACIDITY, DoubleType, true),
      StructField(VOLATILE_ACIDITY, DoubleType, true),
      StructField(CITRIC_ACID, DoubleType, true),
      StructField(RESIDUAK_SUGAR, DoubleType, true),
      StructField(CHLORIDES, DoubleType, true),
      StructField(FREE_SULFUR_DIOXIDE, DoubleType, true),
      StructField(TOTAL_SULFUR_DIOXIDE, DoubleType, true),
      StructField(DENSITY, DoubleType, true),
      StructField(PH, DoubleType, true),
      StructField(SULPHATES, DoubleType, true),
      StructField(ALCOHOL, DoubleType, true),
      StructField(QUALITY, IntegerType, true)
    ))

    val validationData: DataFrame = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(customSchema)
      .csv(validationDataPath);

    // Make predictions on the test data
    val predictions = model.transform(validationData)

    // Evaluate the model using accuracy as the metric for multi-class classification
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(QUALITY) // Change "label" to your actual target column name
      .setPredictionCol("prediction")
      .setMetricName("f1")

    // Calculate the F1 score
    val f1Score = evaluator.evaluate(predictions)
    println(s"F1 Score: $f1Score")

    // Read a line of text from the user
    val input = scala.io.StdIn.readLine()

    spark.stop()
  }
}