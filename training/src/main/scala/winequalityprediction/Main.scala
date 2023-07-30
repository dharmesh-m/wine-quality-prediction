import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.types._
import java.io.PrintWriter
import Constants._
import DirectoryZipper._
import S3Uploader._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Wine Quality Prediction")
      .master("local[*]")
      .getOrCreate()

    val inputPath = "data/TrainingDataset.csv"
    //val inputPath = "s3a://dmg56-wine-quality-prediction/data/TrainingDataset.csv"
    val outputPath = "data/output"

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

    val trainingData: DataFrame = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(customSchema)
      .csv(inputPath);

    val validationData: DataFrame = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(customSchema)
      .csv(inputPath);  

    // Identify the input features and the target label
    val inputColumns = trainingData.columns.filter(_ != QUALITY)
    
    val assembler = new VectorAssembler()
      .setInputCols(inputColumns)
      .setOutputCol("features")

    val rfc = new RandomForestClassifier()
      .setLabelCol(QUALITY)
      .setFeaturesCol("features")

    // Build the ML pipeline
    val pipeline = new Pipeline().setStages(Array(assembler, rfc))

    // Fit the pipeline to the training data to train the logistic regression model
    val model = pipeline.fit(trainingData)

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

    println("Saving model to local file system")
    val modelVersion = ModelVersionStringGenerator.generateVersionString();
    val directoryPath = s"output/$modelVersion";
    model.write.save(directoryPath)

    println("Creating zip file from model output")
    val zipFilePath = s"output/$modelVersion.zip";
    DirectoryZipper.zipDirectory(directoryPath, zipFilePath)

    println(s"Uploading file to S3 bucket: $BUCKET_NAME")
    val keyName = s"models/$modelVersion/$modelVersion.zip"
    S3Uploader.uploadFile(BUCKET_NAME, keyName, zipFilePath)

    println(s"Uploading F1 Score file")
    uploadF1Score(f1Score, s"output/$modelVersion/F1Score", s"models/$modelVersion/F1Score.txt")

    print("Press any key to continue: ")

    // Read a line of text from the user
    val input = scala.io.StdIn.readLine()

    spark.stop()
  }

  def uploadF1Score(f1Score: Double, filePath: String, s3KeyName: String): Unit = {
    // Write the F1 score to the text file
    val writer = new PrintWriter(filePath)
    writer.write(s"F1 Score: $f1Score")
    writer.close()

    S3Uploader.uploadFile(BUCKET_NAME, s3KeyName, filePath)
  }
}
