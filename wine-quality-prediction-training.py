import argparse

from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

FIXED_ACIDITY = "\"\"\"\"\"fixed acidity\"\"\"\""
VOLATILE_ACIDITY = "\"\"\"\"volatile acidity\"\"\"\""
CITRIC_ACID = "\"\"\"\"citric acid\"\"\"\""
RESIDUAK_SUGAR = "\"\"\"\"residual sugar\"\"\"\""
CHLORIDES = "\"\"\"\"chlorides\"\"\"\""
FREE_SULFUR_DIOXIDE = "\"\"\"\"free sulfur dioxide\"\"\"\""
TOTAL_SULFUR_DIOXIDE = "\"\"\"\"total sulfur dioxide\"\"\"\""
DENSITY = "\"\"\"\"density\"\"\"\""
PH = "\"\"\"\"pH\"\"\"\""
SULPHATES = "\"\"\"\"sulphates\"\"\"\""
ALCOHOL = "\"\"\"\"alcohol\"\"\"\""
QUALITY = "\"\"\"\"quality\"\"\"\"\""
LABEL = "quality"

# Define the custom schema
SCHEMA = StructType([
    StructField(FIXED_ACIDITY, DoubleType(), True),
    StructField(VOLATILE_ACIDITY, DoubleType(), True),
    StructField(CITRIC_ACID, DoubleType(), True),
    StructField(RESIDUAK_SUGAR, DoubleType(), True),
    StructField(CHLORIDES, DoubleType(), True),
    StructField(FREE_SULFUR_DIOXIDE, DoubleType(), True),
    StructField(TOTAL_SULFUR_DIOXIDE, DoubleType(), True),
    StructField(DENSITY, DoubleType(), True),
    StructField(PH, DoubleType(), True),
    StructField(SULPHATES, DoubleType(), True),
    StructField(ALCOHOL, DoubleType(), True),
    StructField(QUALITY, IntegerType(), True)
])

def load_csv_data(spark, data_source):
    data = spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .schema(SCHEMA) \
        .csv(data_source)
    
    for col in data.columns:
        data = data.withColumnRenamed(col, col.replace('"', ''))
    
    return data

def predict_wine_quality(data_source, output_uri, validation_data_source):
    with SparkSession.builder.appName("Train Wine Quality Prediction Model").getOrCreate() as spark:
        if data_source is not None:
            # Read the CSV file
            training_data = load_csv_data(spark, data_source)

            # Prepare the features and label columns for training
            feature_cols = training_data.columns[:-1]
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

            # Create the RandomForest classifier
            rf = RandomForestClassifier(labelCol=LABEL, featuresCol="features", numTrees=10, seed=42)

            # Create a pipeline for the entire training process
            pipeline = Pipeline(stages=[assembler, rf])

            # Train the model
            model = pipeline.fit(training_data)

            # Save the trained model to an S3 bucket
            model.write().overwrite().save(output_uri)

            print("Model Training Completed")

        if validation_data_source is not None:
            # Make predictions on the validation data
            validation_data = load_csv_data(spark, validation_data_source)
            predictions = model.transform(validation_data)

            # Evaluate the model with F1-score
            evaluator = MulticlassClassificationEvaluator(labelCol=LABEL, predictionCol="prediction", metricName="f1")
            f1_score = evaluator.evaluate(predictions)

            print(f"F1 Score: {f1_score}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    parser.add_argument(
        '--validation_data_source', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    predict_wine_quality(args.data_source, args.output_uri, args.validation_data_source)
