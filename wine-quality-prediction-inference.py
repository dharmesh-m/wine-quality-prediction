import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.ml import PipelineModel

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

def train_wine_quality_prediction_model(data_source, output_uri):
    with SparkSession.builder.appName("Wine Quality Prediction").getOrCreate() as spark:
        # Read the CSV file with the custom schema
        input_data = load_csv_data(spark, data_source)

        # Load Random Forest classifier model
        model = PipelineModel.load("model")

        # Perform predictions
        predictions = model.transform(input_data)

        # Write model prediction output to output_uri (s3)
        predictions.write \
            .format("json") \
            .mode("overwrite") \
            .option("header", "true") \
            .save(output_uri)
        
        print("Wine quality predictions completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    train_wine_quality_prediction_model(args.data_source, args.output_uri)
