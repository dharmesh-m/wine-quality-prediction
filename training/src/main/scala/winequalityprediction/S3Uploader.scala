import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import java.io.File

object S3Uploader {
  
    val region = "us-east-1" // Replace with your desired AWS region

    // Use the default profile from the credentials file (~/.aws/credentials)
    val credentialsProvider = new ProfileCredentialsProvider()

    // Create an AmazonS3 client
    val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s"s3.$region.amazonaws.com", region))
      .withCredentials(credentialsProvider)
      .build()

    def uploadFile(bucketName: String, keyName: String, filePath: String): Unit = {
      try {
        val fileToUpload = new File(filePath)
        s3Client.putObject(bucketName, keyName, fileToUpload)
        println(s"Successfully uploaded '$filePath' to S3 bucket '$bucketName' with key '$keyName'")
      } catch {
        case ex: Exception => println(s"Error uploading file: ${ex.getMessage}")
      }
    }
}
