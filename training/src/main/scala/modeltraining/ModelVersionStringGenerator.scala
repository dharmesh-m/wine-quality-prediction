package edu.njit.cs643.dmg56

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import Constants.WINE_QUALITY_PREDICTION_MODEL_FILE_NAME

object ModelVersionStringGenerator {
  def generateVersionString(): String = {
    // Get the current date and time
    val currentDateTime = LocalDateTime.now()

    // Define a date-time format for the version string
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")

    // Format the current date and time as a version string
    val versionString = currentDateTime.format(formatter)

    s"$WINE_QUALITY_PREDICTION_MODEL_FILE_NAME-$versionString"
  }
}
