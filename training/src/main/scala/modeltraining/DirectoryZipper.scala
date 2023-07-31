package edu.njit.cs643.dmg56

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Path, Paths, SimpleFileVisitor, FileVisitResult}
import java.nio.file.attribute.BasicFileAttributes
import java.util.zip.{ZipEntry, ZipOutputStream}

object DirectoryZipper {
  def zipDirectory(directoryPath: String, zipFilePath: String): Unit = {
    val zipOutput = new ZipOutputStream(new FileOutputStream(zipFilePath))

    try {
      Files.walkFileTree(Paths.get(directoryPath), new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val inputFile = file.toFile
          val relativePath = Paths.get(directoryPath).relativize(file)
          val zipEntry = new ZipEntry(relativePath.toString)
          zipOutput.putNextEntry(zipEntry)

          val fileInput = new FileInputStream(inputFile)
          val buffer = new Array[Byte](1024)
          var bytesRead = fileInput.read(buffer)
          while (bytesRead != -1) {
            zipOutput.write(buffer, 0, bytesRead)
            bytesRead = fileInput.read(buffer)
          }

          fileInput.close()
          zipOutput.closeEntry()
          println(s"Added file: ${relativePath.toString} to the zip archive.")
          FileVisitResult.CONTINUE
        }

        override def visitFileFailed(file: Path, exc: java.io.IOException): FileVisitResult = {
          println(s"Failed to access file: ${file.toAbsolutePath}. Reason: ${exc.getMessage}")
          FileVisitResult.CONTINUE
        }
      })
    } finally {
      zipOutput.close()
    }
  }
}