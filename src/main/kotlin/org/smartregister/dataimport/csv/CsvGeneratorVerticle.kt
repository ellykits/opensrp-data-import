package org.smartregister.dataimport.csv

import io.vertx.core.json.JsonObject
import org.smartregister.dataimport.shared.*
import org.smartregister.dataimport.shared.model.Organization
import org.smartregister.dataimport.shared.model.OrganizationLocation
import java.io.File
import java.io.IOException
import java.nio.file.FileSystemException

class CsvGeneratorVerticle : BaseVerticle() {

  override suspend fun start() {
    super.start()

    //Delete previous files inside the directory named opensrp-data
    with(File(dataDirectoryPath)) {
      if (exists()) {
        deleteDirectory(this)
      }
      mkdirs()
    }

    try {
      vertx.eventBus().consumer<JsonObject>(EventBusAddress.CSV_GENERATE) { message ->
        vertx.executeBlocking<Unit> {
          val fileName = message.body().getString(FILE_NAME)
          val payload = message.body().getString(PAYLOAD)

          createNonExistentFile(fileName)
          when (Choices.valueOf(fileName.uppercase())) {
            Choices.ORGANIZATIONS -> writeCsv<Organization>(fileName, payload)
            Choices.ORGANIZATION_LOCATIONS -> writeCsv<OrganizationLocation>(fileName, payload)
            else -> logger.info("CSV File not supported")
          }
        }
      }
    } catch (ioException: IOException) {
      vertx.exceptionHandler().handle(ioException)
    }
  }

  private fun deleteDirectory(toBeDeleted: File): Boolean {
    toBeDeleted.listFiles()?.forEach { file ->
      deleteDirectory(file)
    }
    return toBeDeleted.delete()
  }

  private fun createNonExistentFile(fileName: String?) {
    try {
      val file = File("$dataDirectoryPath$fileName.csv")

      if (!file.exists()) {
        file.createNewFile()
      }
    } catch (exception: FileSystemException) {
      vertx.exceptionHandler().handle(exception)
    }
  }
}
