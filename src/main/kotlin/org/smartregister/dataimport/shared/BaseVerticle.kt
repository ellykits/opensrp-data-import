package org.smartregister.dataimport.shared

import com.opencsv.CSVWriter
import com.opencsv.bean.ColumnPositionMappingStrategy
import com.opencsv.bean.CsvToBeanBuilder
import com.opencsv.bean.StatefulBeanToCsvBuilder
import io.vertx.circuitbreaker.CircuitBreaker
import io.vertx.circuitbreaker.CircuitBreakerOptions
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.web.client.HttpRequest
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.multipart.MultipartForm
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.io.FileReader
import java.io.FileWriter
import java.io.IOException
import java.nio.file.FileSystems

/**
 * This is the base class extending [CoroutineVerticle] for all the Verticles. It provides the [webClient] used to perform
 * web requests. Also has [circuitBreaker] for wrapping web requests. [CircuitBreaker] is an implementation of
 * circuit breaker pattern.
 *
 * The [limit] and [requestInterval] are config defaults used to limit the number of records that can be sent per each
 * request and the time period between each server request respectively.
 */
abstract class BaseVerticle : CoroutineVerticle() {

  private lateinit var webClient: WebClient

  private lateinit var circuitBreaker: CircuitBreaker

  protected lateinit var dataDirectoryPath: String

  protected val deployedVerticlesMap = mutableMapOf<String, String>()

  protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

  protected var limit = 50

  protected var requestInterval: Long = 10000

  protected val concreteClassName: String = this::class.java.simpleName

  override suspend fun start() {
    vertx.exceptionHandler { throwable ->
      logger.error("$concreteClassName::Vertx exception", throwable)
      vertx.close()
    }

    try {
      val filePath = configsFile ?: "conf/application.properties"
      val options = ConfigRetrieverOptions().addStore(
        ConfigStoreOptions()
          .setType("file")
          .setFormat("properties")
          .setConfig(
            JsonObject().put(
              "path", filePath
            )
          )
      )
      val appConfigs = ConfigRetriever.create(vertx, options).config.await()
      config.mergeIn(appConfigs)

      circuitBreaker = CircuitBreaker.create(
        CIRCUIT_BREAKER_NAME, vertx, CircuitBreakerOptions().setMaxFailures(5)
          .setTimeout(config.getLong("request.timeout", 10000))
          .setFallbackOnFailure(true)
          .setResetTimeout(config.getLong("reset.timeout", 10000))
      )

      webClient = WebClient.wrap(vertx.createHttpClient())

      limit = config.getInteger("data.limit", 50)
      requestInterval = config.getLong("request.interval")

      dataDirectoryPath = vertx.executeBlocking<String> {
        try {
          val separator = FileSystems.getDefault().separator
          val fullPath = System.getProperty(HOME_DIR_PROPERTY) + separator + OPENSRP_DATA_PATH + separator
          it.complete(fullPath)
        } catch (exception: FileNotFoundException) {
          it.fail(exception)
          vertx.exceptionHandler().handle(exception)
        }
      }.await()

      logger.info("$concreteClassName deployed with id: $deploymentID")
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

  protected suspend fun deployVerticle(verticle: BaseVerticle, configs: JsonObject = JsonObject()) {
    val deploymentID = vertx.deployVerticle(verticle, deploymentOptionsOf(config = configs)).await()
    deployedVerticlesMap[verticle::class.java.simpleName] = deploymentID
  }

  private fun User.getAccessToken() = this.principal().getString(ACCESS_TOKEN)!!

  protected suspend fun webRequest(
    method: HttpMethod = HttpMethod.POST, url: String, payload: Any? = null, queryParams: Map<String, String> = mapOf()
  ): HttpResponse<Buffer>? {

    if (user == null && user!!.expired()) {
      return null
    }

    val httpRequest = httpRequest(method, url)
      .putHeader("Accept", "application/json")
      .putHeader("Content-Type", "application/json")
      .followRedirects(true)
      .bearerTokenAuthentication(user!!.getAccessToken())
      .timeout(config.getLong("request.timeout"))

    if (queryParams.isNotEmpty()) queryParams.forEach { httpRequest.addQueryParam(it.key, it.value) }

    return circuitBreaker.execute<HttpResponse<Buffer>?> {
      launch(Dispatchers.IO) {
        when (payload) {
          is JsonArray -> it.complete(httpRequest.sendBuffer(payload.toBuffer()).await())
          is JsonObject -> it.complete(httpRequest.sendBuffer(payload.toBuffer()).await())
          is MultipartForm -> it.complete(httpRequest.sendMultipartForm(payload).await())
          null -> it.complete(httpRequest.send().await())
          else -> it.complete(null)
        }
      }
    }.await()
  }

  protected fun HttpResponse<Buffer>.logHttpResponse() {
    val statusCode = statusCode()
    val statusMessage = statusMessage()
    when (statusCode) {
      in 200..300 -> logger.info("Successful request, status code: $statusCode")
      else -> logger.warn("$concreteClassName::Failed request: status code $statusCode, status message $statusMessage")
    }
    when {
      body() == null -> logger.info("$concreteClassName::Server response: No response")
      else -> logger.info("$concreteClassName::Server response: ${body()}")
    }
  }

  private fun httpRequest(httpMethod: HttpMethod, url: String): HttpRequest<Buffer> = when (httpMethod) {
    HttpMethod.POST -> webClient.postAbs(url)
    HttpMethod.DELETE -> webClient.deleteAbs(url)
    HttpMethod.PUT -> webClient.putAbs(url)
    HttpMethod.GET -> webClient.getAbs(url)
    HttpMethod.PATCH -> webClient.patchAbs(url)
    else -> webClient.headAbs(url)
  }

  fun setConfigsFile(configsFile: String) {
    Companion.configsFile = configsFile
  }

  override suspend fun stop() {
    logger.info("Stopping verticle:  $concreteClassName")
  }

  protected fun getVerticleCommonConfigs() =
    JsonObject().apply {
      put(SOURCE_FILE, config.getString(SOURCE_FILE, ""))
      put(SKIP_LOCATION_TAGS, config.getBoolean(SKIP_LOCATION_TAGS, false))
    }

  protected inline fun <reified T> writeCsv(fileName: String, payload: String) {
    val fileWriter = FileWriter("$dataDirectoryPath$fileName.csv", true)
    try {
      val builder: StatefulBeanToCsvBuilder<T> = StatefulBeanToCsvBuilder(fileWriter)

      val mappingStrategy = ColumnPositionMappingStrategy<T>().apply {
        type = T::class.java
      }

      val beanWriter = builder.withMappingStrategy(mappingStrategy)
        .withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
        .build()

      beanWriter.write(Json.decodeFromString<T>(payload))
      fileWriter.flush()
    } catch (exception: IOException) {
      vertx.exceptionHandler().handle(exception)
    } finally {
      fileWriter.close()
    }
  }

  protected inline fun <reified T> readCsvData(fileName: String): List<T> {
    val reader = FileReader("$dataDirectoryPath$fileName.csv")
    val mappingStrategy = ColumnPositionMappingStrategy<T>().apply { type = T::class.java }
    val csvData = mutableListOf<T>()

    try {
      val csvToBean = CsvToBeanBuilder<T>(reader)
        .withType(T::class.java)
        .withIgnoreLeadingWhiteSpace(true)
        .withMappingStrategy(mappingStrategy)
        .build()

      val iterator: MutableIterator<T> = csvToBean.iterator()
      while (iterator.hasNext()) {
        csvData.add(iterator.next())
      }
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    } finally {
      reader.close()
    }
    return csvData
  }

  companion object {
    var user: User? = null
    var configsFile: String? = null
  }
}
