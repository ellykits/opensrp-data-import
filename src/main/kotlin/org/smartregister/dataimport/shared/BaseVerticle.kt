package org.smartregister.dataimport.shared

import com.opencsv.CSVWriter
import com.opencsv.bean.ColumnPositionMappingStrategy
import com.opencsv.bean.CsvToBeanBuilder
import com.opencsv.bean.StatefulBeanToCsvBuilder
import io.vertx.circuitbreaker.CircuitBreaker
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.AsyncResult
import io.vertx.core.DeploymentOptions
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.Counter
import io.vertx.ext.auth.User
import io.vertx.ext.web.client.HttpRequest
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.multipart.MultipartForm
import io.vertx.kotlin.circuitbreaker.circuitBreakerOptionsOf
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.web.client.webClientOptionsOf
import kotlinx.coroutines.launch
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.io.FileNotFoundException
import java.io.FileReader
import java.io.FileWriter
import java.io.IOException
import java.nio.file.FileSystems

/**
 * This is the base class extending [CoroutineVerticle] for all the Verticles. It provides the [webClient] used to perform
 * web requests.
 *
 * The [limit] is config defaults used to limit the number of records that can be sent per each
 * request.
 */
abstract class BaseVerticle : CoroutineVerticle() {

  private lateinit var webClient: WebClient

  protected lateinit var dataDirectoryPath: String

  protected val deployedVerticleIds = mutableSetOf<String>()

  protected val logger: Logger = LogManager.getLogger(this::class.java)

  protected var limit = 50

  protected var singleRequestInterval: Long = 1500

  protected val concreteClassName: String = this::class.java.simpleName

  protected fun jsonEncoder() = Json { encodeDefaults = true }

  private var requestTimeout: Long = -1

  private var workerPoolSize: Int = 10

  protected lateinit var circuitBreaker: CircuitBreaker

  private var maximumRetries = 10

  override suspend fun start() {
    vertx.exceptionHandler { throwable ->
      logger.error("$concreteClassName::Vertx exception", throwable)
      vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
    }

    try {
      val filePath = configsFile ?: "conf/application.properties"
      val options = ConfigRetrieverOptions().addStore(
        ConfigStoreOptions()
          .setType("file")
          .setFormat("properties")
          .setConfig(JsonObject().put("path", filePath))
      )
      val appConfigs = ConfigRetriever.create(vertx, options).config.await()
      config.mergeIn(appConfigs)

      limit = config.getInteger("data.limit", 20)
      maximumRetries = config.getInteger("circuit.breaker.max.retries", 10)
      workerPoolSize = config.getInteger("worker.pool.size", 10)
      requestTimeout = config.getLong("request.timeout", -1)
      singleRequestInterval = config.getLong("single.request.interval", 1000)
      val resetTimeout = config.getLong("reset.timeout", 5000)

      val circuitBreakerOptions = circuitBreakerOptionsOf(
        timeout = requestTimeout,
        resetTimeout = resetTimeout,
        maxFailures = 10,
        maxRetries = maximumRetries
      )

      circuitBreaker = CircuitBreaker.create(CIRCUIT_BREAKER_NAME, vertx, circuitBreakerOptions).retryPolicy { count ->
        count * 2500L
      }.fallback { logger.error("Error: $CIRCUIT_BREAKER_NAME -> ${it.message}", it) }

      webClient = WebClient.wrap(vertx.createHttpClient(), webClientOptionsOf(keepAlive = true, verifyHost = false))

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

      logger.info("$concreteClassName deployed")
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

  protected suspend fun deployVerticle(verticle: BaseVerticle, poolName: String? = null)
    : String? = try {
    val deploymentOptions = deploymentOptionsOf(
      worker = true,
      workerPoolSize = workerPoolSize,
      workerPoolName = poolName?.replace("_", "-"),
      config = commonConfigs()
    )
    val deploymentId = vertx.deployVerticle(
      verticle, if (!poolName.isNullOrBlank()) deploymentOptions else DeploymentOptions().setConfig(commonConfigs())
    ).await()
    deployedVerticleIds.add(deploymentId)
    deploymentId
  } catch (throwable: Throwable) {
    vertx.exceptionHandler().handle(throwable)
    null
  }

  private fun User.getAccessToken() = this.principal().getString(ACCESS_TOKEN)!!

  protected fun webRequest(
    method: HttpMethod = HttpMethod.POST, url: String, payload: Any? = null, queryParams: Map<String, String> = mapOf(),
    handler: Handler<AsyncResult<HttpResponse<Buffer>?>>
  ) {

    if (user == null || (user != null && user!!.expired())) return

    circuitBreaker.execute<HttpResponse<Buffer>?>({ cbPromise ->
      try {
        val httpRequest = httpRequest(method, url)
          .putHeader("Accept", "application/json")
          .putHeader("Content-Type", "application/json")
          .followRedirects(true)
          .bearerTokenAuthentication(user!!.getAccessToken())
          .timeout(requestTimeout)

        if (queryParams.isNotEmpty()) queryParams.forEach { httpRequest.addQueryParam(it.key, it.value) }

        when (payload) {
          is String -> httpRequest.sendBuffer(Buffer.buffer(payload)) {
            cbPromise.handleResponse<HttpResponse<Buffer>?>(it)
          }
          is MultipartForm -> httpRequest.sendMultipartForm(payload) {
            cbPromise.handleResponse<HttpResponse<Buffer>?>(it)
          }
          is JsonArray -> httpRequest.sendBuffer(payload.toBuffer()) {
            cbPromise.handleResponse<HttpResponse<Buffer>?>(it)
          }
          is JsonObject -> httpRequest.sendBuffer(payload.toBuffer()) {
            cbPromise.handleResponse<HttpResponse<Buffer>?>(it)
          }
          null -> httpRequest.send {
            cbPromise.handleResponse<HttpResponse<Buffer>?>(it)
          }
        }
      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }, handler)
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

  protected fun commonConfigs() =
    JsonObject().apply {
      put(IMPORT_OPTION, config.getString(IMPORT_OPTION))
      put(SOURCE_FILE, config.getString(SOURCE_FILE, ""))
      put(USERS_FILE, config.getString(USERS_FILE, ""))
      put(SKIP_LOCATION_TAGS, config.getBoolean(SKIP_LOCATION_TAGS, false))
      put(SKIP_LOCATIONS, config.getBoolean(SKIP_LOCATIONS, false))
      put(SKIP_USER_GROUP, config.getBoolean(SKIP_USER_GROUP, false))
      put(GENERATE_TEAMS, config.getString(GENERATE_TEAMS, ""))
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

  protected inline fun <reified T> readCsvData(
    fileName: String, fromAnyPlace: Boolean = false, skipLines: Int = 0
  ): List<T> {
    val fullPath = if (fromAnyPlace) fileName else "$dataDirectoryPath$fileName.csv"

    val reader = FileReader(fullPath)
    val mappingStrategy = ColumnPositionMappingStrategy<T>().apply { type = T::class.java }
    val csvData = mutableListOf<T>()

    try {
      val csvToBean = CsvToBeanBuilder<T>(reader)
        .withType(T::class.java)
        .withIgnoreLeadingWhiteSpace(true)
        .withSkipLines(skipLines)
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

  protected fun <T> consumeCSVData(csvData: List<List<T>>, dataItem: DataItem, action: suspend (List<T>) -> Unit) {
    if (csvData.isEmpty()) {
      logger.info("TASK IGNORED [${taskName(dataItem)}]: No data to migrate to OpenSRP")
      completeTask(dataItem = dataItem, ignored = true)
      return
    }

    launch(vertx.dispatcher()) {
      try {
        // do not create counter for key as requests are made one after the other
        val requestInterval = getRequestInterval(dataItem)
        when (dataItem) {
          DataItem.KEYCLOAK_USERS, DataItem.KEYCLOAK_USERS_GROUP, DataItem.LOCATION_TAGS -> {
            val dataSize = csvData.flatten().size.toLong()
            startVertxCounter(dataItem = dataItem, dataSize, true)
          }
          else -> startVertxCounter(dataItem, csvData.size.toLong())
        }

        for ((index, _) in csvData.withIndex()) {
          awaitEvent<Long> { vertx.setTimer(requestInterval, it) }
          action(csvData[index])
        }
      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  protected suspend fun startVertxCounter(dataItem: DataItem, dataSize: Long, singleRequests: Boolean = false) {
    val requestsCount = vertx.sharedData().getCounter(dataItem.name).await().addAndGet(dataSize).await()
    val requestInterval = getRequestInterval(dataItem) / 1000
    if (!singleRequests) {
      logger.info(
        "TASK STARTED [${this.taskName(dataItem)}]: Submitting $requestsCount request(s) periodically with " +
          "$requestInterval seconds delay/request"
      )
    } else {
      logger.info(
        "TASK STARTED [${taskName(dataItem)}]: Submitting $dataSize single request(s) with $requestInterval seconds delay/request"
      )
    }
  }

  protected fun completeTask(dataItem: DataItem, ignored: Boolean = false) {
    with(dataItem) {
      logger.info(
        "TASK ${if (ignored) "IGNORED" else "COMPLETED"} [${taskName(this)}]: ${taskName(this)}" +
          if (ignored) " not migrated" else " migrated"
      )
      if (config.getString(SOURCE_FILE).isNullOrBlank())
        vertx.eventBus().send(EventBusAddress.OPENMRS_TASK_COMPLETE, name)
      else vertx.eventBus().send(EventBusAddress.TASK_COMPLETE, name)
    }
  }

  protected suspend fun checkTaskCompletion(counter: Counter, dataItem: DataItem) {
    val currentCount = counter.decrementAndGet().await()
    logger.info("${dataItem.name}::Request count down -> $currentCount")
    if (currentCount == 0L) completeTask(dataItem = dataItem)
  }

  protected fun updateUserIds(userIdsMap: MutableMap<String, String>) {
    vertx.eventBus().consumer<JsonObject>(EventBusAddress.USER_FOUND).handler {
      with(it.body()) {
        val username = getString(USERNAME)
        val keycloakId = getString(ID)
        userIdsMap[username] = keycloakId
      }
    }
  }

  /**
   * Get delay period in milliseconds between each request. Keycloak does not allow batch posting of user data
   * therefore it has to be configured independently.
   */

  /**
   * Get delay period in milliseconds between each request. Keycloak does not allow batch posting of user data
   * therefore it has to be configured independently.
   */
  protected fun getRequestInterval(dataItem: DataItem): Long =
    when (dataItem) {
      DataItem.KEYCLOAK_USERS, DataItem.KEYCLOAK_USERS_GROUP -> {
        config.getLong("keycloak.request.delay", 50000)
      }
      else -> config.getLong("request.interval", 30000)
    }

  protected fun taskName(dataItem: DataItem) = dataItem.name.lowercase()

  fun shutDown(dataItem: DataItem, message: String? = null) {
    val sourceFile = this.taskName(dataItem)
    logger.info(message ?: "NOT SUPPORTED: Run command with the options --import (locations) --source-file ($sourceFile.csv)" +
    " --users-file (users.csv)")
    vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
  }

  private fun <T> Promise<T>.handleResponse(response: AsyncResult<T>) {
    if (response.succeeded()) complete(response.result() as T)
    else fail(response.cause())
  }

  companion object {
    var user: User? = null
    var configsFile: String? = null
  }
}
