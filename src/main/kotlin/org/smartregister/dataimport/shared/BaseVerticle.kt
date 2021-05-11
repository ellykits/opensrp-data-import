package org.smartregister.dataimport.shared

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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

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

  protected val deployedVerticlesMap = mutableMapOf<String, String>()

  protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

  protected var limit = 50

  protected var requestInterval: Long = 10000

  protected val concreteClassName: String = this::class.java.simpleName

  override suspend fun start() {
    circuitBreaker = CircuitBreaker.create(
      CIRCUIT_BREAKER_NAME, vertx, CircuitBreakerOptions().setMaxFailures(5)
        .setTimeout(config.getLong("request.timeout", 10000))
        .setFallbackOnFailure(true)
        .setResetTimeout(config.getLong("reset.timeout", 10000))
    )

    webClient = WebClient.wrap(vertx.createHttpClient())

    vertx.exceptionHandler { throwable ->
      logger.error("$concreteClassName::Vertx exception", throwable)
      vertx.close()
    }

    logger.info("$concreteClassName deployed with id: $deploymentID")

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

      limit = config.getInteger("data.limit", 50)
      requestInterval = config.getLong("request.interval")

    } catch (throwable: Throwable) {
      logger.error("Error reading configs")
      vertx.exceptionHandler().handle(throwable)
    }
  }

  protected suspend fun deployVerticle(verticle: BaseVerticle, configs:JsonObject = JsonObject()) {
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

  fun setConfigsFile(configsFile: String){
    Companion.configsFile = configsFile
  }

  override suspend fun stop() {
    logger.info("Stopping verticle:  $concreteClassName")
  }

  companion object {
    var user: User? = null
    var configsFile: String? = null
  }
}
