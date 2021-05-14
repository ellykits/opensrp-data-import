package org.smartregister.dataimport.opensrp

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import kotlinx.serialization.encodeToString
import org.smartregister.dataimport.shared.*

/**
 * Subclass of [BaseVerticle] which is the base class for all classes with OpenSRP related code
 */
abstract class BaseOpenSRPVerticle : BaseVerticle() {

  /**
   * This function consumes the messaged in the event bus from the [countAddress]. Once A count has been received,
   * the function request for data from OpenMRS periodically (as configured via the [requestInterval] property)
   * via event bus through [loadAddress]. This will prompt querying OpenMRS
   * database for data once the data is received the callback method [action] with the data as its parameter
   */
  protected fun consumeOpenMRSData(countAddress: String, loadAddress: String, action: suspend (JsonArray) -> Unit) {
    vertx.eventBus().consumer<Int>(countAddress).handler {
      try {
        var offset = 0
        val count = it.body()
        vertx.setPeriodic(requestInterval) { timerId ->
          launch(vertx.dispatcher()) {
            if (offset >= count) {
              completeTask(loadAddress, timerId = timerId)
            }
            val message = awaitResult<Message<JsonArray>> { handler ->
              vertx.eventBus().request(loadAddress, offset, handler)
            }
            action(message.body())
            offset += limit
          }
        }
      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  /**
   * Post [data] to the provided [url] keeping track of the number of responses from the server.This will help in correctly
   * determining whether the task is fully completed. The counter starts off with number of requests sent to the server.
   * When 0 it means that was the last request.
   */
  protected suspend inline fun <reified T> postData(url: String, data: List<T>, dataItem: DataItem) {
    val item = dataItem.name.lowercase()
    logger.info("Posting ${data.size} $item data to OpenSRP")
    val payload = JsonArray(jsonEncoder().encodeToString(data))
    val counter = vertx.sharedData().getCounter(dataItem.name).await()
    try {
      awaitResult<HttpResponse<Buffer>?> { webRequest(url = url, payload = payload, handler = it) }
        ?.run {
          logHttpResponse()
          logger.info("Posted ${data.size} $item to OpenSRP")
          checkTaskCompletion(counter, dataItem)
        }
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

  protected suspend inline fun <reified T> sendData(address: String, dataItem: DataItem, data: List<T>) {
    vertx.sharedData().getCounter(dataItem.name).await().addAndGet(data.size.toLong()).await()
    val payload = JsonArray(jsonEncoder().encodeToString(data))
    vertx.eventBus().send(address, payload)
  }

  protected fun updateUserIds(userIdsMap: MutableMap<String, String>) {
    vertx.eventBus().consumer<JsonObject>(EventBusAddress.USER_FOUND).handler {
      with(it.body()) {
        val username = getString(USERNAME)
        val keycloakId = getString(ID)
        logger.info("Keycloak user Id set for $username")
        userIdsMap[username] = keycloakId
      }
    }
  }
}
