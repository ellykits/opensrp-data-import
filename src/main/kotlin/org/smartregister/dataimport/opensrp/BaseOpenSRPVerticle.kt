package org.smartregister.dataimport.opensrp

import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.serialization.encodeToString
import org.smartregister.dataimport.shared.BaseVerticle
import org.smartregister.dataimport.shared.DataItem
import org.smartregister.dataimport.shared.EventBusAddress

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

  protected suspend fun <T> consumeCSVData(csvData: List<List<T>>, dataItem: DataItem, action: (List<T>) -> Unit) {
    val counter = vertx.sharedData().getCounter(dataItem.name).await()
    vertx.setPeriodic(requestInterval) { timerId ->
      launch(vertx.dispatcher()) {
        val index: Int = counter.andIncrement.await().toInt()
        if (index >= csvData.size) {
          completeTask(dataItem = dataItem, timerId = timerId)
        }
        if (index < csvData.size) {
            action(csvData[index])
        }
      }
    }
  }

  private fun completeTask(dataLoadAddress: String = "data", dataItem: DataItem? = null, timerId: Long) {
    val data = when (dataLoadAddress) {
      EventBusAddress.OPENMRS_USERS_LOAD -> "users"
      EventBusAddress.OPENMRS_TEAM_LOCATIONS_LOAD -> "team locations mapping"
      EventBusAddress.OPENMRS_TEAMS_LOAD -> "teams"
      EventBusAddress.OPENMRS_LOCATIONS_LOAD -> "locations"
      else -> "data"
    }
    logger.info("TASK COMPLETED: ${dataItem?.name?.lowercase() ?: data} migrated to OpenSRP.")
    if(dataItem != null) vertx.eventBus().send(EventBusAddress.TASK_COMPLETE, dataItem.name)
    vertx.cancelTimer(timerId)
  }

  protected inline fun <reified T> postData(url: String, data: List<T>, dataItem: DataItem) {
    val payload = JsonArray(jsonEncoder().encodeToString(data))
    launch(Dispatchers.IO) {
      try {
        webRequest(url = url, payload = payload)?.logHttpResponse()
        logger.info("Posted ${data.size} ${dataItem.name.lowercase()} to OpenSRP")
      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }
}
