package org.smartregister.dataimport.opensrp

import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.shared.BaseVerticle
import org.smartregister.dataimport.shared.EventBusAddress

/**
 * Subclass of [BaseVerticle] which is the base class for all classes with OpenSRP related code
 */
abstract class BaseOpenSRPVerticle : BaseVerticle() {

  /**
   * This function consumes the messaged in the event bus from the [countAddress]. Once A count has been received,
   * the function request for data from OpenMRS periodically (as configured via the [requestInterval] property)
   * via event bus through [loadAddress]. This will prompt querying OpenMRS
   * database for data once the data is received the callback method [block] with the data as its parameter
   */
  protected fun consumeData(countAddress: String, loadAddress: String, block: suspend (JsonArray) -> Unit) {
    vertx.eventBus().consumer<Int>(countAddress).handler {
      try {
        var offset = 0
        val count = it.body()
        vertx.setPeriodic(requestInterval) {
          launch(vertx.dispatcher()) {
            if (offset >= count) {
              endOperation(loadAddress)
              vertx.cancelTimer(it)
            }
            val message = awaitResult<Message<JsonArray>> { handler ->
              vertx.eventBus().request(loadAddress, offset, handler)
            }
            block(message.body())
            offset += limit
          }
        }
      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  fun endOperation(dataLoadAddress: String) {
    val data = when (dataLoadAddress) {
      EventBusAddress.OPENMRS_USERS_LOAD -> "users"
      EventBusAddress.OPENMRS_TEAM_LOCATIONS_LOAD -> "team locations mapping"
      EventBusAddress.OPENMRS_TEAMS_LOAD -> "teams"
      EventBusAddress.OPENMRS_LOCATIONS_LOAD -> "locations"
      else -> "data"
    }
    logger.info("Completed migrating $data to OpenSRP")
  }
}
