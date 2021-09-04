package org.smartregister.dataimport.opensrp.location

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.openmrs.OpenMRSLocationTagVerticle
import org.smartregister.dataimport.opensrp.BaseOpenSRPVerticle
import org.smartregister.dataimport.shared.*
import org.smartregister.dataimport.shared.EventBusAddress.OPENMRS_LOCATION_TAGS_LOAD

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP location tags and for deploying
 * [OpenSRPLocationVerticle] verticle once all the tags are created.
 */
class OpenSRPLocationTagVerticle : BaseOpenSRPVerticle() {
  override suspend fun start() {
    super.start()
    val sourceFile = config.getString(SOURCE_FILE)
    val skipLocationsTags = config.getBoolean(SKIP_LOCATION_TAGS)
    if (!skipLocationsTags) {
      if (sourceFile.isNullOrBlank()) {
        deployVerticle(OpenMRSLocationTagVerticle(), OPENMRS_LOCATION_TAGS)
        vertx.eventBus().consumer<Int>(EventBusAddress.OPENMRS_LOCATION_TAGS_COUNT).handler {
            countMessage ->
          try {
            var offset = 0
            val count = countMessage.body()
            launch(vertx.dispatcher()) {
              startVertxCounter(DataItem.LOCATION_TAGS, count.toLong(), true)
              while (offset <= count) {
                awaitEvent<Long> { vertx.setTimer(getRequestInterval(DataItem.LOCATION_TAGS), it) }
                val message =
                    awaitResult<Message<JsonArray>> { handler ->
                      vertx.eventBus().request(OPENMRS_LOCATION_TAGS_LOAD, offset, handler)
                    }
                postLocationTags(message.body())
                offset += limit
              }
            }
          } catch (throwable: Throwable) {
            vertx.exceptionHandler().handle(throwable)
          }
        }
      } else {
        val counter = vertx.sharedData().getCounter(DataItem.LOCATION_TAGS.name).await()
        val tags =
            config.getString("location.hierarchy").split(',').map { it.split(":").first().trim() }
        startVertxCounter(DataItem.LOCATION_TAGS, tags.size.toLong(), true)
        tags.forEach {
          awaitEvent<Long> { timerId -> vertx.setTimer(2000L, timerId) }
          val locationTag = JsonObject().put(NAME, it).put(DESCRIPTION, "$it Tag").put(ACTIVE, true)

          awaitResult<HttpResponse<Buffer>?> { handler ->
            webRequest(
                method = HttpMethod.POST,
                url = config.getString("opensrp.rest.location.tag.url"),
                payload = locationTag,
                handler = handler)
          }
              ?.run {
                logHttpResponse()
                checkTaskCompletion(counter, DataItem.LOCATION_TAGS)
              }
        }
      }
    } else {
      completeTask(DataItem.LOCATION_TAGS, ignored = true)
    }
  }

  private suspend fun postLocationTags(locationTags: JsonArray) {
    locationTags.filterNotNull().forEach { locationTag ->
      awaitEvent<Long> { timer -> vertx.setTimer(singleRequestInterval, timer) }
      awaitResult<HttpResponse<Buffer>?> {
        webRequest(
            method = HttpMethod.POST,
            url = config.getString("opensrp.rest.location.tag.url"),
            payload = locationTag,
            handler = it)
      }
          ?.run {
            logHttpResponse()
            val counter = vertx.sharedData().getCounter(DataItem.LOCATION_TAGS.name).await()
            checkTaskCompletion(counter, DataItem.LOCATION_TAGS)
          }
    }
  }
}
