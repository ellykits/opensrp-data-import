package org.smartregister.dataimport.opensrp.location

import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.awaitResult
import org.smartregister.dataimport.opensrp.BaseOpenSRPVerticle
import org.smartregister.dataimport.shared.*

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP location tags and for deploying [OpenSRPLocationVerticle]
 * verticle once all the tags are created.
 */
class OpenSRPLocationTagVerticle : BaseOpenSRPVerticle() {
  override suspend fun start() {
    super.start()

    val skipLocationsTags = config.getBoolean(SKIP_LOCATION_TAGS)

    if (!skipLocationsTags) {
      val counter = vertx.sharedData().getCounter(DataItem.LOCATION_TAGS.name).await()

      val tags = config.getString("location.hierarchy")
        .split(',')
        .map { it.split(":").first().trim() }

      startVertxCounter(DataItem.LOCATION_TAGS, tags.size.toLong(), true)

      tags.forEach {
        awaitEvent<Long> { timerId -> vertx.setTimer(2000L, timerId) }
        val locationTag = JsonObject()
          .put(NAME, it)
          .put(DESCRIPTION, "$it Tag")
          .put(ACTIVE, true)

        awaitResult<HttpResponse<Buffer>?> { handler ->
          webRequest(
            method = HttpMethod.POST,
            url = config.getString("opensrp.rest.location.tag.url"),
            payload = locationTag,
            handler = handler
          )

        }?.run {
          logHttpResponse()
          checkTaskCompletion(counter, DataItem.LOCATION_TAGS)
        }
      }
    } else completeTask(DataItem.LOCATION_TAGS, ignored = true)
  }
}
