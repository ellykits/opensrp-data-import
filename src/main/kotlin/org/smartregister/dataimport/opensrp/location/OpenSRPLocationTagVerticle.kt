package org.smartregister.dataimport.opensrp.location

import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.opensrp.BaseOpenSRPVerticle
import org.smartregister.dataimport.shared.*

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP location tags and for deploying [OpenSRPLocationVerticle]
 * verticle once all the tags are created.
 */
class OpenSRPLocationTagVerticle : BaseOpenSRPVerticle() {
  override suspend fun start() {
    super.start()

    val skipLocations = config.getBoolean(SKIP_LOCATIONS)
    val counter =  vertx.sharedData().getCounter(DataItem.LOCATION_TAGS.name).await()
    launch(vertx.dispatcher()) {
      if (!config.getBoolean(SKIP_LOCATION_TAGS)) {
        config.getString("location.hierarchy")
          .split(',')
          .map { it.split(":").first().trim() }
          .also { counter.getAndAdd(it.size.toLong()).await() }
          .forEach {

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
              if(skipLocations) checkTaskCompletion(counter, DataItem.LOCATION_TAGS)
            }
          }
      }

      if (!skipLocations) deployVerticle(OpenSRPLocationVerticle(), commonConfigs())
    }
  }
}
