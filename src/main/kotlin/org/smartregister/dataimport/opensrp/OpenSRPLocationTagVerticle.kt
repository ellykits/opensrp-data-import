package org.smartregister.dataimport.opensrp

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.shared.ACTIVE
import org.smartregister.dataimport.shared.DESCRIPTION
import org.smartregister.dataimport.shared.NAME

class OpenSRPLocationTagVerticle : BaseOpenSRPVerticle() {
  override suspend fun start() {
    super.start()
    launch(vertx.dispatcher()) {
      config.getString("location.hierarchy").split(',').forEach {
        val locationTag = JsonObject()
          .put(NAME, it)
          .put(DESCRIPTION, "$it Tag")
          .put(ACTIVE, true)

        webRequest(
          HttpMethod.POST,
          url = config.getString("opensrp.rest.location.tag.url"),
          payload = locationTag
        )?.logHttpResponse()
      }
      vertx.deployVerticle(OpenSRPLocationVerticle())

    }
  }
}
