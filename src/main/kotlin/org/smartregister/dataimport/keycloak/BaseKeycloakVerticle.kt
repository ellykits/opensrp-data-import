package org.smartregister.dataimport.keycloak

import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.awaitResult
import org.smartregister.dataimport.shared.*

abstract class BaseKeycloakVerticle : BaseVerticle() {

  protected lateinit var baseUrl: String

  protected var providerGroupId: String? = null

  override suspend fun start() {
    super.start()
    baseUrl = config.getString("keycloak.rest.users.url", "")

    val groupResponse =
      awaitResult<HttpResponse<Buffer>?> {
        webRequest(
          method = HttpMethod.GET,
          url = config.getString("keycloak.rest.groups.url"),
          handler = it
        )
      }

    if (groupResponse != null) {
      providerGroupId = groupResponse.bodyAsJsonArray().map { it as JsonObject }
        .find { it.getString(NAME).equals(PROVIDER, true) }?.getString(ID)
    }
  }

  protected suspend fun assignUserToGroup(userId: String) {
    if (providerGroupId == null) {
      throw DataImportException("Provider Group Missing!")
    }
    val url = "$baseUrl/$userId/groups/$providerGroupId"
    awaitResult<HttpResponse<Buffer>?> {
      webRequest(
        method = HttpMethod.PUT,
        url = url,
        handler = it
      )
    }?.logHttpResponse()
  }

  /**
   * Search for keycloak user with the provided [username]. Get the first matching item from the response and return
   * Also send data to notify any listener about the user's id so they can update their map accordingly.
   */
  protected suspend fun getUserByUsername(username: String): JsonObject? {
    val user =
      awaitResult<HttpResponse<Buffer>?> {
        webRequest(
          method = HttpMethod.GET,
          url = "$baseUrl?username=$username",
          handler = it
        )
      }
        ?.bodyAsJsonArray()?.map { it as JsonObject }?.first()

    if (user != null) {
      try {
        vertx.eventBus().publish(
          EventBusAddress.USER_FOUND, JsonObject().put(USERNAME, user.getString(USERNAME)).put(ID, user.getString(ID))
        )
      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
      return user
    }
    return null
  }
}
