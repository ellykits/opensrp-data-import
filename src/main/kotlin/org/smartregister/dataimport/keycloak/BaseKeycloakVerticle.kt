package org.smartregister.dataimport.keycloak

import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import org.smartregister.dataimport.shared.*
import java.util.*

abstract class BaseKeycloakVerticle : BaseVerticle() {

  private lateinit var baseUrl: String

  protected var providerGroupId: String? = null

  protected var loadFromOpenMRS: Boolean = false

  protected val userIdsMap = TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER)

  override suspend fun start() {
    super.start()
    baseUrl = config.getString("keycloak.rest.users.url", "")
    updateUserIds(userIdsMap)

    val eventBus = vertx.eventBus()
    eventBus.consumer<String>(EventBusAddress.OPENMRS_TASK_COMPLETE).handler {
      when (DataItem.valueOf(it.body())) {
        DataItem.KEYCLOAK_USERS -> {
          eventBus.send(EventBusAddress.OPENMRS_KEYCLOAK_USERS_GROUP_ASSIGN, true)
        }
        else -> eventBus.send(EventBusAddress.APP_SHUTDOWN, true)
      }
    }
  }

  protected suspend fun assignUserToGroup(userId: String) {
    if (providerGroupId == null) {
      throw DataImportException("Provider Group Missing!")
    }

    try {
      val url = "$baseUrl/$userId/groups/$providerGroupId"
      awaitResult<HttpResponse<Buffer>?> {
        webRequest(method = HttpMethod.PUT, url = url, handler = it)
      }?.run {
        logHttpResponse()
        val counter = vertx.sharedData().getCounter(DataItem.KEYCLOAK_USERS_GROUP.name).await()
        checkTaskCompletion(counter, DataItem.KEYCLOAK_USERS_GROUP)
      }
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

  /**
   * Search for keycloak user with the provided [username]. Get the first matching item from the response and return
   * Also send data to notify any listener about the user's id so they can update their map accordingly.
   */
  protected suspend fun checkKeycloakUser(username: String): JsonObject? {
    val resultArray = awaitResult<HttpResponse<Buffer>?> {
      webRequest(
        method = HttpMethod.GET,
        url = "$baseUrl?username=$username",
        handler = it
      )
    }?.bodyAsJsonArray()

    if (resultArray != null && !resultArray.isEmpty) {
      val userJson = resultArray.map { it as JsonObject }.find { it.getString(USERNAME).equals(username, true) }
      if (userJson != null) {
        try {
          val keycloakUserId = userJson.getString(ID)
          logger.info("Keycloak username $username found")
          vertx.eventBus()
            .publish(EventBusAddress.USER_FOUND, JsonObject().put(USERNAME, username).put(ID, keycloakUserId))
        } catch (throwable: Throwable) {
          vertx.exceptionHandler().handle(throwable)
        }
      }
      return userJson
    }
    return null
  }
}
