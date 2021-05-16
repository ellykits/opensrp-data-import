package org.smartregister.dataimport.keycloak

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.openmrs.OpenMRSUserVerticle
import org.smartregister.dataimport.shared.*

/**
 * A subclass of [BaseKeycloakVerticle] that queries all OpenMRS users and creates corresponding user accounts in Keycloak
 */
class KeycloakUserVerticle : BaseKeycloakVerticle() {

  override suspend fun start() {
    super.start()

    loadFromOpenMRS = config.getString(SOURCE_FILE, "").isNullOrBlank()

    if (loadFromOpenMRS) {

      vertx.deployVerticle(OpenMRSUserVerticle()).await()

      createUsersFromOpenMRS()

    } else {
      vertx.eventBus().consumer<JsonArray>(EventBusAddress.CSV_KEYCLOAK_USERS_LOAD).handler {
        launch(vertx.dispatcher()) {
          getOrCreateUser(it.body())
        }
      }

      vertx.eventBus().consumer<JsonArray>(EventBusAddress.CSV_KEYCLOAK_USERS_GROUP_ASSIGN).handler {
        launch(vertx.dispatcher()) {
          assignUsersToProviderGroup(it.body())
        }
      }
    }
  }

  private fun createUsersFromOpenMRS() {
    vertx.eventBus().consumer<Int>(EventBusAddress.OPENMRS_USERS_COUNT).handler { countMessage ->
      try {
        var offset = 0
        val count = countMessage.body()
        vertx.setPeriodic(requestInterval) {
          launch(vertx.dispatcher()) {
            if (offset >= count) {
              logger.info("Completed adding users to keycloak")
              deployVerticle(KeycloakUserGroupVerticle())
              vertx.cancelTimer(it)
            }
            val message = awaitResult<Message<JsonArray>> { handler ->
              vertx.eventBus().request(EventBusAddress.OPENMRS_USERS_LOAD, offset, handler)
            }
            getOrCreateUser(message.body())
            offset += limit
          }
        }
      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  private suspend fun getOrCreateUser(users: JsonArray) {
    if (!users.isEmpty) {
      val counter = vertx.sharedData().getCounter(DataItem.KEYCLOAK_USERS.name).await()
      try {
        users.map { it as JsonObject }
          .onEach { payload ->
            val existingUser = checkKeycloakUser(payload.getString(USERNAME))
            if(existingUser == null) {
              awaitResult<HttpResponse<Buffer>?> {
                webRequest(
                  method = HttpMethod.POST,
                  url = config.getString("keycloak.rest.users.url"),
                  payload = payload.apply {
                    remove(IDENTIFIER)
                    remove(NAME)
                  },
                  handler = it
                )
              }?.run {
                logHttpResponse()
                checkKeycloakUser(payload.getString(USERNAME))
                checkTaskCompletion(counter, DataItem.KEYCLOAK_USERS)
              }
            } else {
              checkTaskCompletion(counter, DataItem.KEYCLOAK_USERS)
            }
          }
      } catch (throwable: Throwable) {
        logger.error("$concreteClassName::Error creating Keycloak User")
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  private suspend fun assignUsersToProviderGroup(users: JsonArray) {
    users.forEach {
      if (it is String) {
        val user = userIdsMap[it]
        if (user != null) assignUserToGroup(user)
      }
    }
  }
}

