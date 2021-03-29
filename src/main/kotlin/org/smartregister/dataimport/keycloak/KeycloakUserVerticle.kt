package org.smartregister.dataimport.keycloak

import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.smartregister.dataimport.openmrs.OpenMRSUserVerticle
import org.smartregister.dataimport.shared.*

/**
 * A subclass of [BaseVerticle] that queries all OpenMRS users and creates corresponding user accounts in Keycloak
 */
class KeycloakUserVerticle : BaseVerticle() {

  override suspend fun start() {
    super.start()
    vertx.deployVerticle(OpenMRSUserVerticle()).await()

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
            createUser(message.body())
            offset += limit
          }
        }

      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  private suspend fun createUser(users: JsonArray) {
    if (!users.isEmpty) {
        //Filter out openmrs & daemon users as they are no longer needed
        try {
          users.map { it as JsonObject }
            .onEach { payload ->
              webRequest(
                method = HttpMethod.POST,
                url = config.getString("keycloak.rest.users.url"),
                payload = payload.apply {
                  remove(IDENTIFIER)
                  remove(NAME)
                })?.logHttpResponse()
            }
        } catch (throwable: Throwable) {
          logger.error("$concreteClassName::Error creating Keycloak User")
          vertx.exceptionHandler().handle(throwable)
        }
    }
  }
}

