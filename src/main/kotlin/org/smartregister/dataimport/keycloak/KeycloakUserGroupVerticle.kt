package org.smartregister.dataimport.keycloak

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.shared.*

/**
 * A subclass of [BaseVerticle] that queries the keycloak groups for a group name called 'Provider' then assigns
 * every user in keycloak it.
 */
class KeycloakUserGroupVerticle : BaseVerticle() {

  override suspend fun start() {
    super.start()

    try {
      val queryParameters = mutableMapOf(FIRST to "0", MAX to limit.toString())
      val groupResponse = webRequest(method = HttpMethod.GET, url = config.getString("keycloak.rest.groups.url"))
      val usersCountResponse =
        webRequest(method = HttpMethod.GET, url = config.getString("keycloak.rest.users.count.url"))

      if (groupResponse != null && usersCountResponse != null) {
        val groupId = groupResponse.bodyAsJsonArray().map { it as JsonObject }
          .find { it.getString(NAME).equals(PROVIDER, true) }?.getString(ID)
        val userCount = usersCountResponse.bodyAsString().toLong()
        val baseUrl = config.getString("keycloak.rest.users.url")
        if (groupId != null) {
          vertx.setPeriodic(requestInterval) { timeId ->
            if (queryParameters.getValue(FIRST).toLong() >= userCount) {
              logger.info("Completed assigning keycloak users to Provider group")
              vertx.cancelTimer(timeId)
            }

            launch(vertx.dispatcher()) {
              logger.info("Current offset: ${queryParameters[FIRST]} and limit: $limit")
              val usersResponse = webRequest(method = HttpMethod.GET, queryParams = queryParameters, url = baseUrl)
              if (usersResponse != null && !usersResponse.bodyAsJsonArray().isEmpty) {
                usersResponse.bodyAsJsonArray().map { item -> item as JsonObject }.onEach {
                  val userId = it.getString(ID)
                  val url = "$baseUrl/$userId/groups/$groupId"
                  //Add the user to 'Provider' group
                  webRequest(method = HttpMethod.PUT, url = url)?.logHttpResponse()
                }
              }
              queryParameters[FIRST] = (queryParameters.getValue(FIRST).toLong() + limit).toString()
            }
          }
        }
      }
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

}
