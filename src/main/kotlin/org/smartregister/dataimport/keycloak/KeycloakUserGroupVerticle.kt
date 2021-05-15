package org.smartregister.dataimport.keycloak

import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.shared.*

/**
 * A subclass of [BaseKeycloakVerticle] that queries the keycloak groups for a group name called 'Provider' then assigns
 * every user in keycloak it.
 */
class KeycloakUserGroupVerticle : BaseKeycloakVerticle() {

  override suspend fun start() {
    super.start()

    if (!config.getBoolean(SKIP_USER_GROUP, false)) {
      val queryParameters = mutableMapOf(FIRST to "0", MAX to limit.toString())

      val usersCountResponse = awaitResult<HttpResponse<Buffer>?> {
        webRequest(method = HttpMethod.GET, url = config.getString("keycloak.rest.users.count.url"), handler = it)
      }

      if (providerGroupId == null) {
        throw DataImportException("Provider Group Missing!")
      }

      if (usersCountResponse != null) {

        val userCount = usersCountResponse.bodyAsString().toLong()

        vertx.setPeriodic(requestInterval) { timeId ->
          launch(vertx.dispatcher()) {
            if (queryParameters.getValue(FIRST).toLong() >= userCount) {
              logger.info("Completed assigning keycloak users to Provider group")
              vertx.cancelTimer(timeId)
            }

            logger.info("Current offset: ${queryParameters[FIRST]} and limit: $limit")

            val usersResponse = awaitResult<HttpResponse<Buffer>?> {
              webRequest(method = HttpMethod.GET, queryParams = queryParameters, url = baseUrl, handler = it)
            }?.bodyAsJsonArray()

            usersResponse?.map { item -> item as JsonObject }?.forEach { user ->
              assignUserToGroup(user.getString(ID))
            }
            queryParameters[FIRST] = (queryParameters.getValue(FIRST).toLong() + limit).toString()
          }
        }
      }
    }
  }
}
