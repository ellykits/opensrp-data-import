package org.smartregister.dataimport.opensrp

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.smartregister.dataimport.openmrs.OpenMRSUserVerticle
import org.smartregister.dataimport.shared.*
import java.util.*

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP practitioners
 */
class OpenSRPPractitionerVerticle : BaseOpenSRPVerticle() {

  private val userIdsMap = TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER)

  override suspend fun start() {
    super.start()

    val baseUrl = config.getString("keycloak.rest.users.url")
    val queryParameters = mutableMapOf(FIRST to "0", MAX to limit.toString())
    val countResponse = webRequest(method = HttpMethod.GET, url = config.getString("keycloak.rest.users.count.url"))

    if (countResponse != null) {
      val userCount = countResponse.bodyAsString().toLong()
      vertx.setPeriodic(config.getLong("keycloak.user.request.interval", 2500)) { timeId ->
        if (queryParameters.getValue(FIRST).toLong() >= userCount) {
          logger.info("Completed fetching ${userIdsMap.size} users from keycloak")
          vertx.deployVerticle(OpenMRSUserVerticle())
          vertx.cancelTimer(timeId)
        }
        launch(vertx.dispatcher()) {
          logger.info("Current offset: ${queryParameters[FIRST]} and limit: $limit")
          val usersResponse = webRequest(method = HttpMethod.GET, queryParams = queryParameters, url = baseUrl)
          usersResponse?.bodyAsJsonArray()?.map { it as JsonObject }?.forEach {
            userIdsMap[it.getString(USERNAME)] = it.getString(ID)
          }
          queryParameters[FIRST] = (queryParameters.getValue(FIRST).toLong() + limit).toString()
        }
      }
    }

    consumeOpenMRSData(
      countAddress = EventBusAddress.OPENMRS_USERS_COUNT,
      loadAddress = EventBusAddress.OPENMRS_USERS_LOAD,
      action = this::createPractitioners
    )
  }

  private suspend fun createPractitioners(practitioners: JsonArray) {
    launch(Dispatchers.IO) {
      val practitionerList: JsonArray = practitioners.onEach {
        if (it is JsonObject) {
          it.put(USER_ID, userIdsMap[it.getString(USERNAME)])
          it.put(ACTIVE, it.remove(ENABLED))
          it.remove(CREDENTIALS)
          it.remove(FIRST_NAME)
          it.remove(LAST_NAME)
        }
      }
      webRequest(url = config.getString("opensrp.rest.practitioner.url"), payload = practitionerList)
        ?.logHttpResponse()
    }
  }

}
