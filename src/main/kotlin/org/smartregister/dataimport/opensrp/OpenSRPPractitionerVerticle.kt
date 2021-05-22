package org.smartregister.dataimport.opensrp

import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.openmrs.OpenMRSUserVerticle
import org.smartregister.dataimport.shared.*
import java.util.*
import kotlin.math.ceil

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP practitioners
 */
class OpenSRPPractitionerVerticle : BaseOpenSRPVerticle() {

  private val userIdsMap = TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER)

  override suspend fun start() {
    super.start()

    if (config.getString(SOURCE_FILE, "").isNullOrBlank()) {
      val baseUrl = config.getString("keycloak.rest.users.url")
      val queryParameters = mutableMapOf(FIRST to "0", MAX to limit.toString())
      val countResponse = awaitResult<HttpResponse<Buffer>?> {
        webRequest(
          method = HttpMethod.GET,
          url = config.getString("keycloak.rest.users.count.url"),
          handler = it
        )
      }

      if (countResponse != null) {
        val userCount = countResponse.bodyAsString().toLong()
        var offset = 0
        val numberOfRequests = ceil(userCount.toDouble().div(limit.toDouble())).toLong()
        launch(vertx.dispatcher()) {
          try {
            val vertxCounter = vertx.sharedData().getCounter(KEYCLOAK_FETCH_USERS).await()
            vertxCounter.addAndGet(numberOfRequests)
            while (offset <= userCount) {
              awaitEvent<Long> { vertx.setTimer(singleRequestInterval, it) }
              logger.info("KEYCLOAK: Fetching user first: ${queryParameters[FIRST]}, max: $limit")
              val usersResponse = awaitResult<HttpResponse<Buffer>?> {
                webRequest(
                  method = HttpMethod.GET,
                  queryParams = queryParameters,
                  url = baseUrl,
                  handler = it
                )
              }
              usersResponse?.bodyAsJsonArray()?.map { it as JsonObject }?.forEach {
                userIdsMap[it.getString(USERNAME)] = it.getString(ID)
              }
              offset += limit
              queryParameters[FIRST] = offset.toString()

              usersFetched()
            }
          } catch (throwable: Throwable) {
            vertx.exceptionHandler().handle(throwable)
          }
        }
      } else shutDown(dataItem = DataItem.PRACTITIONERS, message = "KEYCLOAK: Cannot retrieve total number of users")

      consumeOpenMRSData(
        dataItem = DataItem.PRACTITIONERS,
        countAddress = EventBusAddress.OPENMRS_USERS_COUNT,
        loadAddress = EventBusAddress.OPENMRS_USERS_LOAD,
        action = this::createPractitioners
      )

      //Watch for any updates on user id
      updateUserIds(userIdsMap)
    } else shutDown(DataItem.PRACTITIONERS)
  }

  private suspend fun usersFetched() {
    val currentCount = vertx.sharedData().getCounter(KEYCLOAK_FETCH_USERS).await().run { decrementAndGet() }.await()
    if (currentCount == 0L) deployVerticle(OpenMRSUserVerticle(), OPENMRS_USERS)
  }

  private suspend fun createPractitioners(practitioners: JsonArray) {
    val counter = vertx.sharedData().getCounter(DataItem.PRACTITIONERS.name).await()
    val practitionerList: JsonArray = practitioners.onEach {
      if (it is JsonObject) {
        it.put(USER_ID, userIdsMap[it.getString(USERNAME)])
        it.put(ACTIVE, it.remove(ENABLED))
        it.remove(CREDENTIALS)
        it.remove(FIRST_NAME)
        it.remove(LAST_NAME)
      }
    }
    awaitResult<HttpResponse<Buffer>?> {
      webRequest(url = config.getString("opensrp.rest.practitioner.url"), payload = practitionerList, handler = it)
    }?.run {
      logHttpResponse()
      checkTaskCompletion(counter, DataItem.PRACTITIONERS)
    }
  }
}
