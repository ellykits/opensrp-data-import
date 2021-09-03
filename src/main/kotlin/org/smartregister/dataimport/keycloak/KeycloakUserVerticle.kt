package org.smartregister.dataimport.keycloak

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
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
import org.smartregister.dataimport.shared.model.OpenMRSUser
import java.util.*

/**
 * A subclass of [BaseKeycloakVerticle] that queries all OpenMRS users and creates corresponding
 * user accounts in Keycloak
 */
class KeycloakUserVerticle : BaseKeycloakVerticle() {

  private val openMRSUsersMap = TreeMap<String, OpenMRSUser>(String.CASE_INSENSITIVE_ORDER)

  override suspend fun start() {
    super.start()
    loadFromOpenMRS = config.getString(SOURCE_FILE, "").isNullOrBlank()
    val usersFile = config.getString(USERS_FILE, "")

    if (loadFromOpenMRS) {
      getProviderGroupId()
      if (!usersFile.isNullOrBlank()) {
        vertx.executeBlocking<Unit> {
          val csvUsers = readCsvData<OpenMRSUser>(
            fileName = usersFile,
            fileNameProvided = true,
            skipLines = 1
          ).associateBy { openMRSUser -> openMRSUser.username }
          openMRSUsersMap.putAll(csvUsers)
        }
      }
      deployVerticle(OpenMRSUserVerticle(), poolName = OPENMRS_USERS)

      vertx
        .eventBus()
        .consumer<Boolean>(EventBusAddress.OPENMRS_KEYCLOAK_USERS_GROUP_ASSIGN)
        .handler {
          // Assign users to 'Provider' group after creation
          if (it.body()) {
            if (!config.getBoolean(SKIP_USER_GROUP, false))
              launch(vertx.dispatcher()) { assignUsersToProviderGroup() }
            else completeTask(dataItem = DataItem.KEYCLOAK_USERS_GROUP, ignored = true)
          } else shutDown(dataItem = DataItem.KEYCLOAK_USERS, message = "Done!")
        }
      createUsersFromOpenMRS()
    } else {
      getProviderGroupId()
      vertx.eventBus().consumer<JsonArray>(EventBusAddress.CSV_KEYCLOAK_USERS_LOAD).handler { message ->
        launch(vertx.dispatcher()) { getOrCreateUser(message.body()) }
      }

      vertx
        .eventBus()
        .consumer<JsonArray>(EventBusAddress.CSV_KEYCLOAK_USERS_GROUP_ASSIGN)
        .handler { message ->
          launch(vertx.dispatcher()) { assignUsersToProviderGroup(message.body()) }
        }
    }
  }

  private suspend fun getProviderGroupId() {
    try {
      val groupResponse =
        awaitResult<HttpResponse<Buffer>?> {
          webRequest(
            method = HttpMethod.GET,
            url = config.getString("keycloak.rest.groups.url"),
            handler = it
          )
        }

      if (groupResponse != null) {
        providerGroupId =
          groupResponse
            .bodyAsJsonArray()
            .map { it as JsonObject }
            .find { it.getString(NAME).equals(PROVIDER, true) }
            ?.getString(ID)
      }
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

  private fun createUsersFromOpenMRS() {
    vertx.eventBus().consumer<Int>(EventBusAddress.OPENMRS_USERS_COUNT).handler { countMessage ->
      try {
        var offset = 0
        val count = countMessage.body()
        launch(vertx.dispatcher()) {
          startVertxCounter(DataItem.KEYCLOAK_USERS, count.toLong(), true)
          while (offset <= count) {
            awaitEvent<Long> { vertx.setTimer(getRequestInterval(DataItem.KEYCLOAK_USERS), it) }
            val message =
              awaitResult<Message<JsonArray>> { handler ->
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
        users.map { it as JsonObject }.forEach { payload ->
          // wait for specified number of times before making next request, default 1 second
          awaitEvent<Long> { timer -> vertx.setTimer(singleRequestInterval, timer) }
          val existingUser = checkKeycloakUser(payload.getString(USERNAME))
          if (existingUser == null) {
            val username = payload.getString(USERNAME)

            awaitResult<HttpResponse<Buffer>?> {
              webRequest(
                method = HttpMethod.POST,
                url = config.getString("keycloak.rest.users.url"),
                payload =
                payload.apply {
                  remove(IDENTIFIER)
                  remove(NAME)
                  if (openMRSUsersMap.isNotEmpty() && openMRSUsersMap.containsKey(username) &&
                    !this.getJsonArray(CREDENTIALS).isEmpty
                  ) {
                    this.getJsonArray(CREDENTIALS).getJsonObject(0).apply {
                      remove(TEMPORARY)
                      put(VALUE, openMRSUsersMap[username])
                    }
                  }
                },
                handler = it
              )
            }
              ?.run {
                logHttpResponse()
                // Get Returned user id from the server response
                val header: String? = this.getHeader(LOCATION_HEADER)
                header?.let {
                  val userId = it.substring(it.lastIndexOf("/") + 1).trim()
                  vertx
                    .eventBus()
                    .publish(
                      EventBusAddress.USER_FOUND,
                      JsonObject().put(USERNAME, username).put(ID, userId)
                    )
                }
                checkTaskCompletion(counter, DataItem.KEYCLOAK_USERS)
              }
          } else checkTaskCompletion(counter, DataItem.KEYCLOAK_USERS)
        }
      } catch (throwable: Throwable) {
        logger.error("$concreteClassName::Error creating Keycloak User")
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  private suspend fun assignUsersToProviderGroup(users: JsonArray) {
    val counter = vertx.sharedData().getCounter(DataItem.KEYCLOAK_USERS_GROUP.name).await()
    try {
      users.forEach {
        // wait for specified number of times before making next request, default 1 second
        awaitEvent<Long> { timer -> vertx.setTimer(singleRequestInterval, timer) }
        if (it is String) {
          val user = userIdsMap[it]
          // Ignore any missing and continue count down
          if (user == null) checkTaskCompletion(counter, DataItem.KEYCLOAK_USERS_GROUP)
          else assignUserToGroup(user)
        }
      }
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

  private suspend fun assignUsersToProviderGroup() {
    startVertxCounter(DataItem.KEYCLOAK_USERS_GROUP, userIdsMap.size.toLong())
    try {
      userIdsMap.forEach {
        // wait for specified number of times before making next request, default 1 second
        awaitEvent<Long> { timer -> vertx.setTimer(singleRequestInterval, timer) }
        assignUserToGroup(it.value)
      }
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }
}
