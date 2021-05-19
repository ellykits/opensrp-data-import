package org.smartregister.dataimport.opensrp

import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.auth.oauth2.OAuth2FlowType
import io.vertx.ext.auth.oauth2.OAuth2Options
import io.vertx.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.smartregister.dataimport.shared.BaseVerticle
import org.smartregister.dataimport.shared.EventBusAddress

/**
 * Subclass of [BaseVerticle] that is responsible for obtaining access token from Keycloak that is then used by the
 * Web client to make requests. This class also runs a periodic task to refresh the token based on the time the
 * token is set to expire.
 */
class OpenSRPAuthVerticle : BaseVerticle() {

  private var oAuth2Auth: OAuth2Auth? = null

  override suspend fun start() {
    super.start()
    oAuth2Auth = awaitResult {
      circuitBreaker.execute({ breakerPromise ->
        KeycloakAuth.discover(vertx, OAuth2Options().apply {
          clientID = config.getString("keycloak.client.id")
          clientSecret = config.getString("keycloak.client.secret")
          site = config.getString("keycloak.site")
          tenant = config.getString("keycloak.realm")
          flow = OAuth2FlowType.PASSWORD
          isValidateIssuer = false
        }).onSuccess { auth ->
          if (auth != null) breakerPromise.complete(auth)
        }.onFailure { throwable ->
          breakerPromise.fail(throwable)
          logger.error(throwable.message, throwable)
        }
      }, it)
    }

    if (oAuth2Auth != null) {
      getAccessToken()
    }
  }

  private fun getAccessToken() {
    if (user == null && oAuth2Auth != null) {
      val credentials = JsonObject().apply {
        put("username", config.getString("keycloak.user.username"))
        put("password", config.getString("keycloak.user.password"))
      }
      oAuth2Auth!!.authenticate(credentials).onSuccess {
        logger.info("Authentication Successful")
        user = it
        vertx.eventBus().send(EventBusAddress.OAUTH_TOKEN_RECEIVED, true)

        //Schedule refresh token. NOTE: Make sure keycloak refresh_token lives longer than the access token
        val expiryPeriod = it.principal().getLong("expires_in")
        refreshTokenPeriodically(expiryPeriod)
      }.onFailure {
        logger.error("Authentication Failed", it)
        vertx.eventBus().send(EventBusAddress.OAUTH_TOKEN_RECEIVED, false)
      }
    }
  }

  private fun refreshTokenPeriodically(expiryPeriod: Long) {
    val mutex = Mutex()
    if (user != null && oAuth2Auth != null) {
      vertx.setPeriodic(expiryPeriod * 1000) {
        try {
          launch(Dispatchers.IO) {
            mutex.withLock {
              if (user!!.expired()) {
                user = oAuth2Auth!!.refresh(user).await()
              }
              logger.info("Access token refreshed")
            }
          }
        } catch (throwable: Throwable) {
          logger.error("Error refreshing token: ")
          vertx.exceptionHandler().handle(throwable)
        }
      }
    }
  }
}
