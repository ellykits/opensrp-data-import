package org.smartregister.dataimport.main

import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.keycloak.KeycloakUserVerticle
import org.smartregister.dataimport.opensrp.*
import org.smartregister.dataimport.shared.*

/**
 * This is the MainVerticle that is used to deploy other verticles. The [OpenSRPAuthVerticle] needs to be deployed first
 * and that is handled by this class. The [MainVerticle] is deployed with a config indicating the name of the verticle
 * to be deployed once access token has been received.
 */
class MainVerticle : BaseVerticle() {

  override suspend fun start() {
    super.start()
    val authVerticleId = vertx.deployVerticle(OpenSRPAuthVerticle()).await()
    if (authVerticleId != null) deployedVerticlesMap[OpenSRPAuthVerticle::class.java.simpleName] = authVerticleId

    vertx.eventBus().consumer<Boolean>(EventBusAddress.OAUTH_TOKEN_RECEIVED).handler { message ->
      launch(vertx.dispatcher()) {
        if (message.body()) {
          val choice = config.getString(IMPORT_OPTION)
          val verticleConfigs = JsonObject().apply {
            put(SOURCE_FILE, config.getString(SOURCE_FILE, ""))
            put(SKIP_LOCATION_TAGS, config.getBoolean(SKIP_LOCATION_TAGS, false))
            put(CREATE_TEAMS, config.getBoolean(CREATE_TEAMS, false))
          }

            if (choice != null) {
              when (Choices.valueOf(choice.toUpperCase())) {
                Choices.LOCATIONS -> deployVerticle(OpenSRPLocationTagVerticle(), verticleConfigs)
                Choices.ORGANIZATIONS -> deployVerticle(OpenSRPOrganizationVerticle(), verticleConfigs)
                Choices.PRACTITIONERS -> deployVerticle(OpenSRPPractitionerVerticle(), verticleConfigs)
                Choices.KEYCLOAK_USERS -> deployVerticle(KeycloakUserVerticle(), verticleConfigs)
                Choices.ORGANIZATION_LOCATIONS -> deployVerticle(OpenSRPOrganizationLocationVerticle(), verticleConfigs)
                Choices.PRACTITIONER_ROLES -> deployVerticle(OpenSRPPractitionerRoleVerticle(), verticleConfigs)
              }

            } else {
              logger.error("Nothing to deploy")
            }
          }
        }
      }
    }
}
