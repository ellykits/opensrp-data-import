package org.smartregister.dataimport.main

import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.keycloak.KeycloakUserVerticle
import org.smartregister.dataimport.opensrp.*
import org.smartregister.dataimport.shared.BaseVerticle
import org.smartregister.dataimport.shared.Choices
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.IMPORT_OPTION

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

    vertx.eventBus().consumer<Boolean>(EventBusAddress.OAUTH_TOKEN_RECEIVED).handler {
      launch(vertx.dispatcher()) {
        if (it.body()) {
          val choice = config.getString(IMPORT_OPTION)
          if (choice != null) {
            when (Choices.valueOf(choice.toUpperCase())) {
              Choices.LOCATIONS -> deployVerticle(OpenSRPLocationTagVerticle())
              Choices.ORGANIZATIONS -> deployVerticle(OpenSRPOrganizationVerticle())
              Choices.PRACTITIONERS -> deployVerticle(OpenSRPPractitionerVerticle())
              Choices.KEYCLOAK_USERS -> deployVerticle(KeycloakUserVerticle())
              Choices.ORGANIZATION_LOCATIONS -> deployVerticle(OpenSRPOrganizationLocationVerticle())
              Choices.PRACTITIONER_ROLES -> deployVerticle(OpenSRPPractitionerRoleVerticle())
            }

          } else {
            logger.error("Nothing to deploy")
          }
        }
      }
    }
  }
}
