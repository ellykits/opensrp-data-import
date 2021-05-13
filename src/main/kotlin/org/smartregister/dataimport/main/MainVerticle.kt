package org.smartregister.dataimport.main

import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.csv.CsvGeneratorVerticle
import org.smartregister.dataimport.keycloak.KeycloakUserVerticle
import org.smartregister.dataimport.opensrp.*
import org.smartregister.dataimport.shared.BaseVerticle
import org.smartregister.dataimport.shared.DataItem
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
    if (authVerticleId != null) deployedVerticleIds.add(authVerticleId)

    //Listen to shutdown request
    vertx.eventBus().consumer<Boolean>(EventBusAddress.APP_SHUTDOWN).handler { message ->
      if (message.body()) {
        logger.warn("SHUTDOWN: Stopping gracefully...(Press Ctrl + C) to force")
        vertx.close()
      }
    }

    vertx.eventBus().consumer<Boolean>(EventBusAddress.OAUTH_TOKEN_RECEIVED).handler { message ->
      launch(vertx.dispatcher()) {
        if (message.body()) {
          val choice = config.getString(IMPORT_OPTION)

          val verticleConfigs = commonConfigs()
          val csvGeneratorVerticleId = vertx.deployVerticle(CsvGeneratorVerticle()).await()
          if (choice != null && csvGeneratorVerticleId != null) {
            when (DataItem.valueOf(choice.uppercase())) {
              DataItem.LOCATIONS -> deployVerticle(OpenSRPLocationTagVerticle(), verticleConfigs)
              DataItem.ORGANIZATIONS -> deployVerticle(OpenSRPOrganizationVerticle(), verticleConfigs)
              DataItem.PRACTITIONERS -> deployVerticle(OpenSRPPractitionerVerticle(), verticleConfigs)
              DataItem.KEYCLOAK_USERS -> deployVerticle(KeycloakUserVerticle(), verticleConfigs)
              DataItem.ORGANIZATION_LOCATIONS -> deployVerticle(OpenSRPOrganizationLocationVerticle(), verticleConfigs)
              DataItem.PRACTITIONER_ROLES -> deployVerticle(OpenSRPPractitionerRoleVerticle(), verticleConfigs)
              else -> {
                logger.warn("Operation not supported")
                vertx.close()
              }
            }
          } else {
            logger.error("Nothing to deploy")
          }
        }
      }
    }
  }
}
