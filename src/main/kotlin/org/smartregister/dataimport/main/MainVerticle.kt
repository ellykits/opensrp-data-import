package org.smartregister.dataimport.main

import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.smartregister.dataimport.csv.CsvGeneratorVerticle
import org.smartregister.dataimport.keycloak.KeycloakUserVerticle
import org.smartregister.dataimport.opensrp.*
import org.smartregister.dataimport.opensrp.location.OpenSRPLocationVerticle
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

          val csvVerticleId = deployVerticle(CsvGeneratorVerticle(), poolName = CSV_GENERATOR)

          if (choice != null && !csvVerticleId.isNullOrBlank()) {

            //Start keycloak service for CSV data sources
            if (!config.getString(SOURCE_FILE, "").isNullOrBlank()) {
              val keycloakVerticleId = deployVerticle(KeycloakUserVerticle(), DataItem.KEYCLOAK_USERS.name.lowercase())
              if (keycloakVerticleId.isNullOrBlank()) {
                logger.warn("Keycloak service not deployed successfully. Retry again.")
                vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
              }
            }

            when (DataItem.valueOf(choice.uppercase())) {
              DataItem.LOCATIONS -> deployVerticle(OpenSRPLocationVerticle(), poolName = choice)
              DataItem.ORGANIZATIONS -> deployVerticle(OpenSRPOrganizationVerticle(), poolName = choice)
              DataItem.PRACTITIONERS -> deployVerticle(OpenSRPPractitionerVerticle(), poolName = choice)
              DataItem.KEYCLOAK_USERS -> deployVerticle(KeycloakUserVerticle(), poolName = choice)
              DataItem.ORGANIZATION_LOCATIONS ->
                deployVerticle(OpenSRPOrganizationLocationVerticle(), poolName = choice)
              DataItem.PRACTITIONER_ROLES -> deployVerticle(OpenSRPPractitionerRoleVerticle(), poolName = choice)
              else -> {
                logger.warn("Operation not supported")
                vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
              }
            }
          } else {
            logger.error("Nothing to deploy or error deploying common verticles. Try again.")
            vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
          }
        }
      }
    }
  }
}
