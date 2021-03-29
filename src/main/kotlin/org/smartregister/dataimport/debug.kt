package org.smartregister.dataimport

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deploymentOptionsOf
import org.smartregister.dataimport.main.MainVerticle
import org.smartregister.dataimport.shared.IMPORT_OPTION

/**
 * Main function for debugging the application in the IDE
 */
fun main() {
  Vertx.vertx()
    .deployVerticle(MainVerticle(), deploymentOptionsOf(config = JsonObject().put(IMPORT_OPTION, "keycloak_users")))
}
