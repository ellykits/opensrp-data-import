package org.smartregister.dataimport

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deploymentOptionsOf
import org.smartregister.dataimport.main.MainVerticle
import org.smartregister.dataimport.opensrp.OpenSRPLocationVerticle
import org.smartregister.dataimport.shared.IMPORT_OPTION
import org.smartregister.dataimport.shared.SKIP_LOCATION_TAGS
import org.smartregister.dataimport.shared.SOURCE_FILE

/**
 * Main function for debugging the application in the IDE
 */

fun main() {
  val configs = JsonObject().apply {
    put(IMPORT_OPTION, "locations")
    put(SOURCE_FILE, "assets/locations.csv")
    put(SKIP_LOCATION_TAGS, true)
  }
  Vertx.vertx().deployVerticle(MainVerticle(), deploymentOptionsOf(config = configs))
}
