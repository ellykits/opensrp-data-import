package org.smartregister.dataimport

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deploymentOptionsOf
import org.smartregister.dataimport.main.MainVerticle
import org.smartregister.dataimport.shared.*

/**
 * Main function for debugging the application in the IDE. Uncomment any of the options to debug or set required system
 * environment variables with the correct configs in your debug configuration class.
 *
 * DO NOT Run wit production configs
 */

fun main() {
  val configs = JsonObject().apply {
    put(IMPORT_OPTION, "locations")
    put(SOURCE_FILE, "assets/locations.csv")
    put(USERS_FILE, "assets/users.csv")
    put(SKIP_USER_GROUP, true)
    put(SKIP_LOCATION_TAGS, true)
//    put(SKIP_LOCATIONS, true)
    put(GENERATE_TEAMS, "Health Facility")
  }
  Vertx.vertx().deployVerticle(MainVerticle(), deploymentOptionsOf(config = configs))
}
