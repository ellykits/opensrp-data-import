package org.smartregister.dataimport

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deploymentOptionsOf
import org.smartregister.dataimport.main.MainVerticle
import org.smartregister.dataimport.shared.*

/**
 * Main function for debugging the application in the IDE
 */

fun main() {
  val configs = JsonObject().apply {
    put(IMPORT_OPTION, "practitioners")
//    put(SOURCE_FILE, "assets/locations.csv")
//    put(USERS_FILE, "assets/users.csv")
//    put(SKIP_LOCATION_TAGS, true)
//    put(GENERATE_TEAMS, "Health Facility")
  }
  Vertx.vertx().deployVerticle(MainVerticle(), deploymentOptionsOf(config = configs))
}
