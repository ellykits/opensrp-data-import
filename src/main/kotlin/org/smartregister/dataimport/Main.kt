package org.smartregister.dataimport

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.choice
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deploymentOptionsOf
import org.smartregister.dataimport.main.MainVerticle
import org.smartregister.dataimport.shared.*

fun main(args: Array<String>) {
  Application().main(args)
}

class Application : CliktCommand(name = "opensrp-data-import") {

  private val vertx = Vertx.vertx()

  private val configsFile: String? by option(
    help = "Configs file", envvar = "OPENSRP_DATA_IMPORT_CONFIGS_FILE",
    names = arrayOf("--configs-file", "-c")
  )

  private val sourceFile: String? by option(
    help = "CSV file with the locations data", envvar = "OPENSRP_DATA_IMPORT_SOURCE_FILE",
    names = arrayOf("--source-file", "-s")
  )

  private val generateTeams: String? by option(
    help = "Indicate the location level for team assignment", names = arrayOf("--assign-team", "-aT")
  )

  private val usersFile: String? by option(
    help = "File containing users details. This is used to export to ",
    names = arrayOf("--users-file", "-u")
  )

  private val skipLocationTags: Boolean by option(
    help = "Skip importing location tags", names = arrayOf("--skip-location-tags", "-sT")
  ).flag(default = false)

  private val skipLocations: Boolean by option(
    help = "Skip importing locations", names = arrayOf("--skip-locations", "-sL")
  ).flag(default = false)

  private val skipUserGroup: Boolean by option(
    help = "Skip importing locations", names = arrayOf("--skip-user-group", "-sG")
  ).flag(default = false)

  private val importOption: String? by option(
    help = """
      You should run the command with options in the following order:

        1. locations - Use to import location tags and locations

        2. organizations - Use to import teams

        3. organization_locations - Use to assign teams to locations

        4. keycloak_users - Use to create and assign Keycloak users to Provider group

        5. practitioners - Use to import practitioners

        6. practitioner_roles = Use assign practitioners to teams

    """.trimIndent(),
    names = arrayOf("--import", "-i")
  ).choice(
    getChoice(DataItem.LOCATIONS),
    getChoice(DataItem.ORGANIZATIONS),
    getChoice(DataItem.ORGANIZATION_LOCATIONS),
    getChoice(DataItem.KEYCLOAK_USERS),
    getChoice(DataItem.PRACTITIONERS),
    getChoice(DataItem.PRACTITIONER_ROLES)
  )

  private fun getChoice(dataItem: DataItem) = dataItem.name.lowercase()

  override fun run() {
    val mainVerticle = MainVerticle()

    if (configsFile != null && importOption != null) {
      mainVerticle.setConfigsFile(configsFile!!)

      val configs = JsonObject().apply {
        put(IMPORT_OPTION, importOption)
        put(SOURCE_FILE, sourceFile)
        put(USERS_FILE, usersFile)
        put(SKIP_LOCATION_TAGS, skipLocationTags)
        put(SKIP_LOCATIONS, skipLocations)
        put(SKIP_USER_GROUP, skipUserGroup)
        put(GENERATE_TEAMS, generateTeams)
      }

      vertx.deployVerticle(mainVerticle, deploymentOptionsOf(config = configs))
    } else {
      echo("--configs-file and --import options are required --source-file optional. Use --help for more information")
    }
  }
}
