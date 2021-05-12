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
    help = "Source file (MUST be in CSV format)", envvar = "OPENSRP_DATA_IMPORT_SOURCE_FILE",
    names = arrayOf("--source-file", "-s")
  )

  private val generateTeams: String? by option(
    help = "Indicate the Location Level to Assign teams",
    names = arrayOf("--generate-teams-at", "-gTA")
  )

  private val skipLocationTags: Boolean by option("--skip-location-tags", "-sLT").flag(default = false)

  private val importOption: String? by option(
    help = """
      You should run the command with options in the following order:

        1. locations - Use to import location tags and locations

        2. organization - Use to import teams

        3. organization_locations - Use to assign teams to locations

        4. keycloak_users - Use to create and assign Keycloak users to Provider group

        5. practitioner - Use to import practitioners

        6. practitioner_roles = Use assign practitioners to teams

    """.trimIndent(),
    names = arrayOf("--import")
  ).choice(
    getChoice(Choices.LOCATIONS),
    getChoice(Choices.ORGANIZATIONS),
    getChoice(Choices.ORGANIZATION_LOCATIONS),
    getChoice(Choices.KEYCLOAK_USERS),
    getChoice(Choices.PRACTITIONERS),
    getChoice(Choices.PRACTITIONER_ROLES)
  )

  private fun getChoice(choices: Choices) = choices.name.lowercase()

  override fun run() {
    val mainVerticle = MainVerticle()

    if (configsFile != null && importOption != null) {
      mainVerticle.setConfigsFile(configsFile!!)

      val configs = JsonObject().apply {
        put(IMPORT_OPTION, importOption)
        put(SOURCE_FILE, sourceFile)
        put(SKIP_LOCATION_TAGS, skipLocationTags)
        put(GENERATE_TEAMS, generateTeams)
      }

      vertx.deployVerticle(mainVerticle, deploymentOptionsOf(config = configs))
    } else {
      echo("--configs-file and --import options are required --source-file optional. Use --help for more information")
    }
  }
}
