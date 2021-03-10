package org.smartregister.dataimport

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.choice
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deploymentOptionsOf
import org.smartregister.dataimport.main.MainVerticle
import org.smartregister.dataimport.shared.Choices
import org.smartregister.dataimport.shared.IMPORT_OPTION

fun main(args: Array<String>) {
  Application().main(args)
}

class Application : CliktCommand(name = "opensrp-data-import") {

  private val vertx = Vertx.vertx()

  private val configsFile: String? by option(
    help = "Configs file", envvar = "OPENSRP_DATA_IMPORT_CONFIGS_FILE",
    names = arrayOf("--configs-file")
  )

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

  private fun getChoice(choices: Choices) = choices.name.toLowerCase()

  override fun run() {
    val mainVerticle = MainVerticle()

    if (configsFile != null && importOption != null) {
      mainVerticle.setConfigsFile(configsFile!!)
      vertx.deployVerticle(mainVerticle, deploymentOptionsOf(config = JsonObject().put(IMPORT_OPTION, importOption)))
    } else {
      echo("--configs-file and --import options are required. Use --help for more information")
    }
  }
}
