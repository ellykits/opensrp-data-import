package org.smartregister.dataimport.opensrp

import io.vertx.core.json.JsonArray
import org.smartregister.dataimport.openmrs.OpenMRSTeamVerticle
import org.smartregister.dataimport.shared.EventBusAddress

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP organizations
 */
class OpenSRPOrganizationVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    super.start()
    vertx.deployVerticle(OpenMRSTeamVerticle())
    consumeOpenMRSData(
      countAddress = EventBusAddress.OPENMRS_TEAMS_COUNT,
      loadAddress = EventBusAddress.OPENMRS_TEAMS_LOAD,
      action = this::postTeams
    )
  }

  private suspend fun postTeams(teams: JsonArray) {
    webRequest(url = config.getString("opensrp.rest.organization.url"), payload = teams)
      ?.logHttpResponse()
  }
}
