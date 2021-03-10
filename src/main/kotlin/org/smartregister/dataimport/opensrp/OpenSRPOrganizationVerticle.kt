package org.smartregister.dataimport.opensrp

import io.vertx.core.json.JsonArray
import org.smartregister.dataimport.openmrs.OpenMRSTeamVerticle
import org.smartregister.dataimport.shared.EventBusAddress

class OpenSRPOrganizationVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    super.start()
    vertx.deployVerticle(OpenMRSTeamVerticle())
    consumeData(
      countAddress = EventBusAddress.OPENMRS_TEAMS_COUNT,
      loadAddress = EventBusAddress.OPENMRS_TEAMS_LOAD,
      block = this::postTeams
    )
  }

  private suspend fun postTeams(teams: JsonArray) {
     webRequest(url = config.getString("opensrp.rest.organization.url"), payload = teams)
      ?.logHttpResponse()
  }
}
