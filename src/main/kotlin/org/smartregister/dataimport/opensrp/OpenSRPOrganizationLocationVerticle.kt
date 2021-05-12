package org.smartregister.dataimport.opensrp

import io.vertx.core.json.JsonArray
import org.smartregister.dataimport.openmrs.OpenMRSTeamLocationVerticle
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.SOURCE_FILE

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for assigning OpenSRP organizations to locations
 */
class OpenSRPOrganizationLocationVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    super.start()
    if (config.getString(SOURCE_FILE, "").isBlank()) {
      vertx.deployVerticle(OpenMRSTeamLocationVerticle())
      consumeOpenMRSData(
        countAddress = EventBusAddress.OPENMRS_TEAM_LOCATIONS_COUNT,
        loadAddress = EventBusAddress.OPENMRS_TEAM_LOCATIONS_LOAD,
        action = this::mapTeamWithLocation
      )
    }
  }

  private suspend fun mapTeamWithLocation(teamLocations: JsonArray) {
    webRequest(
      url = config.getString("opensrp.rest.organization.location.url"), payload = teamLocations
    )?.logHttpResponse()
  }
}
