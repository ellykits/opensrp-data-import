package org.smartregister.dataimport.opensrp

import io.vertx.core.json.JsonArray
import org.smartregister.dataimport.openmrs.OpenMRSTeamLocationVerticle
import org.smartregister.dataimport.shared.EventBusAddress

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for assigning OpenSRP organizations to locations
 */
class OpenSRPOrganizationLocationVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    vertx.deployVerticle(OpenMRSTeamLocationVerticle())
    consumeData(
      countAddress = EventBusAddress.OPENMRS_TEAM_LOCATIONS_COUNT,
      loadAddress = EventBusAddress.OPENMRS_TEAM_LOCATIONS_LOAD,
      block = this::mapTeamWithLocation
    )
  }

  private suspend fun mapTeamWithLocation(teamLocations: JsonArray) {
    webRequest(
      url = config.getString("opensrp.rest.organization.location.url"), payload = teamLocations
    )?.logHttpResponse()
  }
}
