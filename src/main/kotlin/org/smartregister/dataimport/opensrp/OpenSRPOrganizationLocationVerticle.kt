package org.smartregister.dataimport.opensrp

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import org.smartregister.dataimport.openmrs.OpenMRSTeamLocationVerticle
import org.smartregister.dataimport.shared.DataItem
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.SOURCE_FILE

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for assigning OpenSRP organizations to locations
 */
class OpenSRPOrganizationLocationVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    super.start()
    if (config.getString(SOURCE_FILE, "").isNullOrBlank()) {
      vertx.deployVerticle(OpenMRSTeamLocationVerticle())
      consumeOpenMRSData(
        dataItem = DataItem.ORGANIZATION_LOCATIONS,
        countAddress = EventBusAddress.OPENMRS_TEAM_LOCATIONS_COUNT,
        loadAddress = EventBusAddress.OPENMRS_TEAM_LOCATIONS_LOAD,
        action = this::mapTeamWithLocation
      )
    }
  }

  private suspend fun mapTeamWithLocation(teamLocations: JsonArray) {
    awaitResult<HttpResponse<Buffer>?> {
      webRequest(
        url = config.getString("opensrp.rest.organization.location.url"), payload = teamLocations, handler = it
      )
    }?.run{
      logHttpResponse()
      val counter = vertx.sharedData().getCounter(DataItem.ORGANIZATION_LOCATIONS.name).await()
      checkTaskCompletion(counter, DataItem.ORGANIZATION_LOCATIONS)
    }
  }
}
