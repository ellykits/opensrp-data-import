package org.smartregister.dataimport.opensrp

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import org.smartregister.dataimport.openmrs.OpenMRSTeamVerticle
import org.smartregister.dataimport.shared.DataItem
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.SOURCE_FILE

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP organizations
 */
class OpenSRPOrganizationVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    super.start()
    if (config.getString(SOURCE_FILE, "").isNullOrBlank()) {
      vertx.deployVerticle(OpenMRSTeamVerticle())
      consumeOpenMRSData(
        dataItem = DataItem.ORGANIZATIONS,
        countAddress = EventBusAddress.OPENMRS_TEAMS_COUNT,
        loadAddress = EventBusAddress.OPENMRS_TEAMS_LOAD,
        action = this::postTeams
      )
    } else {
      shutDown(DataItem.ORGANIZATIONS)
    }
  }

  private suspend fun postTeams(teams: JsonArray) {
    awaitResult<HttpResponse<Buffer>?> {
      webRequest(url = config.getString("opensrp.rest.organization.url"), payload = teams, handler = it)
    }?.run {
      logHttpResponse()
      val counter = vertx.sharedData().getCounter(DataItem.ORGANIZATIONS.name).await()
      checkTaskCompletion(counter, DataItem.ORGANIZATIONS)
    }
  }
}
