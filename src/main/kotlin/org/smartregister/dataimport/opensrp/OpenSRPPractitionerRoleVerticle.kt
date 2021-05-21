package org.smartregister.dataimport.opensrp

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import org.smartregister.dataimport.openmrs.OpenMRSUserRoleVerticle
import org.smartregister.dataimport.shared.*

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for assigning OpenSRP practitioners to organizations
 */
class OpenSRPPractitionerRoleVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    super.start()
    if (config.getString(SOURCE_FILE, "").isNullOrBlank()) {
      deployVerticle(OpenMRSUserRoleVerticle(), OPENMRS_TEAM_ASSIGNMENT)
      consumeOpenMRSData(
        dataItem = DataItem.PRACTITIONER_ROLES,
        countAddress = EventBusAddress.OPENMRS_USER_ROLE_COUNT,
        loadAddress = EventBusAddress.OPENMRS_USER_ROLE_LOAD,
        action = this::postUserRoles
      )
    } else shutDown(DataItem.PRACTITIONER_ROLES)
  }

  private suspend fun postUserRoles(teams: JsonArray) {
    awaitResult<HttpResponse<Buffer>?> {
      webRequest(url = config.getString("opensrp.rest.practitioner.role.url"), payload = teams, handler = it)
    }?.run {
      logHttpResponse()
      val counter = vertx.sharedData().getCounter(DataItem.PRACTITIONER_ROLES.name).await()
      checkTaskCompletion(counter, DataItem.PRACTITIONER_ROLES)
    }
  }
}
