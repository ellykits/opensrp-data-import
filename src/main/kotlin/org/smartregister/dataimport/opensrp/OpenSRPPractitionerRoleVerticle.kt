package org.smartregister.dataimport.opensrp

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.awaitResult
import org.smartregister.dataimport.openmrs.OpenMRSUserRoleVerticle
import org.smartregister.dataimport.shared.EventBusAddress

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for assigning OpenSRP practitioners to organizations
 */
class OpenSRPPractitionerRoleVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    super.start()
    vertx.deployVerticle(OpenMRSUserRoleVerticle())
    consumeOpenMRSData(
      countAddress = EventBusAddress.OPENMRS_USER_ROLE_COUNT,
      loadAddress = EventBusAddress.OPENMRS_USER_ROLE_LOAD,
      action = this::postUserRoles
    )
  }

  private suspend fun postUserRoles(teams: JsonArray) {
    awaitResult<HttpResponse<Buffer>?> {
      webRequest(url = config.getString("opensrp.rest.practitioner.role.url"), payload = teams, handler = it)
    }?.logHttpResponse()
  }
}
