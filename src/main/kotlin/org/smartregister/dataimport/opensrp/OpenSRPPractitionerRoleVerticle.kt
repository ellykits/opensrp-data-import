package org.smartregister.dataimport.opensrp

import io.vertx.core.json.JsonArray
import org.smartregister.dataimport.openmrs.OpenMRSUserRoleVerticle
import org.smartregister.dataimport.shared.EventBusAddress

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for assigning OpenSRP practitioners to organizations
 */
class OpenSRPPractitionerRoleVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    super.start()
    vertx.deployVerticle(OpenMRSUserRoleVerticle())
    consumeData(
      countAddress = EventBusAddress.OPENMRS_USER_ROLE_COUNT,
      loadAddress = EventBusAddress.OPENMRS_USER_ROLE_LOAD,
      block = this::postUserRoles
    )
  }

  private suspend fun postUserRoles(teams: JsonArray) {
    webRequest(url = config.getString("opensrp.rest.practitioner.role.url"), payload = teams)
      ?.logHttpResponse()
  }
}
