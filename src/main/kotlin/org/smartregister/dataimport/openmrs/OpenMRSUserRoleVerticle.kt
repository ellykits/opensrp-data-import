package org.smartregister.dataimport.openmrs

import org.smartregister.dataimport.shared.DatabaseQueries
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.PRACTITIONER_ROLES

/**
 * Subclass of [BaseOpenMRSVerticle] used to retrieve user team assignments from OpenMRS
 */
class OpenMRSUserRoleVerticle : BaseOpenMRSVerticle() {
  override suspend fun start() {

    super.start()
    countRecords(EventBusAddress.OPENMRS_USER_ROLE_COUNT, DatabaseQueries.USERS_ROLE_COUNT_QUERY)

    vertx.eventBus().consumer<Int>(EventBusAddress.OPENMRS_USER_ROLE_LOAD).handler {
      replyWithOpenMRSData(it, DatabaseQueries.getUserRoleImportQuery(it.body(), limit), PRACTITIONER_ROLES)
    }
  }
}
