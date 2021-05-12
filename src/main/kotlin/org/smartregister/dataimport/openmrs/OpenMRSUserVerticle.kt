package org.smartregister.dataimport.openmrs

import org.smartregister.dataimport.shared.DatabaseQueries
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.PRACTITIONERS

/**
 * This subclass of [BaseOpenMRSVerticle] returns all users in OpenMRS
 */
class OpenMRSUserVerticle : BaseOpenMRSVerticle() {
  override suspend fun start() {

    super.start()
    countRecords(EventBusAddress.OPENMRS_USERS_COUNT, DatabaseQueries.USERS_COUNT_QUERY)

    vertx.eventBus().consumer<Int>(EventBusAddress.OPENMRS_USERS_LOAD).handler {
      replyWithOpenMRSData(it, DatabaseQueries.getUsersImportQuery(it.body(), limit), PRACTITIONERS)
    }
  }
}
