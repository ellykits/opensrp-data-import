package org.smartregister.dataimport.openmrs

import org.smartregister.dataimport.shared.DatabaseQueries
import org.smartregister.dataimport.shared.DatabaseQueries.getTeamLocationsImportQuery
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.TEAM_LOCATIONS

/**
 * Subclass of [BaseOpenMRSVerticle] that returns the locations assigned to the various teams available in OpenMRS
 */
class OpenMRSTeamLocationVerticle : BaseOpenMRSVerticle() {

  override suspend fun start() {
    super.start()

    countRecords(EventBusAddress.OPENMRS_TEAM_LOCATIONS_COUNT, DatabaseQueries.TEAM_LOCATIONS_COUNT_QUERY)

    vertx.eventBus().consumer<Int>(EventBusAddress.OPENMRS_TEAM_LOCATIONS_LOAD).handler {
      replyWithOpenMRSData(it, getTeamLocationsImportQuery(it.body(), limit), TEAM_LOCATIONS)
    }
  }

}
