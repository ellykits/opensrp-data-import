package org.smartregister.dataimport.openmrs

import org.smartregister.dataimport.shared.DatabaseQueries
import org.smartregister.dataimport.shared.DatabaseQueries.getLocationsImportQuery
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.LOCATIONS

class OpenMRSLocationVerticle : BaseOpenMRSVerticle() {

  override suspend fun start() {
    super.start()

    countRecords(EventBusAddress.OPENMRS_LOCATIONS_COUNT, DatabaseQueries.LOCATIONS_COUNT_QUERY)

    val locationHierarchy = config.getString("location.hierarchy").split(',')
    vertx.eventBus().consumer<Int>(EventBusAddress.OPENMRS_LOCATIONS_LOAD).handler {
      replyWithOpenMRSData(it, getLocationsImportQuery(it.body(), locationHierarchy, limit), LOCATIONS)
    }
  }
}
