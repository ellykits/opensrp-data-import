package org.smartregister.dataimport.openmrs

import org.smartregister.dataimport.shared.DatabaseQueries
import org.smartregister.dataimport.shared.DatabaseQueries.getLocationTagsQuery
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.LOCATION_TAGS

/**
 * Subclass of [BaseOpenMRSVerticle] that is used to retrieve all the openmrs locations.
 */
class OpenMRSLocationTagVerticle : BaseOpenMRSVerticle() {

  override suspend fun start() {
    super.start()

    countRecords(EventBusAddress.OPENMRS_LOCATION_TAGS_COUNT, DatabaseQueries.LOCATION_TAGS_COUNT_QUERY)

    vertx.eventBus().consumer<Int>(EventBusAddress.OPENMRS_LOCATION_TAGS_LOAD).handler {
      replyWithOpenMRSData(it, getLocationTagsQuery(it.body(), limit), LOCATION_TAGS)
    }
  }
}
