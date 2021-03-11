package org.smartregister.dataimport.openmrs

import org.smartregister.dataimport.shared.DatabaseQueries
import org.smartregister.dataimport.shared.DatabaseQueries.getTeamsImportQuery
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.TEAMS

/**
 * Subclass of [BaseOpenMRSVerticle] that returns all the available teams in OpenMRS
 */
class OpenMRSTeamVerticle : BaseOpenMRSVerticle() {

  override suspend fun start() {
    super.start()

    countRecords(EventBusAddress.OPENMRS_TEAMS_COUNT, DatabaseQueries.TEAMS_COUNT_QUERY)

    vertx.eventBus().consumer<Int>(EventBusAddress.OPENMRS_TEAMS_LOAD).handler {
      replyWithOpenMRSData(it, getTeamsImportQuery(it.body(), limit), TEAMS)
    }
  }

}
