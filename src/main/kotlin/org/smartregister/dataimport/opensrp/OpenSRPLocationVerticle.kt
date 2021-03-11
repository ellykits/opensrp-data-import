package org.smartregister.dataimport.opensrp

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.smartregister.dataimport.openmrs.OpenMRSLocationVerticle
import org.smartregister.dataimport.shared.EventBusAddress
import org.smartregister.dataimport.shared.ID
import org.smartregister.dataimport.shared.LOCATION_TAGS

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP locations
 */
class OpenSRPLocationVerticle : BaseOpenSRPVerticle() {

  override suspend fun start() {
    super.start()
    vertx.deployVerticle(OpenMRSLocationVerticle())
    consumeData(
      countAddress = EventBusAddress.OPENMRS_LOCATIONS_COUNT,
      loadAddress = EventBusAddress.OPENMRS_LOCATIONS_LOAD,
      block = this::postLocations
    )
  }

  private suspend fun postLocations(locations: JsonArray) {
    launch (Dispatchers.IO) {
      locations.forEach { location ->
        //Delete locationTags attributes for locations without tags
        if (location is JsonObject && location.containsKey(LOCATION_TAGS)) {
          val locationTags = location.getJsonArray(LOCATION_TAGS)
          locationTags.forEach { tag ->
            if (tag is JsonObject && tag.getString(ID) == null) {
              location.remove(LOCATION_TAGS)
            }
          }
        }
      }
      webRequest(url = config.getString("opensrp.rest.location.url"), payload = locations)
        ?.logHttpResponse()
    }
  }
}
