package org.smartregister.dataimport.opensrp

import com.opencsv.CSVReaderBuilder
import io.vertx.core.Promise
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.smartregister.dataimport.openmrs.OpenMRSLocationVerticle
import org.smartregister.dataimport.shared.*
import org.smartregister.dataimport.shared.model.*
import java.io.FileReader
import java.util.*

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP locations
 */
class OpenSRPLocationVerticle : BaseOpenSRPVerticle() {

  private var locationTagsMap = mapOf<String, LocationTag>()

  private var locationIds = mutableMapOf<String, String>()

  override suspend fun start() {
    super.start()

    val sourceFile = config.getString(SOURCE_FILE)

    if (sourceFile.isNullOrBlank()) {
      vertx.deployVerticle(OpenMRSLocationVerticle())
      consumeData(
        countAddress = EventBusAddress.OPENMRS_LOCATIONS_COUNT,
        loadAddress = EventBusAddress.OPENMRS_LOCATIONS_LOAD,
        block = this::postLocations
      )
    } else {
      extractLocationsFromCSV(sourceFile)
    }
  }

  private suspend fun postLocations(locations: JsonArray) {
    launch(Dispatchers.IO) {
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
      webRequest(url = config.getString("opensrp.rest.location.url"), payload = locations)?.logHttpResponse()
    }
  }

  private suspend fun extractLocationsFromCSV(sourceFile: String?) {
    val geoLevels = config.getString("location.hierarchy")
      .split(',').associateByTo(mutableMapOf(), { key: String ->
        key.split(":").first().trim()
      }, { value: String ->
        value.split(":").last().trim().toInt()
      })

    val locationTags = webRequest(HttpMethod.GET, config.getString("opensrp.rest.location.tag.url"))?.body()

    if (locationTags != null && sourceFile != null) {

      locationTagsMap = Json.decodeFromString<List<LocationTag>>(locationTags.toString()).associateBy { it.name }

      val locationsData: List<List<Location>> = vertx.executeBlocking<List<List<Location>>> { promise ->

        val locations = mutableListOf<Location>()

        try {
          val csvReader = CSVReaderBuilder(FileReader(sourceFile)).build()
          var cells = csvReader.readNext()
          val headers = cells
          var counter = 1
          if (validateHeaders(headers, promise)) {
            while (cells != null) {
              if (counter > 1) {
                locations.addAll(processLocations(headers, cells, geoLevels))
              }
              cells = csvReader.readNext()
              counter++
            }
          }
        } catch (exception: NoSuchFileException) {
          logError(promise, exception.localizedMessage)
        }


        //Generate team and users for locations tagged with hasTeam
        val generateTeams = config.getString(GENERATE_TEAMS, "")

        locations.filter { it.hasTeam }.associateBy { it.id }.map { it.value }.forEach {
          if (generateTeams.isNotBlank()) {
            createOrganizations(it)
          }
        }
        val newLocations = locations.filter { it.isNew }.associateBy { it.id }.map { it.value }.chunked(limit)
        promise.complete(newLocations)
      }.await()


      val counter = vertx.sharedData().getCounter(COUNTER).await()
      vertx.setPeriodic(requestInterval) { timerId ->
        launch(vertx.dispatcher()) {
          val index: Int = counter.andIncrement.await().toInt()
          if (index >= locationsData.size) {
            endOperation(EventBusAddress.OPENMRS_LOCATIONS_LOAD)
            vertx.cancelTimer(timerId)
          }
          if (index < locationsData.size) {
            val locations = locationsData[index]
            val payload = JsonArray(Json.encodeToString(locations))
            try {
              webRequest(url = config.getString("opensrp.rest.location.url"), payload = payload)?.logHttpResponse()
              logger.info("Posted ${locations.size} locations")
            } catch (throwable: Throwable) {
              vertx.exceptionHandler().handle(throwable)
            }
          }
        }
      }
    }
  }

  private fun validateHeaders(headers: Array<String>, promise: Promise<List<List<Location>>>): Boolean {

    //Columns must be at least 4 and even number
    val isSizeValid = headers.size % 2 == 0 && headers.size >= 4
    if (!isSizeValid) {
      logError(promise, "Error: CSV format not valid - expected an even number of at least 4 columns")
      return false
    }

    //Columns MUST be named in the order of location level with the id column preceding the level e.g. Country Id, Country, Province Id, Province
    headers.toList().chunked(2).onEach {
      val (levelId, level) = it

      if (!locationTagsMap.containsKey(level)) {
        logError(promise, "Error: Location tag $level does not exist. Import location tags and continue.")
        return false
      }

      if (!levelId.endsWith(ID, true) || !levelId.split(" ").containsAll(level.split(" "))) {
        logError(
          promise, """
          Error: INCORRECT format for columns ($levelId and $level)
          Columns MUST be named in the order of location levels with the id column preceding e.g. Country Id, Country, Province Id, Province
        """.trimIndent()
        )
        return false
      }
    }
    return isSizeValid
  }

  private fun logError(promise: Promise<List<List<Location>>>, message: String, throwable: Throwable? = null) {
    promise.fail(message)
    if (throwable != null) {
      vertx.exceptionHandler().handle(throwable)
    } else {
      vertx.exceptionHandler().handle(DataImportException(message))
    }
    vertx.close()
  }

  private fun processLocations(header: Array<String>, cells: Array<String>, geoLevels: MutableMap<String, Int>)
    : List<Location> {

    var parentIdPos = 0
    var parentNamePos = 1
    var idPos = 2
    var namePos = 3

    val locations = mutableListOf<Location>()
    val zippedList = header.zip(cells) //Combine header with row to know the correct location level for the value

    do {
      val locationTag = zippedList[namePos].first
      val parentId = zippedList[parentIdPos].second
      val name = zippedList[namePos].second
      val key = zippedList[parentNamePos].second.plus(name)
      var isNewLocation = false

      //New Locations should not have ids system should generate these
      var id = zippedList[idPos].second
      if (id.isBlank()) {
        id = locationIds.getOrPut(key) { UUID.randomUUID().toString() }
        isNewLocation = true
      }

      val location = Location(
        id = id,
        locationTags = listOf(locationTagsMap.getValue(locationTag)),
        properties = LocationProperties(
          parentId = parentId,
          name = name,
          geographicalLevel = geoLevels.getOrDefault(locationTag, 0)
        ),
        isNew = isNewLocation,
        hasTeam = config.getString(GENERATE_TEAMS, "").equals(locationTag, ignoreCase = true)
      )

      locations.add(location)

      parentIdPos += 2
      parentNamePos = parentIdPos + 1
      idPos = parentIdPos + 2
      namePos = idPos + 1

    } while (parentIdPos <= zippedList.size / 2)

    //Use previous location id as the parent id of the next - also include the first item in the list which is always ignored with zip
    return locations.subList(0, 1)
      .plus(locations.zipWithNext().map {
        it.second.copy().apply { properties.parentId = it.first.id }
      })
  }

  private fun createOrganizations(location: Location) {
    with(location){
      try {
        val organizationId = UUID.randomUUID().toString()

        vertx.eventBus().send(EventBusAddress.CSV_GENERATE, JsonObject().apply {
          put(FILE_NAME, Choices.ORGANIZATIONS.name.lowercase())
          put(PAYLOAD, JsonObject(Json.encodeToString(Organization(identifier = organizationId, name = "Team ${properties.name}"))))
        })

        vertx.eventBus().send(EventBusAddress.CSV_GENERATE, JsonObject().apply {
          put(FILE_NAME, Choices.ORGANIZATION_LOCATIONS.name.lowercase())
          put(PAYLOAD, JsonObject(Json.encodeToString(OrganizationLocation(organizationId, id))))
        })

      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }
}
