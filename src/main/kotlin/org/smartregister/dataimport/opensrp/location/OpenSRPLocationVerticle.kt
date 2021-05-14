package org.smartregister.dataimport.opensrp.location

import com.opencsv.CSVReaderBuilder
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.smartregister.dataimport.keycloak.KeycloakUserVerticle
import org.smartregister.dataimport.openmrs.OpenMRSLocationVerticle
import org.smartregister.dataimport.opensrp.BaseOpenSRPVerticle
import org.smartregister.dataimport.shared.*
import org.smartregister.dataimport.shared.model.*
import java.io.FileReader
import java.io.IOException
import java.util.*

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP locations
 * This verticle can get data from either openmrs or CSV. Using CSV, gives more flexibility
 */
class OpenSRPLocationVerticle : BaseOpenSRPVerticle() {

  private var locationTagsMap = mapOf<String, LocationTag>()

  private var locationIdsMap = mutableMapOf<String, String>()

  private var organizationUsers = mapOf<String, List<KeycloakUser>>()

  private var keycloakUsers = listOf<KeycloakUser>()

  private val organizations = mutableListOf<Organization>()

  private val organizationLocations = mutableListOf<OrganizationLocation>()

  private var practitioners = listOf<Practitioner>()

  private var practitionerRoles = listOf<PractitionerRole>()

  private val userIdsMap = TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER)

  private var locationOrganizationMap = mapOf<String, OrganizationLocation>()

  override suspend fun start() {
    super.start()

    val sourceFile = config.getString(SOURCE_FILE)

    if (sourceFile.isNullOrBlank()) {
      vertx.deployVerticle(OpenMRSLocationVerticle())
      consumeOpenMRSData(
        countAddress = EventBusAddress.OPENMRS_LOCATIONS_COUNT,
        loadAddress = EventBusAddress.OPENMRS_LOCATIONS_LOAD,
        action = this::postLocations
      )
    } else {
      try {
        val usersFile = config.getString(USERS_FILE)

        if (usersFile.isNotBlank()) {
          extractUsersFromCSV(usersFile)
          deployVerticle(KeycloakUserVerticle(), commonConfigs())
        }

        extractLocationsFromCSV(sourceFile)

        //Begin by posting locations from CSV, then organization, map organizations to locations, post keycloak users,
        // create practitioners and finally assign practitioners to organizations
        cascadeDataImportation()

        updateUserIds(userIdsMap)
      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  private fun cascadeDataImportation() {
    try {
      vertx.eventBus().consumer<String>(EventBusAddress.TASK_COMPLETE).handler { message ->
        when (DataItem.valueOf(message.body())) {
          DataItem.LOCATIONS -> {
            val organizationsChunked = organizations.chunked(limit)
            consumeCSVData(organizationsChunked, DataItem.ORGANIZATIONS) {
              postData(config.getString("opensrp.rest.organization.url"), it, DataItem.ORGANIZATIONS)
            }
          }
          DataItem.ORGANIZATIONS -> {
            val organizationLocationsChunked = organizationLocations.chunked(limit)
            consumeCSVData(organizationLocationsChunked, DataItem.ORGANIZATION_LOCATIONS) {
              postData(
                config.getString("opensrp.rest.organization.location.url"), it, DataItem.ORGANIZATION_LOCATIONS
              )
            }
          }
          DataItem.ORGANIZATION_LOCATIONS -> {
            keycloakUsers = organizationUsers.map { it.value }.flatten().onEach {
              it.organizationLocation = locationIdsMap[it.parentLocation + it.location]
            }
            logger.info("Posting ${keycloakUsers.size} users to Keycloak")
            val keycloakUsersChunked = keycloakUsers.chunked(limit)
            consumeCSVData(keycloakUsersChunked, DataItem.KEYCLOAK_USERS) {
              sendData(EventBusAddress.CSV_KEYCLOAK_USERS_LOAD, DataItem.KEYCLOAK_USERS, it)
            }
          }
          DataItem.KEYCLOAK_USERS -> {
            val usernames = keycloakUsers.filter { it.username != null }.map { it.username!! }
            logger.info("Assigning ${usernames.size} users to 'Provider' group")

            //Skip assigning users to provider group
            if (config.getBoolean(SKIP_USER_GROUP, false)) {
              completeTask(dataItem = DataItem.KEYCLOAK_USERS_GROUPS)
            }

            val usernamesChunked = usernames.chunked(limit)
            consumeCSVData(usernamesChunked, DataItem.KEYCLOAK_USERS_GROUPS) {
              sendData(EventBusAddress.CSV_KEYCLOAK_USERS_GROUP_ASSIGN, DataItem.KEYCLOAK_USERS_GROUPS, it)
            }
          }
          DataItem.KEYCLOAK_USERS_GROUPS -> {
            practitioners = generatePractitioners()
            consumeCSVData(practitioners.chunked(limit), DataItem.PRACTITIONERS) {
              postData(config.getString("opensrp.rest.practitioner.url"), it, DataItem.PRACTITIONERS)
            }
          }
          DataItem.PRACTITIONERS -> {
            practitionerRoles = generatePractitionerRoles()
            consumeCSVData(practitionerRoles.chunked(limit), DataItem.PRACTITIONER_ROLES) {
              postData(config.getString("opensrp.rest.practitioner.role.url"), it, DataItem.PRACTITIONER_ROLES)
            }
          }
          DataItem.PRACTITIONER_ROLES -> {
            logger.warn("DONE: OpenSRP Data Import completed...sending request to shutdown)")
            vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
          }
          else -> vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
        }
      }
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

  private fun generatePractitioners() =
    keycloakUsers.filter { it.username != null && userIdsMap.containsKey(it.username) }.map {
      Practitioner(
        identifier = it.practitionerId,
        name = "${it.firstName} ${it.lastName}",
        userId = userIdsMap[it.username]!!,
        username = it.username!!.lowercase()
      )
    }.onEach { convertToCSV(DataItem.PRACTITIONERS, it) }

  private fun generatePractitionerRoles() =
    keycloakUsers.filter { it.username != null && userIdsMap.containsKey(it.username) && it.organizationLocation != null }
      .map {
        PractitionerRole(
          identifier = UUID.randomUUID().toString(),
          organization = locationOrganizationMap.getValue(it.organizationLocation!!).organization!!,
          practitioner = it.practitionerId
        )
      }.onEach { convertToCSV(DataItem.PRACTITIONER_ROLES, it) }

  private suspend fun postLocations(locations: JsonArray) {
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
    awaitResult<HttpResponse<Buffer>?> {
      webRequest(
        url = config.getString("opensrp.rest.location.url"),
        payload = locations,
        handler = it
      )
    }?.logHttpResponse()
  }

  private suspend fun extractUsersFromCSV(usersFile: String) {
    organizationUsers = vertx.executeBlocking<Map<String, List<KeycloakUser>>> { promise ->
      try {
        val users = readCsvData<KeycloakUser>(usersFile, true, 1)
          .groupBy { it.parentLocation + it.location }
        promise.complete(users)
      } catch (exception: IOException) {
        vertx.exceptionHandler().handle(exception)
      }
    }.await()
  }

  private suspend fun extractLocationsFromCSV(sourceFile: String?) {
    val geoLevels: Map<String, Int> = config.getString("location.hierarchy")
      .split(',').associateByTo(mutableMapOf(), { key ->
        key.split(":").first().trim()
      }, { value -> value.split(":").last().trim().toInt() })

    //Generate team and users for locations tagged with hasTeam
    val generateTeams = config.getString(GENERATE_TEAMS, "")

    val locationTags = awaitResult<HttpResponse<Buffer>?> {
      webRequest(
        method = HttpMethod.GET,
        url = config.getString("opensrp.rest.location.tag.url"),
        handler = it
      )
    }?.body()

    if (locationTags != null && !sourceFile.isNullOrBlank()) {

      locationTagsMap = Json.decodeFromString<List<LocationTag>>(locationTags.toString()).associateBy { it.name }

      val locationsData: List<List<Location>> = vertx.executeBlocking<List<List<Location>>> { promise ->

        val allLocations = mutableListOf<Location>()

        try {
          val csvReader = CSVReaderBuilder(FileReader(sourceFile)).build()
          var cells = csvReader.readNext()
          val headers = cells
          var counter = 1
          if (validateHeaders(headers, promise)) {
            while (cells != null) {
              if (counter > 1) {
                val processedLocations = processLocations(headers, cells, geoLevels)
                allLocations.addAll(processedLocations)
              }
              cells = csvReader.readNext()
              counter++
            }
          }
        } catch (exception: IOException) {
          logError(promise, exception.localizedMessage)
        }

        val newLocations = allLocations.filter { it.isNew }.onEach {
          if (it.assignTeam && !generateTeams.isNullOrBlank()) {
            generateOrganizations(it)
          }
        }.chunked(limit)

        //Associate location to organization for future mapping with practitioners
        locationOrganizationMap = organizationLocations.filterNot { it.jurisdiction == null }
          .associateBy { it.jurisdiction!! }

        promise.complete(newLocations)
      }.await()

      consumeCSVData(csvData = locationsData, DataItem.LOCATIONS) {
        postData(config.getString("opensrp.rest.location.url"), it, DataItem.LOCATIONS)
      }
    }
  }

  private fun validateHeaders(headers: Array<String>, promise: Promise<List<List<Location>>>): Boolean {

    //Columns must be at least 4 and even number
    val isSizeValid = headers.size % 2 == 0 && headers.size >= 2
    if (!isSizeValid) {
      logError(promise, "INVALID_COLUMNS_COUNT: expected an even number of columns")
      return false
    }

    //Format: Location level ID column followed by the level e.g. Country Id, Country, Province Id, Province
    headers.toList().chunked(2).onEach {
      val (levelId, level) = it

      if (!locationTagsMap.containsKey(level)) {
        logger.warn("Location level MUST be below ROOT (e.g Province:1 comes after Country:0)")
        logError(promise, "UNRECOGNIZED_TAG: $level does not exist. Import/use correct location tag to continue.")
        return false
      }

      if (!levelId.endsWith(ID, true) || !levelId.split(" ").containsAll(level.split(" "))) {
        logError(
          promise, """
          INVALID_COLUMN_NAME: ($levelId and $level) columns MUST be correctly and named in the order of
          location levels with the ID column preceding e.g.Country Id, Country, Province Id, Province
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
  }

  private fun processLocations(headers: Array<String>, values: Array<String>, geoLevels: Map<String, Int>)
    : List<Location> {

    val locations = mutableListOf<Location>()

    val chunkedCellRanges = headers.zip(values) { header: String, value: String -> CellRange(header, value) }.chunked(2)

    val neighbouringCellRanges = chunkedCellRanges.zipWithNext()

    for ((index, cellRanges: List<CellRange>) in chunkedCellRanges.withIndex()) {
      val idCellRange = cellRanges.first()
      val nameCellRange = cellRanges.last()

      val isNewLocation = idCellRange.value.isBlank()

      val newLocationId = UUID.randomUUID().toString()
      var parentId = ""
      var parentKey = ""

      //Generate ids for new locations. Also track team location ids
      val key = if (index == 0) nameCellRange.value else {
        with(neighbouringCellRanges) {
          val currentCellRanges = this[index - 1]
          val (parentCellRanges, childCellRanges) = currentCellRanges
          parentId = parentCellRanges.first().value
          parentKey = getUniqueName(getParentKeyCellRanges(index, neighbouringCellRanges))
          getUniqueName(parentCellRanges.plus(childCellRanges))
        }
      }

      if (locationIdsMap.containsKey(key)) { //At this point there is a matching location that was already processed
        continue
      }

      locationIdsMap[key] = if (isNewLocation) newLocationId else idCellRange.value

      val locationTag = nameCellRange.header
      val assignTeam = config.getString(GENERATE_TEAMS, "").equals(locationTag, ignoreCase = true)

      //blank Parent Id means we are processing a location from a row of the csv thus refer to the child location against parent map
      val locationId = locationIdsMap.getValue(key)
      val locationProperties =
        LocationProperties(
          name = nameCellRange.value,
          geographicLevel = geoLevels.getOrDefault(locationTag, 0),
          parentId = if (parentId.isBlank()) locationIdsMap.getOrDefault(parentKey, "") else parentId
        )

      val location = Location(
        id = locationId,
        locationTags = listOf(locationTagsMap.getValue(locationTag)),
        properties = locationProperties,
        isNew = isNewLocation,
        assignTeam = assignTeam,
        uniqueName = key,
        uniqueParentName = parentKey
      )
      locations.add(location)
    }
    return locations
  }

  private fun getParentKeyCellRanges(
    index: Int, neighbouringCellRanges: List<Pair<List<CellRange>, List<CellRange>>>
  ): MutableList<CellRange> {

    var currentIndex = index - 1
    var limit = 0

    val parentNameCellRanges = mutableListOf<CellRange>()
    while (currentIndex >= 0) {
      if (limit == 2) {
        break
      }
      parentNameCellRanges.addAll(neighbouringCellRanges[currentIndex].first)
      currentIndex--
      limit++
    }
    return parentNameCellRanges.asReversed()
  }

  private fun getUniqueName(cellRanges: List<CellRange>) = cellRanges.filter { !it.header.endsWith(ID, true) }
    .joinToString(separator = "") { it.value }.trim()

  private fun generateOrganizations(location: Location) {
    with(location) {
      try {
        val organizationId = UUID.randomUUID().toString()

        val organization = Organization(identifier = organizationId, name = "Team ${properties.name}")
        organizations.add(organization)
        convertToCSV(DataItem.ORGANIZATIONS, organization)

        val organizationLocation = OrganizationLocation(organizationId, id)
        organizationLocations.add(organizationLocation)
        convertToCSV(DataItem.ORGANIZATION_LOCATIONS, organizationLocation)

      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  private inline fun <reified T> convertToCSV(dataItem: DataItem, model: T) {
    vertx.eventBus().send(EventBusAddress.CSV_GENERATE, JsonObject().apply {
      put(ACTION, dataItem.name.lowercase())
      put(PAYLOAD, JsonObject(jsonEncoder().encodeToString(model)))
    })
  }
}
