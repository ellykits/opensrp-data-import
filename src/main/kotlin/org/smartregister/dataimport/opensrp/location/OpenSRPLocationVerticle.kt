package org.smartregister.dataimport.opensrp.location

import com.opencsv.CSVReaderBuilder
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import java.io.FileReader
import java.io.IOException
import java.util.*
import kotlinx.coroutines.launch
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.smartregister.dataimport.opensrp.BaseOpenSRPVerticle
import org.smartregister.dataimport.shared.*
import org.smartregister.dataimport.shared.model.*

/**
 * Subclass of [BaseOpenSRPVerticle] responsible for posting OpenSRP locations This verticle can get
 * data from either openmrs or CSV. Using CSV, gives more flexibility
 */
class OpenSRPLocationVerticle : BaseOpenSRPVerticle() {

  private var locationIdsMap = TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER)

  private var organizationUsers = mapOf<String, List<KeycloakUser>>()

  private var keycloakUsers = listOf<KeycloakUser>()

  private val organizations = mutableListOf<Organization>()

  private val organizationLocations = mutableListOf<OrganizationLocation>()

  private var practitioners = listOf<Practitioner>()

  private var practitionerRoles = listOf<PractitionerRole>()

  private val userIdsMap = TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER)

  private var locationOrganizationMap = mapOf<String, OrganizationLocation>()

  private var sourceFile: String? = null

  private var organizationFile: String? = null

  private var loadExistingLocations: Boolean = false

  private val existingLocation = mutableListOf<Location>()

  override suspend fun start() {
    super.start()

    sourceFile = config.getString(SOURCE_FILE)

    loadExistingLocations = config.getBoolean(LOAD_EXISTING_LOCATIONS, false)

    organizationFile = config.getString(ORGANIZATION_LOCATIONS_FILE, "")

    try {
      if (sourceFile.isNullOrBlank()) {
        consumeOpenMRSLocation(action = this::postLocations)
      } else {

        val createNewTeam = config.getString(CREATE_NEW_TEAMS)
        validateLoadExistingLocations(createNewTeam)
        validateCreateUsersInExistingTeams(createNewTeam)
        validateCreateNewUsersInNewTeams(createNewTeam)

        consumeDataFromSources()
        val usersFile = config.getString(USERS_FILE, "")
        if (!usersFile.isNullOrBlank()) {
          extractUsersFromCSV(usersFile)
          updateUserIds(userIdsMap)
        }
      }

      deployVerticle(OpenSRPLocationTagVerticle(), poolName = DataItem.LOCATION_TAGS.name.lowercase())
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

  private fun validateLoadExistingLocations(createNewTeam: String?) {
    //--create-new-teams command option required when running import for existing location. Valid options yes|no
    if (loadExistingLocations && createNewTeam == null) {
      logger.error(
        """

              Error: Missing --create-new-teams command option
              Description: Explicitly specify whether to create new teams when working with existing locations
              Solution:
              Rerun the command with this boolean option "--create-new-teams <<yes|no>>

            """.trimMargin()
      )
      vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
    }
  }

  private fun validateCreateNewUsersInNewTeams(createNewTeam: String?) {
    //Do not provide organization locations file when creating new teams to existing locations
    if (YES.equals(ignoreCase = true, other = createNewTeam) &&
      loadExistingLocations && !organizationFile.isNullOrBlank()
    ) {
      logger.error(
        """

              Error: Organization locations CSV file is not needed.
              Description: Organization locations file is not required when creating new users in new teams for existing
              locations
              Solution:

              1. Remove offending command option "--organization-locations-file <<organization_location.csv>>

            """.trimMargin()
      )
      vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
    }
  }

  private fun validateCreateUsersInExistingTeams(createNewTeam: String?) {
    //organization_locations file is needed when creating new users to existing teams in existing locations
    if (NO.equals(ignoreCase = true, other = createNewTeam) &&
      loadExistingLocations && organizationFile.isNullOrBlank()
    ) {
      logger.error(
        """

              Error: Missing organization locations CSV file.
              Description: Organization locations file is required when importing data for existing locations
              Solution:

              1. Run a query on OpenSRP database to retrieve organization locations in CSV format (organization,jurisdiction)
              2. Rerun the command with this option "--organization-locations-file <<organization_location.csv>>

            """.trimMargin()
      )
      vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
    }
  }

  private fun consumeDataFromSources() {
    //Be notified when locations fetch complete
    vertx.eventBus().consumer<Boolean>(EventBusAddress.OPENSRP_LOCATION_FETCH_COMPLETE) { message ->
      val geoLevels: Map<String, Int> =
        config
          .getString("location.hierarchy")
          .split(',')
          .associateByTo(
            mutableMapOf(),
            { key -> key.split(":").first().trim() },
            { value -> value.split(":").last().trim().toInt() })

      // Generate team and users for locations tagged with hasTeam
      val generateTeams = config.getString(GENERATE_TEAMS, "")

      if (message.body()) {
        launch(vertx.dispatcher()) {
          try {
            val locationsData = locationsData(geoLevels, generateTeams)
            val skipLocations = config.getBoolean(SKIP_LOCATIONS, false)
            if (skipLocations) {
              completeTask(dataItem = DataItem.LOCATIONS, ignored = true)
            } else {
              consumeCSVData(csvData = locationsData, DataItem.LOCATIONS) {
                postData(config.getString("opensrp.rest.location.url"), it, DataItem.LOCATIONS)
              }
            }
          } catch (exception: Exception) {
            logger.error(exception)
          }
        }
      }
    }

    try {
      vertx.eventBus().consumer<String>(EventBusAddress.TASK_COMPLETE).handler { message ->
        when (DataItem.valueOf(message.body())) {
          DataItem.LOCATION_TAGS ->
            launch(vertx.dispatcher()) { extractLocationsFromCSV() }
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
                url = config.getString("opensrp.rest.organization.location.url"),
                data = it,
                dataItem = DataItem.ORGANIZATION_LOCATIONS
              )
            }
          }
          DataItem.ORGANIZATION_LOCATIONS -> {
            if (config.getString(GENERATE_TEAMS, "").isNullOrBlank()) {
              // Skip adding users to keycloak
              completeTask(dataItem = DataItem.KEYCLOAK_USERS, ignored = true)
            } else {
              keycloakUsers =
                organizationUsers.map { it.value }.flatten().onEach {
                  it.practitionerId = UUID.randomUUID().toString()
                  it.organizationLocation = locationIdsMap[locationKey(it)] // name separated with '|' sign
                }

              logger.info("Posting ${keycloakUsers.size} users to Keycloak")
              val keycloakUsersChunked = keycloakUsers.chunked(limit)
              consumeCSVData(keycloakUsersChunked, DataItem.KEYCLOAK_USERS) {
                sendData(EventBusAddress.CSV_KEYCLOAK_USERS_LOAD, it)
              }
            }
          }
          DataItem.KEYCLOAK_USERS -> {
            val usernames = keycloakUsers.filter { it.username != null }.map { it.username!! }
            logger.info("Assigning ${usernames.size} users to 'Provider' group")

            // Skip assigning users to provider group
            if (config.getBoolean(SKIP_USER_GROUP, false)) {
              completeTask(dataItem = DataItem.KEYCLOAK_USERS_GROUP, ignored = true)
            } else {
              val usernamesChunked = usernames.chunked(limit)
              consumeCSVData(usernamesChunked, DataItem.KEYCLOAK_USERS_GROUP) {
                sendData(EventBusAddress.CSV_KEYCLOAK_USERS_GROUP_ASSIGN, it)
              }
            }
          }
          DataItem.KEYCLOAK_USERS_GROUP -> {
            practitioners = generatePractitioners()
            consumeCSVData(practitioners.chunked(limit), DataItem.PRACTITIONERS) {
              postData(config.getString("opensrp.rest.practitioner.url"), it, DataItem.PRACTITIONERS)
            }
          }
          DataItem.PRACTITIONERS -> {
            practitionerRoles = generatePractitionerRoles()
            consumeCSVData(practitionerRoles.chunked(limit), DataItem.PRACTITIONER_ROLES) {
              postData(
                config.getString("opensrp.rest.practitioner.role.url"),
                it,
                DataItem.PRACTITIONER_ROLES
              )
            }
          }
          DataItem.PRACTITIONER_ROLES -> {
            logger.warn("DONE: OpenSRP Data Import completed...sending request to shutdown)")
            vertx.eventBus().send(EventBusAddress.APP_SHUTDOWN, true)
          }
        }
      }
    } catch (throwable: Throwable) {
      vertx.exceptionHandler().handle(throwable)
    }
  }

  private fun generatePractitioners() =
    keycloakUsers
      .filter {
        it.practitionerId != null && it.username != null && userIdsMap.containsKey(it.username)
      }
      .map {
        Practitioner(
          identifier = it.practitionerId!!,
          name = "${it.firstName} ${it.lastName}",
          userId = userIdsMap[it.username]!!,
          username = it.username!!.lowercase()
        )
      }
      .onEach { convertToCSV(DataItem.PRACTITIONERS, it) }

  private fun generatePractitionerRoles() =
    keycloakUsers
      .filter {
        it.practitionerId != null &&
          it.username != null &&
          userIdsMap.containsKey(it.username) &&
          it.organizationLocation != null
      }
      .map {
        PractitionerRole(
          identifier = UUID.randomUUID().toString(),
          organization = locationOrganizationMap.getValue(it.organizationLocation!!).organization!!,
          practitioner = it.practitionerId!!
        )
      }
      .onEach { convertToCSV(DataItem.PRACTITIONER_ROLES, it) }

  private suspend fun postLocations(locations: List<String>) {
    val locationsList: List<Location> = locations.map { Json.decodeFromString(it) }
    val chunkedLocations = locationsList.chunked(limit)
    startVertxCounter(dataItem = DataItem.LOCATIONS, dataSize = chunkedLocations.size.toLong())
    chunkedLocations.forEach {
      awaitEvent<Long> { timer -> vertx.setTimer(getRequestInterval(DataItem.LOCATIONS), timer) }
      postData(config.getString("opensrp.rest.location.url"), it, DataItem.LOCATIONS)
    }
  }

  private suspend fun extractUsersFromCSV(usersFile: String) {
    if (!config.getString(GENERATE_TEAMS, "").isNullOrBlank()) {
      organizationUsers =
        vertx
          .executeBlocking<Map<String, List<KeycloakUser>>> { promise ->
            try {
              val users = readCsvData<KeycloakUser>(fileName = usersFile, fileNameProvided = true, skipLines = 1)
                .groupBy { locationKey(it) }
              promise.complete(users)
            } catch (exception: IOException) {
              promise.fail(exception)
              vertx.exceptionHandler().handle(exception)
            }
          }
          .await()
    }
  }

  private fun locationKey(keycloakUser: KeycloakUser) =
    "${keycloakUser.parentLocation}|${keycloakUser.location}"

  private suspend fun extractLocationsFromCSV() {

    retrieveLocationTags()

    if (loadExistingLocations) {
      val queryParameters = mutableMapOf(
        Pair(SERVER_VERSION, "0"),
        Pair(IS_JURISDICTION, "true"),
        Pair(LIMIT, limit.toString()),
      )
      fetchLocationsRecursively(queryParameters)
    } else {
      vertx.eventBus().send(EventBusAddress.OPENSRP_LOCATION_FETCH_COMPLETE, true)
    }
  }

  private suspend fun locationsData(geoLevels: Map<String, Int>, generateTeams: String?): List<List<Location>> {

    val locationsById = existingLocation.associateBy { it.id }

    if (existingLocation.isNotEmpty()) {
      val locationsMap = existingLocation.associateBy { location ->
        val locationName = location.properties?.name
        val parentId = location.properties?.parentId
        val parentLocationName = if (parentId.isNullOrEmpty()) "" else locationsById.getValue(parentId).properties?.name
        """$parentLocationName${if (parentLocationName.isNullOrEmpty()) "" else "|"}$locationName"""
      }.mapValues { it.value.id!! }
      locationIdsMap.putAll(locationsMap)
    }

    val locationsData: List<List<Location>> = vertx.executeBlocking<List<List<Location>>> { promise ->
      val csvGeneratedLocation = mutableListOf<Location>()

      try {
        val csvReader = CSVReaderBuilder(FileReader(sourceFile!!)).build()
        var cells = csvReader.readNext()
        val headers = cells
        var counter = 1
        if (validateHeaders(headers, promise)) {
          while (cells != null) {
            if (counter > 1) {
              val processedLocations = processLocations(headers, cells, geoLevels)
              processedLocations.forEach {
                val csvLocation =
                  CSVLocation(
                    it.parentId,
                    it.uniqueName,
                    it.id,
                    it.exactName,
                    it.firstLocationTag
                  )
                convertToCSV(DataItem.LOCATIONS, csvLocation)
              }
              csvGeneratedLocation.addAll(processedLocations)
            }
            cells = csvReader.readNext()
            counter++
          }
        }
      } catch (exception: IOException) {
        logError(promise, exception.localizedMessage)
      }

      if (loadExistingLocations && !organizationFile.isNullOrBlank()) {
        val organizationLocations =
          readCsvData<OrganizationLocation>(fileName = organizationFile!!, fileNameProvided = true, skipLines = 1)
        locationOrganizationMap = organizationLocations.associateBy { it.jurisdiction!! }
      }

      val createNewTeam = config.getString(CREATE_NEW_TEAMS, NO).equals(ignoreCase = true, other= YES)
      val newLocations =
        csvGeneratedLocation
          .onEach {
            //When assigning users to new teams to new locations
            if (!loadExistingLocations && it.assignTeam && !generateTeams.isNullOrBlank()) {
              generateOrganizations(it)
            }
          }
          .filter { it.isNew }
          .chunked(limit)

      if (loadExistingLocations && createNewTeam) {
        //Create organizations using the users file
        organizationUsers.forEach {
          val hasNoTeam = it.value.any { keycloakUser -> keycloakUser.team.isNullOrBlank() }
          if (!hasNoTeam) {
            val organizationId = UUID.randomUUID().toString()
            val organization = Organization(identifier = organizationId, name = teamName(it.value.first().team!!))
            organizations.add(organization)
            convertToCSV(DataItem.ORGANIZATIONS, organization)
            val locationKey = locationKey(it.value.first())
            if (locationIdsMap.containsKey(locationKey)) {
              val organizationLocation = OrganizationLocation(organizationId, locationIdsMap.getValue(locationKey))
              organizationLocations.add(organizationLocation)
              convertToCSV(DataItem.ORGANIZATION_LOCATIONS, organizationLocation)
            }
          }
        }
        locationOrganizationMap =
          organizationLocations.filterNot { it.jurisdiction == null }.associateBy { it.jurisdiction!! }
      }

      // Associate location to organization for future mapping with practitioners
      if (!loadExistingLocations) {
        locationOrganizationMap =
          organizationLocations.filterNot { it.jurisdiction == null }.associateBy { it.jurisdiction!! }
      }
      promise.complete(newLocations)

    }.await()
    return locationsData
  }

  private suspend fun fetchLocationsRecursively(queryParameters: MutableMap<String, String>) {
    logger.info("OPENSRP_LOCATION: Fetched location with serverVersion ${queryParameters[SERVER_VERSION]} and limit - $limit")
    awaitEvent<Long> { vertx.setTimer(getRequestInterval(DataItem.LOCATIONS), it) }
    val locationsResponse = awaitResult<HttpResponse<Buffer>?> {
      webRequest(
        method = HttpMethod.GET,
        queryParams = queryParameters,
        url = config.getString("opensrp.rest.location.fetch.url"),
        handler = it
      )
    }
    if (locationsResponse == null || locationsResponse.bodyAsJsonArray().isEmpty) {
      logger.info("OPENSRP_LOCATION: Fetched all ${existingLocation.size} location(s)")
      vertx.eventBus().send(EventBusAddress.OPENSRP_LOCATION_FETCH_COMPLETE, true)
      return
    }

    val locations = jsonEncoder().decodeFromString<List<Location>>(locationsResponse.bodyAsString())
    existingLocation.addAll(locations)
    val maxServerVersion: Long = locations.maxOf { it.serverVersion!! }
    fetchLocationsRecursively(queryParameters.apply { put(SERVER_VERSION, maxServerVersion.plus(1).toString()) })
  }

  private fun validateHeaders(
    headers: Array<String>,
    promise: Promise<List<List<Location>>>
  ): Boolean {

    // Columns must be at least 4 and even number
    val isSizeValid = headers.size % 2 == 0 && headers.size >= 2
    if (!isSizeValid) {
      logError(promise, "INVALID_COLUMNS_COUNT: expected an even number of columns")
      return false
    }

    // Format: Location level ID column followed by the level e.g. Country Id, Country, Province Id, Province
    headers.toList().chunked(2).onEach {
      val (levelId, level) = it

      if (!locationTagsMap.containsKey(level)) {
        logger.warn("Location level MUST be below ROOT (e.g Province:1 comes after Country:0)")
        logError(
          promise,
          "UNRECOGNIZED_TAG: $level does not exist. Import/use correct location tag to continue."
        )
        return false
      }

      if (!levelId.endsWith(ID, true) || !levelId.split(" ").containsAll(level.split(" "))) {
        logError(
          promise,
          """
          INVALID_COLUMN_NAME: ($levelId and $level) columns MUST be correctly and named in the order of
          location levels with the ID column preceding e.g.Country Id, Country, Province Id, Province
        """.trimIndent()
        )
        return false
      }
    }
    return isSizeValid
  }

  private fun logError(
    promise: Promise<List<List<Location>>>,
    message: String,
    throwable: Throwable? = null
  ) {
    promise.fail(message)
    if (throwable != null) {
      vertx.exceptionHandler().handle(throwable)
    } else {
      vertx.exceptionHandler().handle(DataImportException(message))
    }
  }

  private fun processLocations(
    headers: Array<String>,
    values: Array<String>,
    geoLevels: Map<String, Int>
  ): List<Location> {

    val locations = mutableListOf<Location>()

    val chunkedCellRanges =
      headers.zip(values) { header: String, value: String -> CellRange(header, value) }.chunked(2)

    val neighbouringCellRanges = chunkedCellRanges.zipWithNext()

    for ((index, cellRanges: List<CellRange>) in chunkedCellRanges.withIndex()) {
      val idCellRange = cellRanges.first()
      val nameCellRange = cellRanges.last()

      val isNewLocation = idCellRange.value.isBlank() && !loadExistingLocations

      val newLocationId = UUID.randomUUID().toString()
      var parentId = ""
      var parentKey = ""

      // Generate ids for new locations. Also track team location ids
      val key =
        if (index == 0) nameCellRange.value
        else {
          with(neighbouringCellRanges) {
            val currentCellRanges = this[index - 1]
            val (parentCellRanges, childCellRanges) = currentCellRanges
            parentId = parentCellRanges.first().value
            parentKey = getUniqueName(getParentKeyCellRanges(index, neighbouringCellRanges))
            getUniqueName(parentCellRanges.plus(childCellRanges))
          }
        }
      // At this point there is a matching location that was already processed. Also skip locations
      // with no names
      if (!loadExistingLocations && (locationIdsMap.containsKey(key) || nameCellRange.value.isBlank())) {
        continue
      }

      if (!loadExistingLocations) {
        locationIdsMap[key] = if (isNewLocation) newLocationId else idCellRange.value
      }

      val locationTag = nameCellRange.header
      val assignTeam = config.getString(GENERATE_TEAMS, "").equals(locationTag, ignoreCase = true)

      // blank Parent ID means we are processing a location from a row of the csv thus refer to the
      // child location against parent map
      val locationId = locationIdsMap.getValue(key)
      val locationProperties =
        LocationProperties(
          name = nameCellRange.value,
          geographicLevel = geoLevels.getOrDefault(locationTag, 0),
          parentId =
          parentId.ifBlank { locationIdsMap.getOrDefault(parentKey, "") }
        )

      val location =
        Location(
          id = locationId,
          locationTags = listOf(locationTagsMap.getValue(locationTag)),
          properties = locationProperties,
          isNew = isNewLocation,
          assignTeam = assignTeam,
          uniqueName = key,
          uniqueParentName = parentKey,
          firstLocationTag = locationTag
        )
          .apply {
            this.exactName = locationProperties.name
            this.parentId = locationProperties.parentId // Flattened for csv
          }

      locations.add(location)
    }
    return locations
  }

  private fun getParentKeyCellRanges(
    index: Int,
    neighbouringCellRanges: List<Pair<List<CellRange>, List<CellRange>>>
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

  private fun getUniqueName(cellRanges: List<CellRange>) =
    cellRanges
      .filter { !it.header.endsWith(ID, true) }
      .joinToString(separator = "|") { it.value }
      .trim()

  private fun generateOrganizations(location: Location) {
    with(location) {
      try {
        val organizationId = UUID.randomUUID().toString()
        val team = organizationUsers[location.uniqueName]?.first()?.team ?: properties!!.name
        val organization = Organization(identifier = organizationId, name = teamName(team))
        organizations.add(organization)
        convertToCSV(DataItem.ORGANIZATIONS, organization)
        val organizationLocation = OrganizationLocation(organizationId, id!!)
        organizationLocations.add(organizationLocation)
        convertToCSV(DataItem.ORGANIZATION_LOCATIONS, organizationLocation)
      } catch (throwable: Throwable) {
        vertx.exceptionHandler().handle(throwable)
      }
    }
  }

  private fun teamName(team: String): String {
    val teamNamePrefix = config.getString("team.name.prefix", "")
    return if (teamNamePrefix.isNullOrBlank()) team else "$teamNamePrefix $team"
  }

  private inline fun <reified T> convertToCSV(dataItem: DataItem, model: T) {
    vertx
      .eventBus()
      .send(EventBusAddress.CSV_GENERATE, JsonObject().apply {
        put(ACTION, dataItem.name.lowercase())
        put(PAYLOAD, JsonObject(jsonEncoder().encodeToString(model)))
      })
  }
}
