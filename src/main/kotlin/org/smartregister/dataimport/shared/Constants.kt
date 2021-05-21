package org.smartregister.dataimport.shared

const val LOCATIONS = "locations"
const val LOCATION_HEADER = "Location"
const val TEAMS = "teams"
const val TEAM_LOCATIONS = "team_locations"
const val USERNAME = "username"
const val ID = "id"
const val USER_ID = "userId"
const val LOCATION_TAGS = "locationTags"
const val ACCESS_TOKEN = "access_token"
const val PRACTITIONERS = "practitioners"
const val PRACTITIONER_ROLES = "practitioner_roles"
const val IDENTIFIER = "identifier"
const val NAME = "name"
const val PROVIDER = "Provider"
const val MAX = "max"
const val FIRST = "first"
const val CREDENTIALS = "credentials"
const val FIRST_NAME = "firstName"
const val LAST_NAME = "lastName"
const val ENABLED = "enabled"
const val ACTIVE = "active"
const val DESCRIPTION = "description"
const val IMPORT_OPTION = "import_option"
const val CSV_GENERATOR = "csv_generator"
const val SOURCE_FILE = "source_file"
const val USERS_FILE = "users_file"
const val SKIP_LOCATION_TAGS = "skipLocationTags"
const val SKIP_USER_GROUP = "skipUserGroup"
const val GENERATE_TEAMS = "generateTeams"
const val OVERWRITE = "overwrite"
const val ACTION = "action"
const val PAYLOAD = "payload"
const val HOME_DIR_PROPERTY = "user.home"
const val OPENSRP_DATA_PATH = "opensrp-data"
const val CIRCUIT_BREAKER_NAME = "opensrp.data.import.circuit"


enum class DataItem {
  LOCATION_TAGS, LOCATIONS, ORGANIZATIONS, ORGANIZATION_LOCATIONS, KEYCLOAK_USERS, KEYCLOAK_USERS_GROUP, PRACTITIONERS, PRACTITIONER_ROLES
}
