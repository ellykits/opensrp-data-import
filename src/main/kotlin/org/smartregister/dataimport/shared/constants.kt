package org.smartregister.dataimport.shared

const val LOCATIONS = "locations"
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
const val CIRCUIT_BREAKER_NAME = "opensrp.circuit.breaker"
const val DESCRIPTION = "description"
const val IMPORT_OPTION = "import_option"

enum class Choices {
  LOCATIONS, ORGANIZATIONS, PRACTITIONERS, KEYCLOAK_USERS, ORGANIZATION_LOCATIONS, PRACTITIONER_ROLES
}
