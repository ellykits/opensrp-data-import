package org.smartregister.dataimport.shared

/**
 * Event bus addresses. Verticles can subscribe and publish to the following addresses
 */
object EventBusAddress {
  const val APP_SHUTDOWN = "app.shutdown"
  const val USER_FOUND = "user.found"
  const val TASK_COMPLETE = "task.complete"
  const val CSV_GENERATE = "csv.generate"
  const val CSV_KEYCLOAK_USERS_LOAD = "csv.keycloak.users.load"
  const val CSV_KEYCLOAK_USERS_GROUP_ASSIGN = "csv.keycloak.users.group.assign"
  const val OPENMRS_USERS_LOAD = "openmrs.users.load"
  const val OPENMRS_USERS_COUNT = "openmrs.users.count"
  const val OPENMRS_LOCATIONS_LOAD = "openmrs.locations.load"
  const val OPENMRS_TEAMS_LOAD = "openmrs.teams.load"
  const val OPENMRS_TEAM_LOCATIONS_LOAD = "openmrs.team.locations.load"
  const val OPENMRS_LOCATIONS_COUNT = "openmrs.location.count"
  const val OPENMRS_TEAMS_COUNT = "openmrs.teams.count"
  const val OPENMRS_TEAM_LOCATIONS_COUNT = "openmrs.team.locations.count"
  const val OPENMRS_USER_ROLE_COUNT = "openmrs.user.role.count"
  const val OPENMRS_USER_ROLE_LOAD = "openmrs.user.role.load"
  const val OPENMRS_TASK_COMPLETE = "openmrs.task.complete"
  const val OPENMRS_KEYCLOAK_USERS_GROUP_ASSIGN = "openmrs.keycloak.users.group.assign"
  const val OAUTH_TOKEN_RECEIVED = "oauth.token.received"
}
