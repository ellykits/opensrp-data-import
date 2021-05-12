package org.smartregister.dataimport.shared

/**
 * Event bus addresses. Verticles can subscribe and publish to the following addresses
 */
object EventBusAddress {
  const val TASK_COMPLETE = "task.complete"
  const val CSV_GENERATE = "csv.generate"
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
  const val OAUTH_TOKEN_RECEIVED = "oauth.token.received"
}
