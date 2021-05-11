package org.smartregister.dataimport.shared

object DatabaseQueries {

  const val TEAMS_COUNT_QUERY = "SELECT count(*) from team;"

  const val USERS_ROLE_COUNT_QUERY = """
  SELECT count(*)
  FROM team_member tm
           INNER JOIN person p on tm.person_id = p.person_id
           INNER JOIN team t on t.team_id = tm.team_id
  """

  const val USERS_COUNT_QUERY = """
  SELECT count(*) count
  FROM users u
           INNER JOIN person_name pn ON u.person_id = pn.person_id
           INNER JOIN person p on pn.person_id = p.person_id
  WHERE (u.username != 'openmrs' AND u.username != 'daemon');
  """

  const val TEAM_LOCATIONS_COUNT_QUERY = """
  SELECT count(*) count
  FROM team
         INNER JOIN location ON team.location_id = location.location_id;
  """

  const val LOCATIONS_COUNT_QUERY = """
  SELECT count(*) count
  FROM location l
           LEFT JOIN location p on p.location_id = l.parent_location
           LEFT JOIN location_tag_map tm on l.location_id = tm.location_id
           LEFT JOIN location_tag t on t.location_tag_id = tm.location_tag_id;
  """

  fun getLocationsImportQuery(offset: Int, locationHierarchy: List<String>, limit: Int): String {
    val tagExpression = StringBuilder("CASE\n")
    locationHierarchy.forEach { tag ->
      val tagSplit = tag.split(":")
      tagExpression.append("WHEN t.name = '${tagSplit.first().trim()}' THEN ${tagSplit.last().trim()}\n")
    }
    tagExpression.append("END")
    val query = """
     SELECT json_object(
                 'id', location_id,
                 'type', location_type,
                 'properties', (json_object(
                  'name', location_name,
                  'status', location_status,
                  'parentId', location_parent_id,
                  'version', location_version,
                  'geographicLevel', location_geographic_level)),
                 'locationTags', (json_array(json_object('id', tag_id,
                                                         'active', tag_active,
                                                         'name', tag_name,
                                                         'description', tag_description
              )))
             )
             as locations
     FROM (SELECT l.uuid                                                           location_id,
                   'Feature'                                                        location_type,
                   l.name                                                           location_name,
                   if(p.uuid is null, '', p.uuid)                                   location_parent_id,
                   'Active'                                                         location_status,
                   0                                                                location_version,
                   $tagExpression                                                   location_geographic_level,
                   t.location_tag_id                                                tag_id,
                   t.name                                                           tag_name,
                   if(t.description is null, concat(t.name, ' Tag'), t.description) tag_description,
                   if(t.retired = 0, 'true', 'false')                               tag_active
     FROM location l
                 LEFT JOIN location p on p.location_id = l.parent_location
                 LEFT JOIN location_tag_map tm on l.location_id = tm.location_id
                 LEFT JOIN location_tag t on t.location_tag_id = tm.location_tag_id
     LIMIT $offset,$limit) AS lt
    """
    return query.trimIndent()
  }

  fun getTeamsImportQuery(offset: Int, limit: Int) =
    """
    SELECT json_object(
                   'identifier', identifier,
                   'active', 'true',
                   'name', team_name,
                   'type', (json_object(
                    'coding', json_array(
                            json_object('system', 'http://terminology.hl7.org/CodeSystem/organization-type',
                                        'code', 'team',
                                        'display', 'Team'
                                ))))
               )
               as teams
    FROM (SELECT t.uuid identifier,
                 t.name team_name
          FROM team t
          LIMIT $offset, $limit) AS tms
    """.trimIndent()

  fun getTeamLocationsImportQuery(offset: Int, limit: Int) =
    """
    SELECT json_object('organization', team.uuid, 'jurisdiction', location.uuid) team_locations
    FROM team
             INNER JOIN location ON team.location_id = location.location_id
    LIMIT $offset,$limit;
    """.trimIndent()

  fun getUsersImportQuery(offset: Int, limit: Int) =
    """
    SELECT json_object(
                   'identifier', p.uuid,
                   'username', u.username,
                   'firstName', pn.given_name,
                   'lastName', pn.family_name,
                   'name', concat(pn.given_name, ' ', pn.family_name),
                   'enabled', if(retired = 0, 'true', 'false'),
                   'credentials', json_array(
                           json_object('type', 'password',
                                       'value', 'Test1234',
                                       'temporary', true)
                       )) practitioners
    FROM users u
             INNER JOIN person_name pn ON u.person_id = pn.person_id
             INNER JOIN person p on pn.person_id = p.person_id
    WHERE (u.username != 'openmrs' AND u.username != 'daemon')
    LIMIT $offset,$limit;
    """.trimIndent()

  fun getUserRoleImportQuery(offset: Int, limit: Int) =
    """
    SELECT json_object(
               'identifier', tm.uuid,
               'active', if(t.voided = 0, 'true', 'false'),
               'organization', t.uuid,
               'practitioner', p.uuid,
               'code', json_object('text', 'Health Worker')
           ) practitioner_roles
    FROM team_member tm
             INNER JOIN person p on tm.person_id = p.person_id
             INNER JOIN team t on t.team_id = tm.team_id
    LIMIT $offset, $limit;
    """.trimIndent()

}
