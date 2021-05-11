package org.smartregister.dataimport.shared

import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class DatabaseQueriesTest {

  @Test
  fun `Should return the correct query for importing locations from openmrs`() {
    val locationHierarchies = listOf(
      "Country:0", "Province:1", "District: 2", "Health Facility: 3", "Residential Area : 5",
      "Health Center Urban: 3", "Health Center Rural:3"
    )
    val locationsImportQuery = DatabaseQueries.getLocationsImportQuery(0, locationHierarchies, 100)
    assertNotNull(locationsImportQuery)
    assertTrue(locationsImportQuery.contains("WHEN t.name = 'Country' THEN 0"))
    assertTrue(locationsImportQuery.contains("WHEN t.name = 'Province' THEN 1"))
    assertTrue(locationsImportQuery.contains("WHEN t.name = 'District' THEN 2"))
    assertTrue(locationsImportQuery.contains("WHEN t.name = 'Health Facility' THEN 3"))
    assertTrue(locationsImportQuery.contains("WHEN t.name = 'Residential Area' THEN 5"))
    assertTrue(locationsImportQuery.contains("WHEN t.name = 'Health Center Urban' THEN 3"))
    assertTrue(locationsImportQuery.contains("WHEN t.name = 'Health Center Rural' THEN 3"))
  }
}
