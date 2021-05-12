package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable

@Serializable
data class Organization(
  @CsvBindByPosition(position = 0)
  val identifier: String,
  @CsvBindByPosition(position = 1)
  val active: Boolean = true,
  @CsvBindByPosition(position = 2)
  val name: String,

  val type: OrganizationCoding = OrganizationCoding()
)

@Serializable
data class OrganizationCoding(
  val coding: List<FhirCoding> = listOf(FhirCoding())
)

@Serializable
data class FhirCoding(
  val system: String = "http://terminology.hl7.org/CodeSystem/organization-type",
  val code: String = "team",
  val display: String = "Team"
)
