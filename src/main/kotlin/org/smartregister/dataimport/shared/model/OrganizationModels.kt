package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable

@Serializable
class Organization {

  constructor() {
    this.active = true
    this.type = OrganizationCoding()
  }

  constructor(
    identifier: String,
    active: Boolean = true,
    name: String,
    type: OrganizationCoding = OrganizationCoding()
  ) : this() {
    this.identifier = identifier
    this.active = active
    this.name = name
    this.type = type
  }

  @CsvBindByPosition(position = 0)
  var identifier: String? = null

  @CsvBindByPosition(position = 1)
  var active: Boolean

  @CsvBindByPosition(position = 2)
  var name: String? = null

  var type: OrganizationCoding
}

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
