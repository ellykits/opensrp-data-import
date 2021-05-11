package org.smartregister.dataimport.shared.model

import kotlinx.serialization.Serializable

@Serializable
data class Organization(
  val identifier: String,
  val active: Boolean = true,
  val name: String,
  val type: OrganizationCoding
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
