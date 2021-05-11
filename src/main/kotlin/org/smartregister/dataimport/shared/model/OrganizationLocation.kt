package org.smartregister.dataimport.shared.model

import kotlinx.serialization.Serializable

@Serializable
data class OrganizationLocation(
  val organization: String,
  val jurisdiction: String
)
