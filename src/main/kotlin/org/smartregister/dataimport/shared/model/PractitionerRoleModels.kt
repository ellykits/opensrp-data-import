package org.smartregister.dataimport.shared.model

import kotlinx.serialization.Serializable

@Serializable
data class PractitionerRole(
  val identifier: String,
  val active: Boolean = true,
  val organization: String,
  val practitioner: String,
  val code: PractitionerCode
)

@Serializable
data class PractitionerCode(
  val text: String = "Health Worker"
)
