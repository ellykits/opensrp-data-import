package org.smartregister.dataimport.shared.model

import kotlinx.serialization.Serializable

@Serializable
data class Practitioner(
  val identifier: String,
  val active: Boolean = true,
  val name: String,
  val userId: String,
  val username: String
)
