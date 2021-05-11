package org.smartregister.dataimport.shared.model

import kotlinx.serialization.Serializable

@Serializable
data class KeycloakUser(
  val firstName: String,
  val lastName: String,
  val username: String,
  val enabled: Boolean = true,
  val credentials: List<KeycloakCredential> = listOf(KeycloakCredential())
)

@Serializable
data class KeycloakCredential(
  val type: String = "password",
  val value: String = "Test1234",
  val temporary: Boolean = false
)
