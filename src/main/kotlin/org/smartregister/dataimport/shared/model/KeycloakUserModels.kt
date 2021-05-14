package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import java.util.*

@Serializable
class KeycloakUser {

  @Transient
  @CsvBindByPosition(position = 0)
  var parentLocation: String? = null

  @Transient
  @CsvBindByPosition(position = 1)
  var location: String? = null

  @CsvBindByPosition(position = 2)
  var firstName: String? = null

  @CsvBindByPosition(position = 3)
  val lastName: String? = null

  @CsvBindByPosition(position = 4)
  val username: String? = null

  @Transient
  @CsvBindByPosition(position = 5)
  var password: String = "Test1234"
    set(value) {
      field = value
      credentials[0].value = value
    }

  val enabled: Boolean = true

  @Transient
  var organizationLocation: String? = null

  @Transient
  var practitionerId: String = UUID.randomUUID().toString()

  val credentials: List<KeycloakCredential> = listOf(KeycloakCredential())
}

@Serializable
data class KeycloakCredential(
  var type: String = "password",
  var value: String = "Test1234",
  var temporary: Boolean = false
)
