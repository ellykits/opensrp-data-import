package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient

@Serializable
class KeycloakUser {

  constructor()

  constructor(firstName: String, lastName:String, username:String ): this(){
    this.firstName = firstName
    this.lastName = lastName
    this.username = username
  }
  @Transient
  @CsvBindByPosition(position = 0)
  var parentLocation: String? = null

  @Transient
  @CsvBindByPosition(position = 1)
  var location: String? = null

  @Transient
  @CsvBindByPosition(position = 2)
  var team: String? = null

  @CsvBindByPosition(position = 3)
  var firstName: String? = null

  @CsvBindByPosition(position = 4)
  var lastName: String? = null

  @CsvBindByPosition(position = 5)
  var username: String? = null

  @Transient
  @CsvBindByPosition(position = 6)
  var password: String = "Test1234"
    set(value) {
      field = value
      credentials[0].value = value
    }

  val enabled: Boolean = true

  @Transient
  var organizationLocation: String? = null

  @Transient
  var practitionerId: String? = null

  val credentials: List<KeycloakCredential> = listOf(KeycloakCredential())
}

@Serializable
data class KeycloakCredential(
  var type: String = "password",
  var value: String = "Test1234",
  var temporary: Boolean = false
)
