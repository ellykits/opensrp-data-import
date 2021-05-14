package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable

@Serializable
class Practitioner() {

  constructor(identifier: String, active: Boolean = true, name: String, userId: String, username: String) : this() {
    this.identifier = identifier
    this.active = active
    this.name = name
    this.userId = userId
    this.username = username
  }

  @CsvBindByPosition(position = 0)
  var identifier: String? = null

  @CsvBindByPosition(position = 1)
  var active: Boolean = true

  @CsvBindByPosition(position = 2)
  var name: String? = null

  @CsvBindByPosition(position = 3)
  var userId: String? = null

  @CsvBindByPosition(position = 4)
  var username: String? = null
}
