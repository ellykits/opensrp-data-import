package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable

@Serializable
class PractitionerRole (){

  constructor(
    identifier: String,
    active: Boolean = true,
    organization: String,
    practitioner: String,
    code: PractitionerCode = PractitionerCode()
  ) :this(){
    this.identifier = identifier
    this.active = active
    this.organization = organization
    this.practitioner = practitioner
    this.code = code
  }

  @CsvBindByPosition(position = 0)
  var identifier: String? = null

  @CsvBindByPosition(position = 1)
  var active: Boolean = true

  @CsvBindByPosition(position = 2)
  var organization: String? = null

  @CsvBindByPosition(position = 3)
  var practitioner: String? = null

  var code: PractitionerCode = PractitionerCode()
}

@Serializable
data class PractitionerCode(
  val text: String = "Health Worker"
)
