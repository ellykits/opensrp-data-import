package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable

@Serializable
class OrganizationLocation() {

  constructor(organization: String, jurisdiction: String) : this() {
    this.organization = organization
    this.jurisdiction = jurisdiction
  }

  @CsvBindByPosition(position = 0)
  var organization: String? = null

  @CsvBindByPosition(position = 1)
  var jurisdiction: String? = null
}
