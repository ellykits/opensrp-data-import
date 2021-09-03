package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient

@Serializable
class OpenMRSUser {
  @CsvBindByPosition(position = 0)
  var username: String = ""
  @Transient
  @CsvBindByPosition(position = 1)
  var password: String = "Test1234"
}
