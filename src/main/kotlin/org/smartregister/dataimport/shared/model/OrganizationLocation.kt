package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable

@Serializable
data class OrganizationLocation(
  @CsvBindByPosition(position = 0)
  val organization: String,
  @CsvBindByPosition (position = 1)
  val jurisdiction: String
)
