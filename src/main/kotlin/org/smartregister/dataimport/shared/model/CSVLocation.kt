package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable

@Serializable
class CSVLocation() {

  constructor(parentId: String?, uniqueName: String?, id: String?, exactName: String?, tag: String?) : this() {
    this.parentId = parentId
    this.uniqueName = uniqueName
    this.id = id
    this.exactName = exactName
    this.tag = tag
  }

  @CsvBindByPosition(position = 0)
  var parentId: String? = null

  @CsvBindByPosition(position = 1)
  var uniqueName: String? = null

  @CsvBindByPosition(position = 2)
  var id: String? = null

  @CsvBindByPosition(position = 3)
  var exactName: String? = null

  @CsvBindByPosition(position = 4)
  var tag: String? = null

}
