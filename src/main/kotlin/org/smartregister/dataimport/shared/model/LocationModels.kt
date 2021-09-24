package org.smartregister.dataimport.shared.model

import com.opencsv.bean.CsvBindByPosition
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient

@Serializable
class Location() {
  constructor(
    id: String,
    type: String = "Feature",
    properties: LocationProperties,
    locationTags: List<LocationTag>? = null,
    isNew: Boolean = false,
    assignTeam: Boolean = false,
    uniqueName: String? = null,
    uniqueParentName: String? = null,
    firstLocationTag: String? = null
  ) : this() {
    this.id = id
    this.type = type
    this.properties = properties
    this.locationTags = locationTags
    this.isNew = isNew
    this.assignTeam = assignTeam
    this.uniqueName = uniqueName
    this.uniqueParentName = uniqueParentName
    this.firstLocationTag = firstLocationTag
  }

  var type: String? = null

  var properties: LocationProperties? = null

  var locationTags: List<LocationTag>? = null

  @Transient
  @CsvBindByPosition(position = 0)
  var parentId: String? = null

  @Transient
  @CsvBindByPosition(position = 1)
  var uniqueName: String? = null

  @CsvBindByPosition(position = 2)
  var id: String? = null

  @Transient
  @CsvBindByPosition(position = 3)
  var exactName: String? = null

  @Transient
  @CsvBindByPosition(position = 4)
  var firstLocationTag: String? = null

  @Transient
  var isNew: Boolean = true

  @Transient
  var assignTeam: Boolean = false

  @Transient
  var uniqueParentName: String? = null

  var serverVersion: Long? = null

}

@Serializable
data class LocationProperties(
  val status: String = "Active",
  var parentId: String = "",
  val name: String,
  val geographicLevel: Int? = 0,
  val version: Int = 0
)

@Serializable
data class LocationTag(
  val id: Int,
  val active: Boolean = true,
  val name: String,
  val description: String = "$name Location Tag"
)
