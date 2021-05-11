package org.smartregister.dataimport.shared.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient

@Serializable
data class Location(
  val id: String,
  val type: String = "Feature",
  val properties: LocationProperties,
  val locationTags: List<LocationTag>? = null,
  @Transient
  var isNew: Boolean = false
)

@Serializable
data class LocationProperties (
  val status: String = "Active",
  var parentId: String = "",
  val name: String,
  val geographicalLevel: Int = 0,
  val version: Int = 0
)

@Serializable
data class LocationTag(
  val id: Int,
  val active: Boolean = true,
  val name: String,
  val description: String = "$name Location Tag"
)