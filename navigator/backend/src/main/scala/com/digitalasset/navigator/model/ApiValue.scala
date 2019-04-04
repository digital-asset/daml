// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model

import java.time.{Instant, LocalDate}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.concurrent.TimeUnit

sealed trait ApiValue

final case class ApiRecordField(label: String, value: ApiValue)

final case class ApiRecord(recordId: Option[DamlLfIdentifier], fields: List[ApiRecordField])
    extends ApiValue {

  /**
    * Fills in missing field names.
    * This function is NOT recursive, i.e., it will not fill in field names in other records that appear
    * within this record.
    */
  /*def withFieldNames(types: DamlLfTypeLookup): ApiRecord = recordId.flatMap(id => types(id)).map(ddt => {
    val ddtFields: List[DamlLfFieldWithType] = ddt.dataType.fold(_.fields, _.fields).toList
    val newFields = ddtFields
      .zip(fields)
      .map(p => ApiRecordField(p._1._1, p._2.value))
    ApiRecord(recordId, newFields)
  }).getOrElse(this)
 */
}

final case class ApiVariant(
    variantId: Option[DamlLfIdentifier],
    constructor: String,
    value: ApiValue)
    extends ApiValue
final case class ApiList(elements: List[ApiValue]) extends ApiValue
final case class ApiOptional(value: Option[ApiValue]) extends ApiValue
final case class ApiMap(value: Map[String, ApiValue]) extends ApiValue
final case class ApiContractId(value: String) extends ApiValue
final case class ApiInt64(value: Long) extends ApiValue
final case class ApiDecimal(value: String) extends ApiValue
final case class ApiText(value: String) extends ApiValue
final case class ApiParty(value: String) extends ApiValue
final case class ApiBool(value: Boolean) extends ApiValue
final case class ApiUnit() extends ApiValue
final case class ApiTimestamp(value: Long) extends ApiValue {
  def toInstant: Instant =
    Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(value), value % 1000000L * 1000L)
  def toIso8601: String = DateTimeFormatter.ISO_INSTANT.format(toInstant)
}
final case class ApiDate(value: Int) extends ApiValue {
  def toLocalDate: LocalDate = LocalDate.ofEpochDay(value.toLong)
  def toInstant: Instant = Instant.from(toLocalDate)
  def toIso8601: String = DateTimeFormatter.ISO_LOCAL_DATE.format(toLocalDate)
}

case object ApiTimestamp {
  // Timestamp has microsecond resolution
  private val formatter: DateTimeFormatter =
    new DateTimeFormatterBuilder().appendInstant(6).toFormatter()

  def fromIso8601(t: String): ApiTimestamp = fromInstant(Instant.parse(t))
  def fromInstant(t: Instant): ApiTimestamp =
    ApiTimestamp(t.getEpochSecond * 1000000L + t.getNano / 1000L)
  def fromMillis(t: Long): ApiTimestamp = ApiTimestamp(t * 1000L)
}

case object ApiDate {
  def fromIso8601(t: String): ApiDate =
    fromLocalDate(LocalDate.parse(t, DateTimeFormatter.ISO_LOCAL_DATE))
  def fromLocalDate(t: LocalDate): ApiDate = ApiDate(t.toEpochDay.toInt)
}
