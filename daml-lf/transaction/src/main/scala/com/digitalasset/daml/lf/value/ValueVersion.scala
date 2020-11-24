// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import com.daml.lf.value.Value._
import com.daml.lf.data.{Decimal, FrontStack, FrontStackCons, ImmArray}
import com.daml.lf.transaction.VersionTimeline

import scala.annotation.tailrec

final case class ValueVersion(protoValue: String)

/**
  * Currently supported versions of the DAML-LF value specification.
  */
private[lf] object ValueVersions
    extends LfVersions(versionsAscending = VersionTimeline.ascendingVersions[ValueVersion])(
      _.protoValue,
    ) {

  private[lf] val minVersion = ValueVersion("1")
  private[lf] val minOptional = ValueVersion("2")
  private[lf] val minContractIdStruct = ValueVersion("3")
  private[lf] val minMap = ValueVersion("4")
  private[lf] val minEnum = ValueVersion("5")
  private[lf] val minNumeric = ValueVersion("6")
  private[lf] val minGenMap = ValueVersion("dev")
  private[lf] val minNoTypeConstructor = ValueVersion("dev")
  private[lf] val minNoRecordLabel = ValueVersion("dev")
  private[lf] val minContractIdV1 = ValueVersion("dev")

  // Older versions are deprecated https://github.com/digital-asset/daml/issues/5220
  val StableOutputVersions: VersionRange[ValueVersion] =
    VersionRange(ValueVersion("6"), ValueVersion("6"))

  val DevOutputVersions: VersionRange[ValueVersion] =
    StableOutputVersions.copy(max = acceptedVersions.last)

  // Empty range
  val Empty: VersionRange[ValueVersion] =
    VersionRange(acceptedVersions.last, acceptedVersions.head)

  def assignVersion[Cid](
      v0: Value[Cid],
      supportedVersions: VersionRange[ValueVersion] = StableOutputVersions,
  ): Either[String, ValueVersion] = {
    import VersionTimeline.{maxVersion => maxVV}
    import VersionTimeline.Implicits._

    @tailrec
    def go(
        currentVersion: ValueVersion,
        values0: FrontStack[Value[Cid]],
    ): Either[String, ValueVersion] = {
      if (currentVersion == maxVersion) {
        Right(currentVersion)
      } else {
        values0 match {
          case FrontStack() => Right(currentVersion)
          case FrontStackCons(value, values) =>
            value match {
              // for things supported since version 1, we do not need to check
              case ValueRecord(_, fs) => go(currentVersion, fs.map(v => v._2) ++: values)
              case ValueVariant(_, _, arg) => go(currentVersion, arg +: values)
              case ValueList(vs) => go(currentVersion, vs.toImmArray ++: values)
              case ValueContractId(_) | ValueInt64(_) | ValueText(_) | ValueTimestamp(_) |
                  ValueParty(_) | ValueBool(_) | ValueDate(_) | ValueUnit =>
                go(currentVersion, values)
              case ValueNumeric(x) if x.scale == Decimal.scale =>
                go(currentVersion, values)
              // for things added after version 1, we raise the minimum if present
              case ValueNumeric(_) =>
                go(maxVV(minNumeric, currentVersion), values)
              case ValueOptional(x) =>
                go(maxVV(minOptional, currentVersion), ImmArray(x.toList) ++: values)
              case ValueTextMap(map) =>
                go(maxVV(minMap, currentVersion), map.values ++: values)
              case ValueGenMap(entries) =>
                val newValues = entries.iterator.foldLeft(values) {
                  case (acc, (key, value)) => key +: value +: acc
                }
                go(maxVV(minGenMap, currentVersion), newValues)
              case ValueEnum(_, _) =>
                go(maxVV(minEnum, currentVersion), values)
            }
        }
      }
    }

    go(supportedVersions.min, FrontStack(v0)) match {
      case Right(inferredVersion) if supportedVersions.max precedes inferredVersion =>
        Left(s"inferred version $inferredVersion is not supported")
      case res =>
        res
    }

  }

  @throws[IllegalArgumentException]
  def assertAssignVersion[Cid](
      v0: Value[Cid],
      supportedVersions: VersionRange[ValueVersion] = DevOutputVersions,
  ): ValueVersion =
    data.assertRight(assignVersion(v0, supportedVersions))

  def asVersionedValue[Cid](
      value: Value[Cid],
      supportedVersions: VersionRange[ValueVersion] = DevOutputVersions,
  ): Either[String, VersionedValue[Cid]] =
    assignVersion(value, supportedVersions).map(VersionedValue(_, value))

  @throws[IllegalArgumentException]
  def assertAsVersionedValue[Cid](
      value: Value[Cid],
      supportedVersions: VersionRange[ValueVersion] = DevOutputVersions,
  ): VersionedValue[Cid] =
    data.assertRight(asVersionedValue(value, supportedVersions))
}
