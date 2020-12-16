// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import com.daml.lf.value.Value._
import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray}
import scalaz.NonEmptyList

import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.tailrec

final case class ValueVersion(protoValue: String)

/**
  * Currently supported versions of the DAML-LF value specification.
  */
object ValueVersion
    extends LfVersions(
      versionsAscending = NonEmptyList(new ValueVersion("6"), new ValueVersion("dev")))(
      _.protoValue,
    ) {

  private[lf] implicit val Ordering: Ordering[ValueVersion] = mkOrdering

  private[value] val minVersion = ValueVersion("6")
  private[value] val minGenMap = ValueVersion("dev")
  private[value] val minContractIdV1 = ValueVersion("dev")

  // Older versions are deprecated https://github.com/digital-asset/daml/issues/5220
  private[lf] val StableOutputVersions: VersionRange[ValueVersion] =
    VersionRange(ValueVersion("6"), ValueVersion("6"))

  private[lf] val DevOutputVersions: VersionRange[ValueVersion] =
    StableOutputVersions.copy(max = acceptedVersions.last)

  private[lf] def assignVersion[Cid](
      v0: Value[Cid],
      supportedVersions: VersionRange[ValueVersion] = StableOutputVersions,
  ): Either[String, ValueVersion] = {
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
              case ValueNumeric(_) =>
                go(currentVersion, values)
              case ValueOptional(x) =>
                go(currentVersion, ImmArray(x.toList) ++: values)
              case ValueTextMap(map) =>
                go(currentVersion, map.values ++: values)
              // for things added after version 6, we raise the minimum if present
              case ValueGenMap(entries) =>
                val newValues = entries.iterator.foldLeft(values) {
                  case (acc, (key, value)) => key +: value +: acc
                }
                go(currentVersion max minGenMap, newValues)
              case ValueEnum(_, _) =>
                go(currentVersion, values)
            }
        }
      }
    }

    go(supportedVersions.min, FrontStack(v0)) match {
      case Right(inferredVersion) if supportedVersions.max < inferredVersion =>
        Left(s"inferred version $inferredVersion is not supported")
      case res =>
        res
    }

  }

  @throws[IllegalArgumentException]
  private[lf] def assertAssignVersion[Cid](
      v0: Value[Cid],
      supportedVersions: VersionRange[ValueVersion] = DevOutputVersions,
  ): ValueVersion =
    data.assertRight(assignVersion(v0, supportedVersions))

  private[lf] def asVersionedValue[Cid](
      value: Value[Cid],
      supportedVersions: VersionRange[ValueVersion] = DevOutputVersions,
  ): Either[String, VersionedValue[Cid]] =
    assignVersion(value, supportedVersions).map(VersionedValue(_, value))

  @throws[IllegalArgumentException]
  private[lf] def assertAsVersionedValue[Cid](
      value: Value[Cid],
      supportedVersions: VersionRange[ValueVersion] = DevOutputVersions,
  ): VersionedValue[Cid] =
    data.assertRight(asVersionedValue(value, supportedVersions))
}
