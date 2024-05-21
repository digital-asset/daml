// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.v30

/** A force flag is used to override specific safety checks in the topology manager.
  */
sealed abstract class ForceFlag(val toProtoV30: v30.ForceFlag)

object ForceFlag {

  case object AlienMember extends ForceFlag(v30.ForceFlag.FORCE_FLAG_ALIEN_MEMBER)

  /** Required
    */
  case object LedgerTimeRecordTimeToleranceIncrease
      extends ForceFlag(v30.ForceFlag.FORCE_FLAG_LEDGER_TIME_RECORD_TIME_TOLERANCE_INCREASE)

  /** This should only be used internally in situations where
    * <ul>
    *   <li>the caller knows what they are doing</li>
    *  <li>it's not necessarily clear which specific flags to use, but there also isn't really any
    *   other choice, eg. when importing a topology snapshot.</li>
    * </ul>
    */
  private[topology] val all: Map[v30.ForceFlag, ForceFlag] =
    Seq[ForceFlag](AlienMember, LedgerTimeRecordTimeToleranceIncrease)
      .map(ff => ff.toProtoV30 -> ff)
      .toMap

  def fromProtoV30(flag: v30.ForceFlag): ParsingResult[ForceFlag] =
    all
      .get(flag)
      .toRight(
        ProtoDeserializationError.UnrecognizedField(s"Unrecognized force_change flag: $flag")
      )
}

/** A container for a set of force flags to pass around.
  */
final case class ForceFlags(private val flags: Set[ForceFlag]) {
  def and(flag: ForceFlag): ForceFlags = copy(flags + flag)
  def permits(flag: ForceFlag): Boolean = flags.contains(flag)

  def toProtoV30: Seq[v30.ForceFlag] = flags.map(_.toProtoV30).toSeq
}

object ForceFlags {
  def apply(flags: ForceFlag*): ForceFlags = ForceFlags(flags.toSet)
  val none: ForceFlags = ForceFlags()

  /** @see [[ForceFlag.all]] */
  private[topology] val all: ForceFlags = ForceFlags(ForceFlag.all.values.toSet)

  def fromProtoV30(flags: Seq[v30.ForceFlag]): ParsingResult[ForceFlags] =
    flags.traverse(ForceFlag.fromProtoV30).map(flags => ForceFlags(flags.toSet))
}
