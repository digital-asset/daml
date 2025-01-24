// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

/** Represents the synchronizer protocol version for the deserialization validation such that
  * cases where no protocol version is defined can be clearly expressed with
  * [[ProtocolVersionValidation.NoValidation]].
  */
sealed trait ProtocolVersionValidation extends Product with Serializable

object ProtocolVersionValidation {

  final case class PV(pv: ProtocolVersion) extends ProtocolVersionValidation
  final case object NoValidation extends ProtocolVersionValidation

  def apply(pv: ProtocolVersion): ProtocolVersionValidation = ProtocolVersionValidation.PV(pv)

  def when(cond: Boolean)(pv: ProtocolVersion): ProtocolVersionValidation =
    if (cond) ProtocolVersionValidation(pv) else ProtocolVersionValidation.NoValidation

  def unless(cond: Boolean)(pv: ProtocolVersion): ProtocolVersionValidation =
    when(!cond)(pv)

}
