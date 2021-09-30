// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlState.DamlStateValue

/** This typeclass signifies that implementor contains an optional Daml state value.
  *
  * Used by the [[com.daml.ledger.validator.preexecution.PreExecutingSubmissionValidator]].
  */
trait HasDamlStateValue[T] {
  def damlStateValue(value: T): Option[DamlStateValue]
}

object HasDamlStateValue {
  def unapply[T](
      value: T
  )(implicit hasDamlStateValue: HasDamlStateValue[T]): Option[DamlStateValue] =
    hasDamlStateValue.damlStateValue(value)

  implicit object `DamlStateValue has itself` extends HasDamlStateValue[DamlStateValue] {
    override def damlStateValue(value: DamlStateValue): Option[DamlStateValue] =
      Some(value)
  }

  implicit def `Option[T] has DamlStateValue if T has DamlStateValue`[T](implicit
      hasDamlStateValue: HasDamlStateValue[T]
  ): HasDamlStateValue[Option[T]] =
    (value: Option[T]) => value.flatMap(hasDamlStateValue.damlStateValue)
}
