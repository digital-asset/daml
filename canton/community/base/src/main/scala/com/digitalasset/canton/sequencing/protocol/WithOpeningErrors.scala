// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.util.{SingletonTraverse, WithGeneric, WithGenericCompanion}

final case class WithOpeningErrors[+Event](
    event: Event,
    openingErrors: Seq[ProtoDeserializationError],
) extends WithGeneric[Event, Seq[ProtoDeserializationError], WithOpeningErrors] {
  override protected def value: Event = event
  override protected def added: Seq[ProtoDeserializationError] = openingErrors
  override protected def update[AA](newValue: AA): WithOpeningErrors[AA] =
    this.copy(event = newValue)

  def hasNoErrors: Boolean = openingErrors.isEmpty
}

object WithOpeningErrors extends WithGenericCompanion {
  implicit val singletonTraverseEventWithErrors
      : SingletonTraverse.Aux[WithOpeningErrors, Seq[ProtoDeserializationError]] =
    singletonTraverseWithGeneric[Seq[ProtoDeserializationError], WithOpeningErrors]
}

object NoOpeningErrors {
  def apply[Event](event: Event): WithOpeningErrors[Event] =
    WithOpeningErrors(event, Seq.empty)
}
