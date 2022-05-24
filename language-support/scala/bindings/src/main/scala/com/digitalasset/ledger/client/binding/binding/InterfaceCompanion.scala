// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.v1.{value => rpcvalue}
import encoding.ExerciseOn

import scala.annotation.nowarn

/** Common superclass of interface companion objects. */
abstract class InterfaceCompanion[T] extends ContractTypeCompanion[T] {
  @nowarn("msg=parameter value actor .* is never used") // part of generated code API
  protected override final def ` exercise`[ExOn, Out](
      actor: Primitive.Party,
      receiver: ExOn,
      choiceId: String,
      arguments: Option[rpcvalue.Value],
  )(implicit exon: ExerciseOn[ExOn, T]): Primitive.Update[Out] = {
    sys.error("TODO #13924 new exercise command, factor with TemplateCompanion")
  }
}
