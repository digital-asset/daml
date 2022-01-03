// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding
package encoding

import Primitive.ContractId
import Template.CreateForExercise

import scala.annotation.implicitNotFound

@implicitNotFound(
  """Cannot decide how to exercise a choice on ${Self}; only well-typed contract IDs, Templates (.createAnd), and keys (TemplateType key k) are candidates for choice exercise"""
)
sealed abstract class ExerciseOn[-Self, Tpl]

object ExerciseOn {
  implicit def OnId[T]: ExerciseOn[ContractId[T], T] = new OnId
  implicit def CreateAndOnTemplate[T]: ExerciseOn[CreateForExercise[T], T] =
    new CreateAndOnTemplate
  implicit def OnKey[T]: ExerciseOn[Template.Key[T], T] = new OnKey

  private[binding] final class OnId[T] extends ExerciseOn[ContractId[T], T]
  private[binding] final class CreateAndOnTemplate[T] extends ExerciseOn[CreateForExercise[T], T]
  private[binding] final class OnKey[T] extends ExerciseOn[Template.Key[T], T]
}
