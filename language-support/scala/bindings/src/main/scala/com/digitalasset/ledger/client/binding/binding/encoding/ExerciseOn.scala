// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding
package encoding

import Primitive.ContractId
import Template.CreateForExercise

import scala.annotation.implicitNotFound

@implicitNotFound(
  """Cannot decide how to exercise a choice on ${Self}; only well-typed contract IDs and Templates (.createAnd) are candidates for choice exercise""")
sealed abstract class ExerciseOn[-Self, Tpl]

object ExerciseOn {
  implicit def OnId[T]: ExerciseOn[ContractId[T], T] = new OnId
  implicit def CreateAndOnTemplate[T]: ExerciseOn[CreateForExercise[T], T] =
    new CreateAndOnTemplate

  private[binding] final class OnId[T] extends ExerciseOn[ContractId[T], T]
  private[binding] final class CreateAndOnTemplate[T] extends ExerciseOn[CreateForExercise[T], T]
}
