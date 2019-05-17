package com.digitalasset.ledger.client.binding
package encoding

import com.digitalasset.ledger.api.v1.{value => rpcvalue}
import Primitive.{Update, ContractId}
import Template.CreateForExercise

import scala.annotation.implicitNotFound

@implicitNotFound(
  """Cannot decide how to exercise a choice on ${Self}; only well-typed contract IDs and Templates are candidates for choice exercise""")
sealed abstract class ExerciseOn[-Self, Tpl] {
  private[binding] def exercise[Out](
      self: Self,
      companion: TemplateCompanion[Tpl],
      choiceId: String,
      arguments: rpcvalue.Value): Update[Out]
}

object ExerciseOn {
  implicit def OnId[T]: ExerciseOn[ContractId[T], T] = new OnId
  implicit def CreateAndOnTemplate[T]: ExerciseOn[CreateForExercise[T], T] =
    new CreateAndOnTemplate

  private[this] final class OnId[T] extends ExerciseOn[ContractId[T], T] {
    private[binding] override def exercise[Out](
        self: ContractId[T],
        companion: TemplateCompanion[T],
        choiceId: String,
        arguments: rpcvalue.Value): Update[Out] =
      Primitive.exercise(companion, self, choiceId, arguments)
  }

  private[this] final class CreateAndOnTemplate[T] extends ExerciseOn[CreateForExercise[T], T] {
    private[binding] override def exercise[Out](
        self: CreateForExercise[T],
        companion: TemplateCompanion[T],
        choiceId: String,
        arguments: rpcvalue.Value): Update[Out] = sys.error("TODO SC CreateAndExercise")

  }
}
