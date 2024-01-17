// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.RollbackContext.{RollbackScope, RollbackSibling, firstChild}
import com.digitalasset.canton.protocol.v3
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

import scala.Ordering.Implicits.*
import scala.math.Ordered.orderingToOrdered

/** RollbackContext tracks the location of lf transaction nodes or canton participant views within a hierarchy of
  * LfNodeRollback suitable for maintaining the local position within the hierarchy of rollback nodes when iterating
  * over a transaction.
  * @param rbScope   scope or path of sibling ordinals ordered "from the outside-in", e.g. [2, 1, 3] points via the
  *                  second rollback node to its first rollback node descendant to the latter's third rollback node
  *                  descendant where rollback node "levels" may be separated from the root and each other only by
  *                  non-rollback nodes.
  * @param nextChild the next sibling ordinal to assign to a newly encountered rollback node. This is needed on top of
  *                  rbScope in use cases in which the overall transaction structure is not known a priori.
  */
final case class RollbackContext private (
    private val rbScope: Vector[RollbackSibling],
    private val nextChild: RollbackSibling = firstChild,
) extends PrettyPrinting
    with Ordered[RollbackContext] {

  def enterRollback: RollbackContext = new RollbackContext(rbScope :+ nextChild)

  def exitRollback: RollbackContext = {
    val lastChild =
      rbScope.lastOption.getOrElse(
        throw new IllegalStateException("Attempt to exit rollback on empty rollback context")
      )

    new RollbackContext(rbScope.dropRight(1), lastChild.increment)
  }

  def rollbackScope: RollbackScope = rbScope

  def inRollback: Boolean = rbScope.nonEmpty

  def isEmpty: Boolean = equals(RollbackContext.empty)

  def toProtoV3: v3.ViewParticipantData.RollbackContext =
    v3.ViewParticipantData.RollbackContext(
      rollbackScope = rollbackScope.map(_.unwrap),
      nextChild = nextChild.unwrap,
    )

  override def pretty: Pretty[RollbackContext] = prettyOfClass(
    param("rollback scope", _.rollbackScope),
    param("next child", _.nextChild),
  )

  private lazy val sortKey: Vector[PositiveInt] = rbScope :+ nextChild
  override def compare(that: RollbackContext): Int = sortKey.compare(that.sortKey)

}

final case class WithRollbackScope[T](rbScope: RollbackScope, unwrap: T)

object RollbackContext {
  type RollbackSibling = PositiveInt
  private val firstChild: RollbackSibling = PositiveInt.one

  type RollbackScope = Seq[RollbackSibling]

  object RollbackScope {
    def empty: RollbackScope = Vector.empty[RollbackSibling]

    def popsAndPushes(origin: RollbackScope, target: RollbackScope): (Int, Int) = {
      val longestCommonRollbackScopePrefixLength =
        origin.lazyZip(target).takeWhile { case (i, j) => i == j }.size

      val rbPops = origin.length - longestCommonRollbackScopePrefixLength
      val rbPushes = target.length - longestCommonRollbackScopePrefixLength
      (rbPops, rbPushes)
    }
  }

  def empty: RollbackContext = new RollbackContext(Vector.empty)

  def apply(scope: RollbackScope): RollbackContext = new RollbackContext(scope.toVector)

  def fromProtoV0(
      maybeRbContext: Option[v3.ViewParticipantData.RollbackContext]
  ): ParsingResult[RollbackContext] =
    maybeRbContext.fold(Either.right[ProtoDeserializationError, RollbackContext](empty)) {
      case v3.ViewParticipantData.RollbackContext(rbScope, nextChildP) =>
        import cats.syntax.traverse.*

        for {
          nextChild <- PositiveInt
            .create(nextChildP)
            .leftMap(_ =>
              ProtoDeserializationError.ValueConversionError(
                "next_child",
                s"positive value expected; found: $nextChildP",
              )
            )

          rbScopeVector <- rbScope.toVector.zipWithIndex
            .traverse { case (value, idx) =>
              PositiveInt.create(value).leftMap { _ =>
                s"positive value expected; found $value at position $idx"
              }
            }
            .leftMap(
              ProtoDeserializationError.ValueConversionError("rollback_scope", _)
            )

        } yield new RollbackContext(rbScopeVector, nextChild)
    }

}
