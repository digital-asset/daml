// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.mockito.Mockito
import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.tailrec
import scala.collection.IterableOnceOps
import scala.quoted.{Expr, Type}

/** Flags statements that return a [[scala.concurrent.Future]]. Typically, we should not discard
  * [[scala.concurrent.Future]] because exceptions inside the future may not get logged. Use
  * `FutureUtil.doNotAwait` to log exceptions and discard the future where necessary.
  *
  * Also detects discarded [[cats.data.EitherT]]`[`[[scala.concurrent.Future]]`, ..., ...]` and
  * [[cats.data.OptionT]]`[`[[scala.concurrent.Future]]`, ...]` and arbitrary nestings of those.
  * Custom type constructors can be registered to take the same role as [[scala.concurrent.Future]]
  * by annotating the type definition with [[DoNotDiscardLikeFuture]].
  *
  * Also flags usages of [[scala.collection.IterableOnceOps.foreach]] and
  * [[scala.collection.IterableOnceOps.tapEach]] where the function returns a future-like type.
  *
  * This wart is a special case of the warts `NonUnitStatements` and [[NonUnitForEach]] and scalac's
  * `-Wnonunit-statement` flag, in that it warns only if the return type is future-like.
  * Additionally, this wart uses a different set of exceptions when no warning is issued. We keep
  * this specialized wart for two reasons:
  *   1. It is not practically feasible to use `-Wnonunit-statement` and [[NonUnitForEach]] in
  *      scalatest because it would flag many of the assertions of the form `x should be >= y` in
  *      statement positions. Yet, it is important to check for discarded futures in tests because a
  *      discarded future may hide an exception.
  *   1. In some production code, it is convenient to suppress the warnings coming from
  *      `-Wnonunit-statement` and [[NonUnitForEach]], just due to how the code is written. In such
  *      places, we still want to benefit from the explicit checks against discarded futures.
  *
  * This wart does not look at futures that are discarded at the end of a unit-type expression.
  * These cases are caught by `-Ywarn-value-discard`. We do not implement a specialized version for
  * future-like values because we do not expect to suppress the warnings coming from
  * `-Ywarn-value-discard`.
  */
object DiscardedFuture extends WartTraverser {
  val message = "Statements must not discard a Future"

  override def apply(u: WartUniverse): u.Traverser =
    // val verifyMethodName: TermName = TermName("verify")

    new u.Traverser(this) {
      import q.reflect.*

      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else
          tree match
            case tree: Block =>
              checkForDiscardedFutures(tree.statements)
            case tree: ClassDef =>
              checkForDiscardedFutures(tree.body)
            case tree if tree.isExpr =>
              tree.asExpr match
                case '{ ($_ : IterableOnceOps[a, cc, c]).foreach[o]($_) } =>
                  if isFutureLike[o] then error(tree.pos, message)
                case '{ ($_ : IterableOnceOps[a, cc, c]).tapEach[o]($_) } =>
                  if isFutureLike[o] then error(tree.pos, message)
                case _ => ()
            case _ => ()
          super.traverseTree(tree)(owner)

      private def checkForDiscardedFutures(statements: List[Statement]): Unit =
        statements.filter(_.isExpr).foreach { statement =>
          statement.asExpr match
            case '{ $expr: t } if isFutureLike[t] && !isMockitoVerify(statement) =>
              error(statement.pos, message)
            case _ => ()
        }

      // Allow Mockito `verify` calls because they do not produce a future but merely check that a mocked Future-returning
      // method has been called.
      @tailrec
      private def isMockitoVerify(tree: Tree): Boolean =
        tree match {
          // Match on verify(...).someMethod
          case Select(tree, _) =>
            tree.asExpr match
              case '{ Mockito.verify($_ : Any) } => true
              case _ => false
          // Strip away any further argument lists on the method as in verify(...).someMethod(...)(...)(...
          // including implicit arguments and type arguments
          case Apply(tree, _) => isMockitoVerify(tree)
          case TypeApply(tree, _) => isMockitoVerify(tree)
          case _ => false
        }

      private val futureLikeTester = FutureLikeTester.tester[DoNotDiscardLikeFuture](u)
      private def isFutureLike[T: Type] = futureLikeTester(TypeRepr.of[T])
    }

}
