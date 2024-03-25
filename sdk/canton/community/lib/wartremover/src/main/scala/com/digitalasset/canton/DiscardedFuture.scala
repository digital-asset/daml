// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.{StaticAnnotation, tailrec}
import scala.collection.IterableOnceOps

/** Flags statements that return a [[scala.concurrent.Future]]. Typically, we should not
  * discard [[scala.concurrent.Future]] because exceptions inside the future may not get logged.
  * Use `FutureUtil.doNotAwait` to log exceptions and discard the future where necessary.
  *
  * Also detects discarded [[cats.data.EitherT]]`[`[[scala.concurrent.Future]]`, ..., ...]`
  * and [[cats.data.OptionT]]`[`[[scala.concurrent.Future]]`, ...]` and arbitrary nestings of those.
  * Custom type constructors can be registered to take the same role as [[scala.concurrent.Future]]
  * by annotating the type definition with [[DoNotDiscardLikeFuture]].
  *
  * Also flags usages of [[scala.collection.IterableOnceOps.foreach]] and [[scala.collection.IterableOnceOps.tapEach]]
  * where the function returns a future-like type.
  *
  * This wart is a special case of the warts `NonUnitStatements` and [[NonUnitForEach]]
  * and scalac's `-Wnonunit-statement` flag,
  * in that it warns only if the return type is future-like.
  * Additionally, this wart uses a different set of exceptions when no warning is issued.
  * We keep this specialized wart for two reasons:
  * 1. It is not practically feasible to use `-Wnonunit-statement` and [[NonUnitForEach]] in scalatest
  *    because it would flag many of the assertions of the form `x should be >= y` in statement positions.
  *    Yet, it is important to check for discarded futures in tests because a discarded future may hide an exception.
  * 2. In some production code, it is convenient to suppress the warnings coming from `-Wnonunit-statement` and [[NonUnitForEach]],
  *    just due to how the code is written. In such places, we still want to benefit from the explicit checks
  *    against discarded futures.
  *
  * This wart does not look at futures that are discarded at the end of a unit-type expression.
  * These cases are caught by `-Ywarn-value-discard`. We do not implement a specialized version
  * for future-like values because we do not expect to suppress the warnings coming from `-Ywarn-value-discard`.
  */
object DiscardedFuture extends WartTraverser {
  val message = "Statements must not discard a Future"

  override def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val verifyMethodName: TermName = TermName("verify")
    val futureLikeType = typeOf[DoNotDiscardLikeFuture]
    val isFutureLike = FutureLikeTester.tester(u)(futureLikeType)

    val iterableOnceOpsTypeSymbol =
      typeOf[IterableOnceOps[Unit, Iterable, Unit]].typeConstructor.typeSymbol
    require(iterableOnceOpsTypeSymbol != NoSymbol)
    val foreachMethodName: TermName = TermName("foreach")
    val tapEachMethodName: TermName = TermName("tapEach")

    // Allow Mockito `verify` calls because they do not produce a future but merely check that a mocked Future-returning
    // method has been called.
    //
    // We do not check whether the receiver of the `verify` call is actually something that inherits from org.mockito.MockitoSugar
    // because `BaseTest` extends `MockitoSugar` and we'd therefore have to do some virtual method resolution.
    // As a result, we ignore all statements of the above form verify(...).someMethod(...)(...)
    @tailrec
    def isMockitoVerify(statement: Tree): Boolean = {
      statement match {
        // Match on verify(...).someMethod
        case Select(
              Apply(TypeApply(Select(_receiver, verifyMethod), _tyargs), _verifyArgs),
              _method,
            ) if verifyMethod == verifyMethodName =>
          true
        // Strip away any further argument lists on the method as in verify(...).someMethod(...)(...)(...
        // including implicit arguments and type arguments
        case Apply(maybeVerifyCall, args) => isMockitoVerify(maybeVerifyCall)
        case TypeApply(maybeVerifyCall, tyargs) => isMockitoVerify(maybeVerifyCall)
        case _ => false
      }
    }

    new u.Traverser {
      def checkForDiscardedFutures(statements: List[Tree]): Unit = {
        statements.foreach {
          case Block((statements0, _)) =>
            checkForDiscardedFutures(statements0)
          case statement =>
            val typeIsFuture = statement.tpe != null && isFutureLike(statement.tpe.dealias)
            if (typeIsFuture && !isMockitoVerify(statement)) {
              error(u)(statement.pos, message)
            }
        }
      }

      def isSubtypeOfIterableOnce(typ: Type): Boolean =
        typ.typeConstructor
          .baseType(iterableOnceOpsTypeSymbol)
          .typeConstructor
          .typeSymbol == iterableOnceOpsTypeSymbol

      override def traverse(tree: Tree): Unit = {
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>

          case Block(statements, _) =>
            checkForDiscardedFutures(statements)
            super.traverse(tree)
          case ClassDef(_, _, _, Template((_, _, statements))) =>
            checkForDiscardedFutures(statements)
            super.traverse(tree)
          case ModuleDef(_, _, Template((_, _, statements))) =>
            checkForDiscardedFutures(statements)
            super.traverse(tree)
          case TypeApply(Select(receiver, method), tyArgs)
              if (method == foreachMethodName || method == tapEachMethodName) &&
                receiver.tpe != null && isSubtypeOfIterableOnce(receiver.tpe) &&
                tyArgs.nonEmpty && isFutureLike(tyArgs(0).tpe) =>
            error(u)(tree.pos, message)

          case _ => super.traverse(tree)
        }
      }
    }
  }
}

/** Annotated type constructors will be treated like a [[scala.concurrent.Future]]
  * when looking for discarded futures.
  */
final class DoNotDiscardLikeFuture extends StaticAnnotation
