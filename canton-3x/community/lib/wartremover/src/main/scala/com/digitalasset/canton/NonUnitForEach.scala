// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.syntax.functorFilter.*
import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.tailrec
import scala.collection.IterableOnceOps
import scala.reflect.internal.Precedence

/** This wart warns when methods [[scala.collection.IterableOnceOps.foreach]] and
  * [[scala.collection.IterableOnceOps.tapEach]] are used with non-unit valued functions.
  * This is needed because these methods' signature accepts any-valued functions.
  * So `-Ywarn-value-discard` does not warn about such discarded values.
  *
  * [[scala.collection.IterableOnceOps.foreach]] is also used in `for` loops without yield
  * and therefore also covers cases such as `for { x <- 1 to 10 } { x + 5 }`.
  *
  * Builder operations such as `b += ...` are identified by their return type `this.type`
  * and ignored when checking for non-unit return values. This allows common usages such
  * as `for { x <- 1 to 10 } { b += x }` and
  * `for { x <- 1 to 10 } { if (x % 2 == 0) b += x }`.
  * This does not work for all usages though; you can force a `Unit` type by
  * specifying the type argument of [[scala.collection.IterableOnceOps.foreach]] and
  * [[scala.collection.IterableOnceOps.tapEach]] as in `(1 to 10).foreach[Unit] { ... }`,
  * or by ascribing `Unit` as in `(1 to 10).foreach { x => f(x): Unit }`,
  * or by explicitly discarding the result: `(1 to 10).foreach { x => f(x).discard }`.
  */
object NonUnitForEach extends WartTraverser {
  val messageUnappliedForeach = " used with non-unit-valued function of type "
  val messageAppliedForeach = " discards non-unit value of type "

  override def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val iterableOnceOpsTypeSymbol =
      typeOf[IterableOnceOps[Unit, Iterable, Unit]].typeConstructor.typeSymbol
    require(iterableOnceOpsTypeSymbol != NoSymbol)
    val foreachMethodName: TermName = TermName("foreach")
    val tapEachMethodName: TermName = TermName("tapEach")

    val unitType = typeOf[Unit]
    require(unitType.typeSymbol != NoSymbol)

    new u.Traverser {

      def isSubtypeOfIterableOnce(typ: Type): Boolean =
        typ.typeConstructor
          .baseType(iterableOnceOpsTypeSymbol)
          .typeConstructor
          .typeSymbol == iterableOnceOpsTypeSymbol

      def uninterestingType(typ: Type): Boolean =
        typ <:< unitType

      @tailrec def stripApplies(t: Tree, args: List[Tree]): (Tree, List[Tree]) =
        t match {
          case TypeApply(x, _) => stripApplies(x, args)
          case Apply(x, xArgs) => stripApplies(x, xArgs ++ args)
          case _ => (t, args)
        }

      /* Is tree an application with result `this.type`?
       * Accept `b.addOne(x)` and also `xs(i) += x`
       * where the op is an assignment operator.
       *
       * This is similar to scala.reflect.internal.Treeinfo.isThisTypeResult.
       * Differences:
       * - Does not work for receivers that are var
       *   (there does not seem to be an easy way to get this information without using internal APIs)
       */
      def isThisTypeResult(tree: Tree): Boolean = {
        stripApplies(tree, Nil) match {
          case (fun @ Select(receiver, op), args) =>
            tree.tpe match {
              case ThisType(sym) =>
                sym == receiver.symbol
              case SingleType(p, sym) =>
                sym == receiver.symbol || args.exists(sym == _.symbol)
              case _ =>
                def checkSingle(sym: Symbol): Boolean =
                  (sym == receiver.symbol) || {
                    receiver match {
                      case Apply(_, _) =>
                        Precedence(op.decodedName.toString).level == 0 // xs(i) += x
                      case _ =>
                        // We would like to allow receivers that are fields and vars.
                        false
                    }
                  }

                @tailrec def loop(mt: Type): Boolean = mt match {
                  case MethodType(_, restpe) =>
                    restpe match {
                      case ThisType(sym) => checkSingle(sym)
                      case SingleType(_, sym) => checkSingle(sym)
                      case _ => loop(restpe)
                    }
                  case PolyType(_, restpe) => loop(restpe)
                  case _ => false
                }

                fun.symbol != null && loop(fun.symbol.info)
            }
          case _ =>
            tree.tpe match {
              case ThisType(_) => true
              case _ => false
            }
        }
      }

      def isInterestingResult(tree: Tree): Option[Tree] = {
        tree match {
          case Function(_, body) => isInterestingResult(body)
          case Block(_, res) => isInterestingResult(res)
          // Look into each branch and warn only if the return type is interesting, i.e., neither Unit nor a builder.
          // Scalac infers Any for the whole if/match statement if some of the branches use builders and others don't,
          // which we would flag as a discarded value.
          case If(_, thenPart, elsePart) =>
            isInterestingResult(thenPart).orElse(isInterestingResult(elsePart))
          case Match(_, cases) =>
            cases.mapFilter {
              case CaseDef(_pat, _guard, body) => isInterestingResult(body)
              case _ => None
            }.headOption
          case _ =>
            Option.when(!uninterestingType(tree.tpe) && !isThisTypeResult(tree))(tree)
        }
      }

      override def traverse(tree: Tree): Unit = {
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>

          case Apply(TypeApply(Select(receiver, method), tyArgs), args)
              if (method == foreachMethodName || method == tapEachMethodName) &&
                receiver.tpe != null && isSubtypeOfIterableOnce(receiver.tpe) &&
                tyArgs.nonEmpty && !uninterestingType(tyArgs(0).tpe) =>
            if (args.isEmpty) {
              error(u)(tree.pos, method.toString + messageUnappliedForeach + tyArgs(0).tpe)
            } else
              isInterestingResult(args(0)) match {
                case None => super.traverse(tree)
                case Some(where) =>
                  error(u)(where.pos, method.toString + messageAppliedForeach + where.tpe)
              }

          case _ => super.traverse(tree)
        }
      }
    }
  }
}
