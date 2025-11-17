// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.tailrec
import scala.collection.IterableOnceOps
import scala.quoted.{Expr, Type}

/** This wart warns when methods [[scala.collection.IterableOnceOps.foreach]] and
  * [[scala.collection.IterableOnceOps.tapEach]] are used with non-unit valued functions. This is
  * needed because these methods' signature accepts any-valued functions. So `-Ywarn-value-discard`
  * does not warn about such discarded values.
  *
  * [[scala.collection.IterableOnceOps.foreach]] is also used in `for` loops without yield and
  * therefore also covers cases such as `for { x <- 1 to 10 } { x + 5 }`.
  *
  * Builder operations such as `b += ...` are identified by their return type `this.type` and
  * ignored when checking for non-unit return values. This allows common usages such as `for { x <-
  * 1 to 10 } { b += x }` and `for { x <- 1 to 10 } { if (x % 2 == 0) b += x }`. This does not work
  * for all usages though; you can force a `Unit` type by specifying the type argument of
  * [[scala.collection.IterableOnceOps.foreach]] and [[scala.collection.IterableOnceOps.tapEach]] as
  * in `(1 to 10).foreach[Unit] { ... }`, or by ascribing `Unit` as in `(1 to 10).foreach { x =>
  * f(x): Unit }`, or by explicitly discarding the result: `(1 to 10).foreach { x => f(x).discard
  * }`.
  */
object NonUnitForEach extends WartTraverser {
  val messageUnappliedForeach = " used with non-unit-valued function of type "
  val messageAppliedForeach = " discards non-unit value of type "

  override def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*
      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        tree match
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(t) => ()
          case _: Inlined | _: Block => super.traverseTree(tree)(owner)
          case _ =>
            if tree.isExpr then
              tree.asExpr match
                case '{ ($_ : IterableOnceOps[a, cc, c]).foreach[o]($arg) }
                    if !uninterestingType[a] =>
                  isInterestingResult(arg.asTerm).foreach { where =>
                    error(where.pos, "foreach" + messageAppliedForeach + where.tpe.show)
                  }
                case '{ ($_ : IterableOnceOps[a, cc, c]).tapEach[o]($arg) }
                    if !uninterestingType[a] =>
                  isInterestingResult(arg.asTerm).foreach { where =>
                    error(where.pos, "foreach" + messageAppliedForeach + where.tpe.show)
                  }
                case _ => ()

            super.traverseTree(tree)(owner)
      end traverseTree

      def isInterestingResult(tree: Tree): Seq[Term] =
        tree match
          case Lambda(_, body) => isInterestingResult(body)
          case Block(_, res) => isInterestingResult(res)
          // Look into each branch and warn only if the return type is interesting, i.e., neither Unit nor a builder.
          // Scalac infers Any for the whole if/match statement if some of the branches use builders and others don't,
          // which we would flag as a discarded value.
          case If(_, thenPart, elsePart) =>
            isInterestingResult(thenPart) ++ isInterestingResult(elsePart)
          case Match(_, cases) =>
            cases.flatMap { case CaseDef(_, _, body) => isInterestingResult(body) }
          case tree: Term if !uninterestingType(tree.tpe) && !isThisTypeResult(tree) =>
            Seq(tree)
          case _ => Seq.empty

      def uninterestingType[T: Type]: Boolean = uninterestingType(TypeRepr.of[T])
      def uninterestingType(tpe: TypeRepr): Boolean = tpe <:< TypeRepr.of[Unit]

      /* Is tree an application with result `this.type`?
       * Accept `b.addOne(x)` and also `xs(i) += x`
       * where the op is an assignment operator.
       *
       * This is similar to scala.reflect.internal.Treeinfo.isThisTypeResult.
       * Differences:
       * - Does not work for receivers that are var
       *   (there does not seem to be an easy way to get this information without using internal APIs)
       */
      def isThisTypeResult(tree: Term): Boolean =
        stripApplies(tree, Nil) match
          case (fun @ Select(receiver, op), args) =>
            tree.tpe match
              case ThisType(tpe) =>
                tpe.termSymbol == receiver.symbol
              case tpe: TermRef =>
                tpe.termSymbol == receiver.symbol || args.exists(tree =>
                  tpe.termSymbol == tree.symbol
                )
              case _ =>
                def checkSingle(sym: Symbol): Boolean =
                  (sym == receiver.symbol) || {
                    receiver match
                      case Apply(_, _) => op == "+=" // xs(i) += x
                      case _ => receiver.symbol.isValDef // fields and vars
                  }

                @tailrec def loop(mt: TypeRepr): Boolean = mt match
                  case MethodType(_, _, restpe) =>
                    restpe match
                      case ThisType(tpe) => checkSingle(tpe.termSymbol)
                      case tpe: TermRef => checkSingle(tpe.termSymbol)
                      case _ => loop(restpe)
                  case PolyType(_, _, restpe) => loop(restpe)
                  case _ => false
                loop(fun.tpe.widen)
          case _ =>
            tree.tpe match
              case ThisType(_) => true
              case _ => false
      end isThisTypeResult

      @tailrec def stripApplies(t: Tree, args: List[Tree]): (Tree, List[Tree]) =
        t match
          case Typed(tree, _) => stripApplies(tree, args)
          case TypeApply(x, _) => stripApplies(x, args)
          case Apply(x, xArgs) => stripApplies(x, xArgs ++ args)
          case _ => (t, args)
    }
}
