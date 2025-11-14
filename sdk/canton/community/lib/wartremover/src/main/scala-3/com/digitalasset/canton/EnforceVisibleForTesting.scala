// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import com.google.common.annotations.VisibleForTesting
import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.tailrec
import scala.collection.mutable

/** Wart such that we avoid using vals, defs, objects, classes, etc... that are marked with
  * [[com.google.common.annotations.VisibleForTesting]] in production code.
  *
  * Notice that usage of such definitions is allowed in the private or protected scope.
  */
object EnforceVisibleForTesting extends WartTraverser {

  val message =
    "A member with the annotation @VisibleForTesting MUST NOT be used in production code outside the private or protected scope."

  def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*
      val currentDefinitions = mutable.Set[Symbol]()

      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else
          tree match
            case _: ClassDef | _: DefDef | _: ValDef =>
              if isVisibleForTesting(tree.symbol) then
                // Do not look into definitions that are annotated with VisibleForTesting.
                // Basically, allow the usage of @VisibleForTesting annotated members within
                // the body of defs/vals defined with @VisibleForTesting
                ()
              else
                // record that we are within a definition
                inDefinition(tree.symbol)(super.traverseTree(tree)(owner))
            case _ if isAccessOfVisibleForTestingDef(tree) =>
              error(tree.pos, message)
              super.traverseTree(tree)(owner)
            case _ =>
              super.traverseTree(tree)(owner)

      private def inDefinition(symbol: Symbol)(f: => Unit): Unit = {
        currentDefinitions.addOne(symbol)
        f
        val _ = currentDefinitions.remove(symbol)
      }

      private def isVisibleForTesting(symbol: Symbol): Boolean =
        symbol.annotations.exists(_.tpe <:< TypeRepr.of[VisibleForTesting]) ||
          symbol.allOverriddenSymbols.exists(isVisibleForTesting)

      private def isAccessOfVisibleForTestingDef(tree: Tree): Boolean =
        tree match
          case Select(receiver, name) =>
            if
              // we are currently in the body of the receiver
              currentDefinitions.contains(receiver.tpe.typeSymbol.companionClass) ||
              // we are currently in the body of the companion class of the receiver
              currentDefinitions.contains(receiver.tpe.typeSymbol) ||
              // we are currently in the enclosing scope of the receiver's type
              currentlyVisitingParents(receiver.tpe.typeSymbol)
            then false
            else
              // check whether the method/val is annotated with @VisibleForTesting
              isVisibleForTesting(tree.symbol) ||
              // check whether the type of the method call/constructor is annotated with @VisibleForTesting
              isVisibleForTesting(receiver.tpe.typeSymbol) ||
              (tree.isExpr && isVisibleForTesting(tree.asExpr.asTerm.tpe.typeSymbol))
          case _ => false

      @tailrec
      def currentlyVisitingParents(symbol: Symbol): Boolean =
        !symbol.isNoSymbol && !symbol.isPackageDef &&
          (
            (!symbol.owner.isNoSymbol && currentDefinitions.contains(symbol.owner)) ||
              currentlyVisitingParents(symbol.owner)
          )
    }
}
