// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.{Foldable, Traverse, TraverseFilter}
import org.wartremover.{WartTraverser, WartUniverse}

import scala.quoted.Type

/** Cats' traverse and sequence methods do not specify the evaluation behaviour and this behaviour
  * has changed between minor versions (e.g., 2.6 and 2.7). When we traverse over containers, we
  * should therefore be explicit whether the traversal is parallel or sequential. This wart flags
  * all usages of Cats' syntax extensions for future-like applicatives, except when used with the
  * singleton containers [[scala.Option]] and [[scala.Either]].
  *
  * Limitations:
  *   - It does not flag traverse calls whose applicative instance is more general and can be
  *     instantiated with a future-like instance.
  */
object FutureTraverse extends WartTraverser {
  def errorMessageFor(methodName: String, hint: String = ""): String =
    s"Do not use $methodName with future-like applicatives.${if hint.isEmpty then "" else " "}$hint"

  override def apply(u: WartUniverse): u.Traverser = {
    new u.Traverser(this) {
      import q.reflect.*

      /* Describes a method call that should be flagged by this wart
       *
       * @param methodName The name of the method
       * @param classSymbol The class symbol that declares the method
       * @param hint A hint to be included in the error message
       */
      final case class ForbiddenMethod(classSymbol: Symbol, methodName: String, hint: String)

      object ForbiddenMethod {
        def apply[T: Type](methodName: String, hint: String = ""): ForbiddenMethod =
          ForbiddenMethod(getClassSymbol[T], methodName, hint)
      }

      // Cats has two ways to call traverse and friends: Through the `Traverse` type class
      // and through the syntax extension method in the corresponding `Ops` class
      // So we need to test for both of them
      val forbidden: Seq[ForbiddenMethod] = Seq(
        ForbiddenMethod[Traverse.Ops[?, ?]](
          "traverse",
          "Use .parTraverse or MonadUtil.sequentialTraverse.",
        ),
        ForbiddenMethod[Traverse[?]](
          "traverse",
          "Use .parTraverse or MonadUtil.sequentialTraverse.",
        ),
        ForbiddenMethod[Traverse.Ops[?, ?]]("traverseTap"),
        ForbiddenMethod[Traverse[?]]("traverseTap"),
        ForbiddenMethod[Traverse.Ops[?, ?]]("flatTraverse", "Use .parFlatTraverse."),
        ForbiddenMethod[Traverse[?]]("flatTraverse", "Use .parFlatTraverse."),
        ForbiddenMethod[Foldable.Ops[?, ?]](
          "traverse_",
          "Use .parTraverse_ or MonadUtil.sequentialTraverse_.",
        ),
        ForbiddenMethod[Foldable[?]](
          "traverse_",
          "Use .parTraverse_ or MonadUtil.sequentialTraverse_.",
        ),
        ForbiddenMethod[TraverseFilter.Ops[?, ?]](
          "traverseFilter",
          "Use .parTraverseFilter.",
        ),
        ForbiddenMethod[TraverseFilter[?]](
          "traverseFilter",
          "Use .parTraverseFilter.",
        ),
        ForbiddenMethod[TraverseFilter.Ops[?, ?]](
          "filterA",
          "Use .parFilterA.",
        ),
        ForbiddenMethod[TraverseFilter[?]](
          "filterA",
          "Use .parFilterA.",
        ),
      )

      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        tree match
          // Ignore trees marked by SuppressWarnings
          case _ if hasWartAnnotation(tree) => ()
          case TypeApply(Select(receiver, method), tyargs) =>
            isForbidden(receiver, method, tyargs) match
              case None => super.traverseTree(tree)(owner)
              case Some(message) => error(tree.pos, message)
          case _ =>
            super.traverseTree(tree)(owner)

      private def isForbidden(
          receiver: Tree,
          method: String,
          tyArgs: List[Tree],
      ): Option[String] = {
        val receiverTpe = receiver.asExpr.asTerm.tpe
        forbidden
          .find { forbid =>
            forbid.methodName == method &&
            receiverTpe.baseClasses.contains(forbid.classSymbol) &&
            isFutureLike(tyArgs(0)) &&
            !isSingletonContainer(receiver)
          }
          .map { forbid =>
            errorMessageFor(forbid.methodName.toString, forbid.hint)
          }
      }

      private val futureLikeTester = FutureLikeTester.tester[DoNotTraverseLikeFuture](u)
      private def isFutureLike(tree: Tree): Boolean = tree match
        case tree: TypeTree => futureLikeTester(tree.tpe)

      private def isSingletonContainer(tree: Tree): Boolean =
        tree.asExpr match
          case '{ $_ : Traverse.Ops[f, a] } => isSingletonContainer[f[Any]]
          case '{ $_ : Traverse[f] } => isSingletonContainer[f[Any]]
          case '{ $_ : Foldable.Ops[f, a] } => isSingletonContainer[f[Any]]
          case '{ $_ : Foldable[f] } => isSingletonContainer[f[Any]]
          case '{ $_ : TraverseFilter.Ops[f, a] } => isSingletonContainer[f[Any]]
          case '{ $_ : TraverseFilter[f] } => isSingletonContainer[f[Any]]

      private def isSingletonContainer[T: Type]: Boolean = {

        val tpe = TypeRepr.of[T]
        tpe <:< TypeRepr.of[Option[Any]] ||
        tpe <:< TypeRepr.of[Either[Any, Any]] ||
        tpe.baseClasses
          .flatMap(_.annotations)
          .exists(term => term.tpe <:< TypeRepr.of[AllowTraverseSingleContainer])
      }

      private def getClassSymbol[T: Type]: Symbol =
        TypeRepr.of[T].classSymbol.getOrElse(report.errorAndAbort("no symbol"))
    }
  }

}
