// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.{Foldable, Traverse, TraverseFilter}
import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.StaticAnnotation

/** Cats' traverse and sequence methods do not specify the evaluation behaviour
  * and this behaviour has changed between minor versions (e.g., 2.6 and 2.7).
  * When we traverse over containers, we should therefore be explicit whether the traversal
  * is parallel or sequential. This wart flags all usages of Cats' syntax extensions
  * for future-like applicatives, except when used with the singleton containers
  * [[scala.Option]] and [[scala.Either]].
  *
  * Limitations:
  * - It does not flag traverse calls whose applicative instance is more general
  *   and can be instantiated with a future-like instance.
  */
object FutureTraverse extends WartTraverser {
  def errorMessageFor(methodName: String, hint: String = ""): String =
    s"Do not use $methodName with future-like applicatives.${if (hint.isEmpty) "" else " "}$hint"

  override def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    /* Describes a method call that should be flagged by this wart
     *
     * @param methodName The name of the method
     * @param receiverTypeSymbol The type symbol of the receiver that declared the called method
     * @param applicativePosition The type argument of the method that should be future-like
     * @param hint A hint to be included in the error message
     */
    final case class ForbiddenMethod(
        methodName: TermName,
        receiverTypeSymbol: Symbol,
        applicativePosition: Int,
        hint: String,
    ) {
      require(applicativePosition >= 0, s"Type argument position must be non-negative")
    }
    object ForbiddenMethod {
      def apply(
          method: String,
          receiverTypeSymbol: Symbol,
          applicativePosition: Int,
          hint: String = "",
      ): ForbiddenMethod =
        ForbiddenMethod(
          TermName(method),
          receiverTypeSymbol,
          applicativePosition,
          hint,
        )
    }

    // Cats has two ways to call traverse and friends: Through the `Traverse` type class
    // and through the syntax extension method in the corresponding `Ops` class
    // So we need to test for both of them
    val traverseTrait = typeOf[Traverse[Seq]].typeSymbol
    require(traverseTrait != NoSymbol)
    val traverseOps = typeOf[Traverse.Ops[Seq, Int]].typeSymbol
    require(traverseOps != NoSymbol)

    val foldableTrait = typeOf[Foldable[Seq]].typeSymbol
    require(foldableTrait != NoSymbol)
    val foldableOps = typeOf[Foldable.Ops[Seq, Int]].typeSymbol
    require(foldableOps != NoSymbol)

    val traverseFilterTrait = typeOf[TraverseFilter[Seq]].typeSymbol
    require(traverseFilterTrait != NoSymbol)
    val traverseFilterOps = typeOf[TraverseFilter.Ops[Seq, Int]].typeSymbol
    require(traverseFilterOps != NoSymbol)

    val unorderedFoldableCompanion = typeOf[cats.UnorderedFoldable[List]].companion
    val traverseInstanceOptionSymbol =
      unorderedFoldableCompanion.decl(TermName("catsTraverseForOption"))
    require(traverseInstanceOptionSymbol != NoSymbol)
    val traverseInstanceEitherSymbol =
      unorderedFoldableCompanion.decl(TermName("catsTraverseForEither"))
    require(traverseInstanceEitherSymbol != NoSymbol)
    val functorFilterCompanion = typeOf[cats.FunctorFilter[List]].companion
    val traverseFilterInstanceOptionSymbol =
      functorFilterCompanion.decl(TermName("catsTraverseFilterForOption"))
    require(traverseFilterInstanceOptionSymbol != NoSymbol)

    val forbidden: Seq[ForbiddenMethod] = Seq(
      ForbiddenMethod(
        "traverse",
        traverseOps,
        0,
        "Use .parTraverse or MonadUtil.sequentialTraverse.",
      ),
      ForbiddenMethod(
        "traverse",
        traverseTrait,
        0,
        "Use .parTraverse or MonadUtil.sequentialTraverse.",
      ),
      ForbiddenMethod("traverseTap", traverseOps, 0),
      ForbiddenMethod("traverseTap", traverseTrait, 0),
      ForbiddenMethod("flatTraverse", traverseOps, 0, "Use .parFlatTraverse."),
      ForbiddenMethod("flatTraverse", traverseTrait, 0, "Use .parFlatTraverse."),
      ForbiddenMethod(
        "traverse_",
        foldableOps,
        0,
        "Use .parTraverse_ or MonadUtil.sequentialTraverse_.",
      ),
      ForbiddenMethod(
        "traverse_",
        foldableTrait,
        0,
        "Use .parTraverse_ or MonadUtil.sequentialTraverse_.",
      ),
      ForbiddenMethod(
        "traverseFilter",
        traverseFilterOps,
        0,
        "Use .parTraverseFilter.",
      ),
      ForbiddenMethod(
        "traverseFilter",
        traverseFilterTrait,
        0,
        "Use .parTraverseFilter.",
      ),
      ForbiddenMethod(
        "filterA",
        traverseFilterOps,
        0,
        "Use .parFilterA.",
      ),
      ForbiddenMethod(
        "filterA",
        traverseFilterTrait,
        0,
        "Use .parFilterA.",
      ),
    )

    val allowedImplicits = Seq(
      traverseInstanceOptionSymbol,
      traverseInstanceEitherSymbol,
      traverseFilterInstanceOptionSymbol,
    )

    val futureLikeType = typeOf[DoNotTraverseLikeFuture]
    val futureLikeTester = FutureLikeTester.tester(u)(futureLikeType)

    def isSingletonContainer(receiver: Tree): Boolean = receiver match {
      case Apply(_, List(implicitArg)) if allowedImplicits.contains(implicitArg.symbol) => true
      case _ => false
    }

    def isForbidden(receiver: Tree, method: Name, tyArgs: List[Tree]): Option[String] = {
      val receiverTpe = receiver.tpe
      forbidden
        .find { forbid =>
          forbid.methodName == method &&
          receiverTpe.typeConstructor
            .baseType(forbid.receiverTypeSymbol)
            .typeConstructor
            .typeSymbol == forbid.receiverTypeSymbol &&
          tyArgs.sizeCompare(forbid.applicativePosition) >= 0 &&
          futureLikeTester(tyArgs(forbid.applicativePosition).tpe.typeConstructor) &&
          !isSingletonContainer(receiver)
        }
        .map { forbid =>
          errorMessageFor(forbid.methodName.toString, forbid.hint)
        }
    }

    new u.Traverser {

      override def traverse(tree: Tree): Unit = {
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>
          case TypeApply(Select(receiver, method), tyargs) =>
            isForbidden(receiver, method, tyargs) match {
              case None => super.traverse(tree)
              case Some(message) =>
                error(u)(tree.pos, message)
            }
          case _ =>
            super.traverse(tree)
        }
      }
    }
  }
}

/** Annotated type constructors will be treated like a [[scala.concurrent.Future]]
  * when looking for traverse-like calls with such an applicative instance.
  */
final class DoNotTraverseLikeFuture extends StaticAnnotation
