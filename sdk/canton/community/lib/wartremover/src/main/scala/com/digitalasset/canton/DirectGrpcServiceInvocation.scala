// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import io.grpc.stub.AbstractStub
import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.{StaticAnnotation, tailrec}

/** This wart warns when methods for a gRPC service stub are called directly.
  * Instead, the wrapper methods from `CantonGrpcUtil` and others should be used.
  * This ensures in particular the following aspects:
  * - The current trace context is set in the gRPC context prior to the call
  *   so that the gRPC stub implementation picks it up.
  * - Error handling is done consistently across gRPC calls.
  *
  * Such helper methods are annotated `@GrpcServiceInvocationMethod`.
  * The wart does not check any arguments at any call site, nor the implementation of such a method
  * or any of its overrides. It is the responsibility of the developer to ensure that
  * typical usage patterns of this method are safe w.r.t. the above aspects.
  * Primary constructors can be annotated via their class.
  *
  * For example, instead of `myServiceStub.myMethod(request)`, you should use
  * `CantonGrpcUtil.sendGrpcRequest(myServiceStub, ...)(_.myMethod(request))`.
  */
object DirectGrpcServiceInvocation extends WartTraverser {

  val message =
    "Do not invoke gRPC services directly. Use the methods from CantonGrpcUtil. If the invocation already uses such a method and arguments are named, make sure that the named arguments are in the same order as in the method definition."

  override def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val abstractStubTypeSymbol =
      typeOf[AbstractStub[Nothing]].typeConstructor.typeSymbol
    val grpcServiceInvocationMethodAnnotation = typeOf[GrpcServiceInvocationMethod]

    def isSubtypeOfAbstractStub(typ: Type): Boolean =
      typ.typeConstructor
        .baseType(abstractStubTypeSymbol)
        .typeConstructor
        .typeSymbol == abstractStubTypeSymbol

    // This is the list of unary method names in AbstractStub. We still want to allow calls to them.
    // If the gRPC service defines an endpoint with such a name, this wart will not catch direct invocations.
    val allowedMethodNames = Seq(
      "equals", // from Object
      "withDeadline",
      "withExecutor",
      "withCompression",
      "withChannel",
      "withInterceptors",
      "withCallCredentials",
      "withMaxInboundMessageSize",
      "withMaxOutboundMessageSize",
    ).map(TermName(_)).toSet[Name]

    new u.Traverser {

      def isGrpcServiceInvocationMethodAnnotation(annotation: Annotation): Boolean =
        annotation.tree.tpe =:= grpcServiceInvocationMethodAnnotation

      def isAllowedInvocationMethodContext(tree: Tree): Boolean =
        tree.symbol.annotations.exists(isGrpcServiceInvocationMethodAnnotation)

      def isMethodDefWithGrpcServiceInvocationMethodAnnotation(symbol: Symbol): Boolean =
        symbol.annotations.exists(isGrpcServiceInvocationMethodAnnotation)
          || symbol.overrides.exists(isMethodDefWithGrpcServiceInvocationMethodAnnotation)

      def isArtifactValDef(x: Tree): Boolean =
        x match {
          case valDef: ValDef => valDef.mods.hasFlag(Flag.ARTIFACT)
          case _ => false
        }

      @tailrec def stripApplies(t: Tree): Tree =
        t match {
          case TypeApply(x, _) => stripApplies(x)
          case Apply(x, _) => stripApplies(x)
          case _ => t
        }

      object MethodCallWithGrpcServiceInvocationAnnotation {
        def unapply(t: Tree): Option[Tree] =
          stripApplies(t) match {
            // Special treatment for annotated primary constructors: grab the annotation from the class
            case Select(New(receiver), _) =>
              Option.when(isAllowedInvocationMethodContext(receiver))(receiver)
            case method @ Select(receiver, _methodName) =>
              Option.when(isAllowedInvocationMethodContext(method))(receiver)
            case _ => None
          }
      }

      override def traverse(tree: Tree): Unit =
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>

          // Do not look into method definitions that are annotated with GrpcServiceInvocationMethod or override such methods
          case t: DefDef if isMethodDefWithGrpcServiceInvocationMethodAnnotation(t.symbol) =>

          // This is a call to a method annotated with `@GrpcServiceInvocationMethod`.
          // We ignore all parameters of the call as one of them typically contains the stub invocation as a lambda.
          case MethodCallWithGrpcServiceInvocationAnnotation(receiver) =>
            super.traverse(receiver)

          // This is a call to a method annotated with `@GrpcServiceInvocationMethod`
          // that the compiler has wrapped in a block with artificial val definitions
          // to ensure evaluation order (e.g., when named arguments are used).
          case Block(defs, MethodCallWithGrpcServiceInvocationAnnotation(receiver))
              if defs.forall(isArtifactValDef) =>
            super.traverse(receiver)

          // This is a call to a stub's method that we want to flag
          case Apply(Select(receiver, method), args)
              if args.sizeIs == 1 && !allowedMethodNames.contains(method) &&
                receiver.tpe != null && isSubtypeOfAbstractStub(receiver.tpe) =>
            error(u)(tree.pos, message)

          case _ =>
            super.traverse(tree)
        }
    }
  }
}

/** Annotation for methods and constructors. Implementations of such method (and any overrides) are not checked.
  * Neither are the arguments to calls of such a method.
  */
final class GrpcServiceInvocationMethod extends StaticAnnotation
