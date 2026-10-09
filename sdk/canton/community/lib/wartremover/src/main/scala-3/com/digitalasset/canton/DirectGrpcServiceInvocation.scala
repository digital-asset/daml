// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import io.grpc.stub.AbstractStub
import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.tailrec
import scala.quoted.Expr

/** This wart warns when methods for a gRPC service stub are called directly. Instead, the wrapper
  * methods from `CantonGrpcUtil` and others should be used. This ensures in particular the
  * following aspects:
  *   - The current trace context is set in the gRPC context prior to the call so that the gRPC stub
  *     implementation picks it up.
  *   - Error handling is done consistently across gRPC calls.
  *
  * Such helper methods are annotated `@GrpcServiceInvocationMethod`. The wart does not check any
  * arguments at any call site, nor the implementation of such a method or any of its overrides. It
  * is the responsibility of the developer to ensure that typical usage patterns of this method are
  * safe w.r.t. the above aspects. Primary constructors can be annotated via their class.
  *
  * For example, instead of `myServiceStub.myMethod(request)`, you should use
  * `CantonGrpcUtil.sendGrpcRequest(myServiceStub, ...)(_.myMethod(request))`.
  */
object DirectGrpcServiceInvocation extends WartTraverser {

  val message =
    "Do not invoke gRPC services directly. Use the methods from CantonGrpcUtil."

  private val allowedMethodNames = Set(
    "equals", // from Object
    "eq", // object reference equality
    "==", // from Object
    "withDeadline",
    "withExecutor",
    "withCompression",
    "withChannel",
    "withInterceptors",
    "withCallCredentials",
    "withMaxInboundMessageSize",
    "withMaxOutboundMessageSize",
  )

  override def apply(u: WartUniverse): u.Traverser =
    // This is the list of unary method names in AbstractStub. We still want to allow calls to them.
    // If the gRPC service defines an endpoint with such a name, this wart will not catch direct invocations.

    new u.Traverser(this) {
      import q.reflect.*
      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        tree match
          // Ignore trees marked by SuppressWarnings
          case _ if hasWartAnnotation(tree) => ()

          // Do not look into method definitions that are annotated with GrpcServiceInvocationMethod or override such methods
          case t: DefDef if hasGrpcServiceInvocationMethodAnnotation(t.symbol) => ()

          // This is a call to a method annotated with `@GrpcServiceInvocationMethod`.
          // We ignore all parameters of the call as one of them typically contains the stub invocation as a lambda.
          case MethodCallWithGrpcServiceInvocationAnnotation(receiver) =>
            super.traverseTree(receiver)(owner)

          // This is a call to a stub's method that we want to flag
          case Apply(Select(receiver, method), args)
              if args.sizeIs == 1 && !allowedMethodNames.contains(method) &&
                receiver.isExpr && isSubtypeOfAbstractStub(receiver.asExpr) =>
            error(tree.pos, message)
            super.traverseTree(tree)(owner)

          case _ =>
            super.traverseTree(tree)(owner)
      end traverseTree

      private object MethodCallWithGrpcServiceInvocationAnnotation {
        def unapply(t: Tree): Option[Tree] =
          stripApplies(t) match
            // Special treatment for annotated primary constructors: grab the annotation from the class
            case Select(New(receiver), _) =>
              Option.when(hasGrpcServiceInvocationMethodAnnotation(receiver.symbol))(receiver)
            case t @ Select(receiver, _) =>
              Option.when(hasGrpcServiceInvocationMethodAnnotation(t.symbol))(receiver)
            case _ => None
      }

      private val abstractStubSym: Option[Symbol] = TypeRepr.of[AbstractStub[?]].classSymbol
      private def isSubtypeOfAbstractStub(expr: Expr[?]): Boolean =
        expr.asTerm.tpe.baseClasses.exists(abstractStubSym.contains)

      private def hasGrpcServiceInvocationMethodAnnotation(symbol: Symbol): Boolean =
        symbol.annotations.exists(_.tpe <:< TypeRepr.of[GrpcServiceInvocationMethod])
          || symbol.allOverriddenSymbols.exists(hasGrpcServiceInvocationMethodAnnotation)

      @tailrec private def stripApplies(t: Tree): Tree =
        t match
          case TypeApply(x, _) => stripApplies(x)
          case Apply(x, _) => stripApplies(x)
          case _ => t
    }
}
