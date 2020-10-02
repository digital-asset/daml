// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.dependencygraph

import scala.language.higherKinds

import com.daml.lf.iface.DefDataType

import scalaz.{Applicative, Traverse}
import scalaz.syntax.functor._

// wraps type declarations and templates so that they can be used together
sealed abstract class TypeDeclOrTemplateWrapper[+TmplI] extends Product with Serializable

object TypeDeclOrTemplateWrapper {
  implicit val `TD covariant`: Traverse[TypeDeclOrTemplateWrapper] =
    new Traverse[TypeDeclOrTemplateWrapper] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: TypeDeclOrTemplateWrapper[A])(
          f: A => G[B]) =
        fa match {
          case fa @ TypeDeclWrapper(_) => Applicative[G].point(fa)
          case TemplateWrapper(t) => f(t) map (TemplateWrapper(_))
        }
    }
}

final case class TypeDeclWrapper(typeDecl: DefDataType.FWT)
    extends TypeDeclOrTemplateWrapper[Nothing]

final case class TemplateWrapper[+TmplI](template: TmplI) extends TypeDeclOrTemplateWrapper[TmplI]
