// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.lf

import com.digitalasset.codegen.lf.DamlRecordOrVariantTypeGen.RecordOrVariant
import com.digitalasset.daml.lf.iface.{Type, TypeVar, TypeCon, TypePrim}
import scalaz.Monoid
import scalaz.std.list._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.syntax.bifoldable._
import scalaz.syntax.foldable._
import scalaz.syntax.monoid._

object UsedTypeParams {

  /**
    * Returns only type parameters that specified in fields, not relying on `RecordOrVariant.typeVars`.
    */
  def collectTypeParamsInUse(typeDecl: RecordOrVariant): Set[String] =
    foldMapGenTypes(typeDecl)(collectTypeParams)

  private def foldMapGenTypes[Z: Monoid](typeDecl: RecordOrVariant)(f: Type => Z): Z = {
    val notAGT = (s: String) => mzero[Z]
    typeDecl.foldMap(_.bifoldMap(f)(_.bifoldMap(_ foldMap (_.bifoldMap(notAGT)(f)))(f)))
  }

  private def collectTypeParams(field: Type): Set[String] = field match {
    case TypeVar(x) => Set(x)
    case TypePrim(_, xs) => xs.toSet.flatMap(collectTypeParams)
    case TypeCon(_, xs) => xs.toSet.flatMap(collectTypeParams)
  }
}
