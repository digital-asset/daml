// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.lf

import com.digitalasset.codegen.lf.DamlDataTypeGen.DataType
import com.digitalasset.daml.lf.iface
import scalaz.Monoid
import scalaz.std.list._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.syntax.bifoldable._
import scalaz.syntax.foldable._
import scalaz.syntax.monoid._

object UsedTypeParams {

  /**
    * Returns only type parameters that specified in fields, not relying on `DataType.typeVars`.
    */
  def collectTypeParamsInUse(typeDecl: DataType): Set[String] =
    foldMapGenTypes(typeDecl)(collectTypeParams)

  private def foldMapGenTypes[Z: Monoid](typeDecl: DataType)(f: iface.Type => Z): Z = {
    val notAGT = (s: String) => mzero[Z]
    typeDecl.foldMap(_.bifoldMap(f)(_.bifoldMap(_ foldMap (_.bifoldMap(notAGT)(f)))(f)))
  }

  private def collectTypeParams(field: iface.Type): Set[String] = field match {
    case iface.TypeVar(x) => Set(x)
    case iface.TypePrim(_, xs) => xs.toSet.flatMap(collectTypeParams)
    case iface.TypeCon(_, xs) => xs.toSet.flatMap(collectTypeParams)
    case iface.TypeNumeric(_) => Set.empty
  }
}
