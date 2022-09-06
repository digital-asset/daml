// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import com.daml.lf.codegen.Util
import com.daml.lf.codegen.lf.UsedTypeParams.Variance
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.PackageSignature.TypeDecl
import scalaz.syntax.foldable._
import scalaz.std.set._

final class VarianceCache(interface: EnvironmentSignature) {

  private[this] def foldTemplateReferencedTypeDeclRoots[Z](interface: EnvironmentSignature, z: Z)(
      f: (Z, ScopedDataType.Name) => Z
  ): Z =
    interface.typeDecls.foldLeft(z) {
      case (z, (id, TypeDecl.Template(_, tpl))) =>
        tpl.foldMap(typ => Util.genTypeTopLevelDeclNames(typ).toSet).foldLeft(f(z, id))(f)
      case (z, _) => z
    }

  protected[this] def precacheVariance(
      interface: EnvironmentSignature
  ): ScopedDataType.Name => ImmArraySeq[Variance] = {
    import UsedTypeParams.ResolvedVariance
    val resolved = foldTemplateReferencedTypeDeclRoots(interface, ResolvedVariance.Empty) {
      (resolved, id) => resolved.allCovariantVars(id, interface)._1
    }
    id => resolved.allCovariantVars(id, interface)._2
  }

  private[this] lazy val precachedVariance = precacheVariance(interface)

  def apply(sdt: ScopedDataType[_]): Seq[Variance] =
    precachedVariance(sdt.name)

}
