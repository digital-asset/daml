// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.service

import com.daml.lf.data.Ref
import com.daml.lf.typesig.PackageSignature
import PackageSignature.TypeDecl.Template
import com.daml.ledger.api.v2.value.Identifier

object TemplateIds {
  def getTemplateIds(packages: Set[PackageSignature]): Set[Identifier] =
    packages.flatMap { pkg =>
      getTemplateIds(
        pkg,
        pkg.typeDecls.iterator.collect { case (qn, _: Template) => qn },
      )
    }

  def getInterfaceIds(packages: Set[PackageSignature]): Set[Identifier] =
    packages.flatMap { pkg =>
      getTemplateIds(pkg, pkg.interfaces.keysIterator)
    }

  private def getTemplateIds(
      pkg: PackageSignature,
      qns: IterableOnce[Ref.QualifiedName],
  ): Set[Identifier] =
    qns.iterator.map { qn =>
      Identifier(
        packageId = pkg.packageId,
        moduleName = qn.module.dottedName,
        entityName = qn.name.dottedName,
      )
    }.toSet
}
