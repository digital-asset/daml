// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.service

import com.daml.lf.data.Ref
import com.daml.lf.iface.Interface
import com.daml.lf.iface.InterfaceType.Template
import com.daml.ledger.api.v1.value.Identifier

object TemplateIds {
  def getTemplateIds(interfaces: Set[Interface]): Set[Identifier] =
    interfaces.flatMap { interface =>
      getTemplateIds(
        interface,
        interface.typeDecls.iterator.collect { case (qn, _: Template) => qn },
      )
    }

  def getInterfaceIds(interfaces: Set[Interface]): Set[Identifier] =
    interfaces.flatMap { interface =>
      getTemplateIds(interface, interface.astInterfaces.keysIterator)
    }

  private def getTemplateIds(
      interface: Interface,
      qns: IterableOnce[Ref.QualifiedName],
  ): Set[Identifier] =
    qns.iterator.map { qn =>
      Identifier(
        packageId = interface.packageId,
        moduleName = qn.module.dottedName,
        entityName = qn.name.dottedName,
      )
    }.toSet
}
