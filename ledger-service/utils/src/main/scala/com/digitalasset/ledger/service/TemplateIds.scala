// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.service

import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.iface.Interface
import com.daml.lf.iface.InterfaceType.Template
import com.daml.ledger.api.v1.value.Identifier

import scala.collection.breakOut

object TemplateIds {
  def getTemplateIds(interfaces: Set[Interface]): Set[Identifier] =
    interfaces.flatMap(getTemplateIds)

  private def getTemplateIds(interface: Interface): Set[Identifier] =
    interface.typeDecls.collect {
      case (qn: QualifiedName, _: Template) =>
        Identifier(
          packageId = interface.packageId,
          moduleName = qn.module.dottedName,
          entityName = qn.name.dottedName)
    }(breakOut)
}
