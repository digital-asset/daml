// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.service

import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.daml.lf.iface.Interface
import com.digitalasset.daml.lf.iface.InterfaceType.Template
import com.digitalasset.ledger.api.v1.value.Identifier

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
