// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.helpers

import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.daml.lf.iface.Interface
import com.digitalasset.daml.lf.iface.InterfaceType.Template
import com.digitalasset.extractor.config.TemplateConfig
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
          name = qn.qualifiedName,
          moduleName = qn.module.dottedName,
          entityName = qn.name.dottedName)
    }(breakOut)

  def intersection(known: Set[Identifier], requested: Set[TemplateConfig]): Set[Identifier] =
    known.filter(a => requested.contains(asConf(a)))

  private def asConf(a: Identifier): TemplateConfig =
    TemplateConfig(moduleName = a.moduleName, entityName = a.entityName)
}
