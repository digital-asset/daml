// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.helpers

import com.daml.lf.iface.Interface
import com.daml.extractor.config.TemplateConfig
import com.daml.ledger.api.v1.value.Identifier

object TemplateIds {

  def getTemplateIds(interfaces: Set[Interface]): Set[Identifier] =
    com.daml.ledger.service.TemplateIds.getTemplateIds(interfaces)

  def intersection(known: Set[Identifier], requested: Set[TemplateConfig]): Set[Identifier] =
    known.filter(a => requested.contains(asConf(a)))

  private def asConf(a: Identifier): TemplateConfig =
    TemplateConfig(moduleName = a.moduleName, entityName = a.entityName)
}
