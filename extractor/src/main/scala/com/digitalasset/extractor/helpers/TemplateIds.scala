// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.helpers

import com.digitalasset.daml.lf.iface.Interface
import com.digitalasset.extractor.config.TemplateConfig
import com.digitalasset.ledger.api.v1.value.Identifier

object TemplateIds {

  def getTemplateIds(interfaces: Set[Interface]): Set[Identifier] =
    com.digitalasset.ledger.service.TemplateIds.getTemplateIds(interfaces)

  def intersection(known: Set[Identifier], requested: Set[TemplateConfig]): Set[Identifier] =
    known.filter(a => requested.contains(asConf(a)))

  private def asConf(a: Identifier): TemplateConfig =
    TemplateConfig(moduleName = a.moduleName, entityName = a.entityName)
}
