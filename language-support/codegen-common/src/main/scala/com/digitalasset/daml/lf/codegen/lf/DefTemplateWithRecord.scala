// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import com.daml.lf.iface

case class DefTemplateWithRecord[+Type](
    `type`: iface.Record[Type],
    template: iface.DefTemplate[Type],
)
object DefTemplateWithRecord {
  type FWT = DefTemplateWithRecord[iface.Type]
}
