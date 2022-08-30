// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import com.daml.lf.typesig.{DefTemplate, Record, Type}

final case class DefTemplateWithRecord(
    `type`: Record[Type],
    template: DefTemplate[Type],
)
