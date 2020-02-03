// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

object ErrorMessages {
  def cannotResolveTemplateId(t: domain.TemplateId[_]): String =
    s"Cannot resolve template ID, given: ${t.toString}"

  def cannotResolveTemplateId(a: domain.ContractLocator[_]): String =
    s"Cannot resolve templateId, given: $a"
}
