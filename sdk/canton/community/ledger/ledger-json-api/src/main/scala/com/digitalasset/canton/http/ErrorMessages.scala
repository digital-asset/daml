// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

object ErrorMessages {
  def cannotResolveTemplateId(t: domain.ContractTypeId[_]): String =
    s"Cannot resolve template ID, given: ${t.toString}"

  def cannotResolveAnyTemplateId: String =
    "Cannot resolve any template ID from request"

  def cannotResolveTemplateId(a: domain.ContractLocator[_]): String =
    s"Cannot resolve templateId, given: $a"
}
