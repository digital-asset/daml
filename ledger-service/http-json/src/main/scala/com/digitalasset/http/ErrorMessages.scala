// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import scalaz.syntax.tag._

object ErrorMessages {
  def cannotResolveTemplateId(t: domain.TemplateId[_]): String =
    s"Cannot resolve template ID, given: ${t.toString}"

  def cannotResolveAnyTemplateId: String =
    "Cannot not resolve any template ID from request"

  def cannotResolveTemplateId(a: domain.ContractLocator[_]): String =
    s"Cannot resolve templateId, given: $a"

  def cannotResolvePayloadType(t: domain.TemplateId[_]): String =
    s"Cannot resolve payload type, given: ${t.toString}"

  def cannotResolveChoiceArgType(t: domain.TemplateId[_], c: domain.Choice): String =
    s"Cannot resolve choice argument type, given: ${t.toString}, ${c.unwrap}"
}
