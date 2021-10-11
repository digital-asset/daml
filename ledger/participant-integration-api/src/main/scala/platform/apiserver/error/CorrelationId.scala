// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

case class CorrelationId(id: Option[String]) extends AnyVal
case object CorrelationId {
  def none: CorrelationId = CorrelationId(None)
}
