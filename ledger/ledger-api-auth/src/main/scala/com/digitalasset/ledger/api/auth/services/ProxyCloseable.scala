// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

trait ProxyCloseable extends AutoCloseable {

  protected def service: AutoCloseable

  override def close(): Unit = service.close()
}
