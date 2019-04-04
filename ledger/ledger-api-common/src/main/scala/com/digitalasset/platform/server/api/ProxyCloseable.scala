// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api

trait ProxyCloseable extends AutoCloseable {

  protected def service: AutoCloseable

  override def close(): Unit = service.close()
}
