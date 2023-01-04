// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.ports.Port

import scala.concurrent.Future

// TODO LLP: Rename package to .apiservice
trait ApiService {

  /** the API port the server is listening on */
  def port: Port

  /** completes when all services have been closed during the shutdown */
  def servicesClosed(): Future[Unit]

}
