// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.ports.Port

import scala.concurrent.Future

trait ApiServer {

  /** The API port that the server is listening on. */
  def port: Port

  /** A future that completes when the server is up and has a valid configuration. */
  def whenReady: Future[Unit]

  /** A future that completes when all services have been closed during the shutdown. */
  def servicesClosed: Future[Unit]

}
