// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import io.grpc.Channel

sealed trait Endpoint

object Endpoint {

  final case object InProcess extends Endpoint

  final case class Remote(hostname: String, port: Int) extends Endpoint

}

final case class ChannelEndpoint(channel: Channel, endpoint: Endpoint)

object ChannelEndpoint {

  def forRemote(channel: Channel, hostname: String, port: Int): ChannelEndpoint =
    ChannelEndpoint(channel, Endpoint.Remote(hostname, port))

  def forInProcess(channel: Channel): ChannelEndpoint = ChannelEndpoint(channel, Endpoint.InProcess)

}
