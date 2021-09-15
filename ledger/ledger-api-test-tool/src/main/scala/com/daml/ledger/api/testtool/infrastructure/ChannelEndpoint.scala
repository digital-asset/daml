// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import io.grpc.Channel

final case class ChannelEndpoint(channel: Channel, hostname: String, port: Int)
