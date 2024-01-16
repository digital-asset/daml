// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.topology.MediatorRef
import com.digitalasset.canton.version.ProtocolVersion

object TransferCommonData {
  val minimumPvForMediatorGroups: ProtocolVersion =
    ProtocolVersion.CNTestNet
  val minimumPvForTransferCounter: ProtocolVersion =
    ProtocolVersion.CNTestNet

  private[data] def isGroupMediatorSupported(
      protocolVersion: ProtocolVersion
  ): Boolean = protocolVersion >= minimumPvForMediatorGroups

  def checkMediatorGroup(
      mediator: MediatorRef,
      protocolVersion: ProtocolVersion,
  ): Either[String, Unit] =
    Either.cond(
      mediator.isSingle || isGroupMediatorSupported(protocolVersion),
      (),
      s"Invariant violation: Mediator groups are not supported in protocol version $protocolVersion",
    )
}
