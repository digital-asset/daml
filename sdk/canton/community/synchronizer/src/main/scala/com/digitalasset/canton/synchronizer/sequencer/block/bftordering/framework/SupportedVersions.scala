// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework

import com.digitalasset.canton.version.ProtocolVersion.ProtocolVersionWithStatus
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion, ProtocolVersionAnnotation}

object SupportedVersions {

  // Canton synchronizer components with multiple releases within a major release line may support multiple Canton
  //  protocol versions, so that they may be also used as drop-in replacement to fix minor bugs, but the protocol
  //  version at runtime is fixed: a synchronizer is created with and stays on a single protocol version
  //  for its entire life (participants, however, can connect to multiple synchronizers that may use different
  //  protocol versions, so they need to be able to speak multiple protocol versions at runtime).
  //
  //  However, since the BFT orderer is unreleased, it currently supports only one Canton protocol version
  //  and only one protobuf data version.

  val CantonProtocol: ProtocolVersionWithStatus[ProtocolVersionAnnotation.Alpha] =
    ProtocolVersion.v34

  // Each protobuf data version can work with multiple Canton protocol versions; the set of consecutive Canton
  //  protocol versions that use the same protobuf data version are designated via a representative
  //  Canton protocol version.
  // TODO(#25269): support multiple protobuf data versions
  val ProtoData: ProtoVersion = ProtoVersion(30)
}
