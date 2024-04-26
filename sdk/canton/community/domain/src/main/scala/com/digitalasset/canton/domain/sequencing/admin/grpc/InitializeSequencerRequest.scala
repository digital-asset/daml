// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.grpc

import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions

final case class InitializeSequencerRequest(
    topologySnapshot: GenericStoredTopologyTransactions,
    domainParameters: StaticDomainParameters,
    sequencerSnapshot: Option[SequencerSnapshot] = None,
)
