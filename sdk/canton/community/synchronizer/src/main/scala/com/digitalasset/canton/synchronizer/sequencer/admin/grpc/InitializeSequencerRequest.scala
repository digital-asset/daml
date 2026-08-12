// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.admin.grpc

import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.synchronizer.sequencer.SequencerSnapshot
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions

final case class InitializeSequencerRequest(
    topologySnapshot: GenericStoredTopologyTransactions,
    synchronizerParameters: StaticSynchronizerParameters,
    sequencerSnapshot: Option[SequencerSnapshot] = None,
)
