// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import cats.Eval
import com.digitalasset.canton.participant.config.ParticipantStoreConfig
import com.digitalasset.canton.participant.ledger.api.CantonAdminToken
import com.digitalasset.canton.participant.store.MultiDomainEventLog
import com.digitalasset.canton.resource.Storage

final case class ParticipantSchedulersParameters(
    isInitiallyActive: Boolean, // whether to initially start schedulers
    multiDomainEventLog: Eval[
      MultiDomainEventLog
    ], // the multi domain event log needed to map time to offsets
    storage: Storage, // storage to build the pruning scheduler store that tracks the current schedule
    adminToken: CantonAdminToken, // the admin token is needed to invoke pruning via the ledger-api
    pruningConfig: ParticipantStoreConfig, // pruning configuration
)
