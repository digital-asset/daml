// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.store.DamlStateKey

final class KeyNotPresentInInputException(key: DamlStateKey)
    extends IllegalStateException(
      s"The committer accessed a key that was not present in the input.\nKey: $key"
    )
