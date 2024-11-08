// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant

/** Interfaces to read from and write to an (abstract) participant state.
  *
  * A Daml ledger participant is code that allows to actively participate in
  * the evolution of a shared Daml ledger. Each such participant maintains a
  * particular view onto the state of the Daml ledger. We call this view the
  * participant state.
  *
  * Actual implementations of a Daml ledger participant will likely maintain
  * more state than what is exposed through the interfaces in this package,
  * which is why we talk about an abstract participant state. It abstracts
  * over the different implementations of Daml ledger participants.
  *
  * The interfaces are optimized for easy implementation. The
  * [[SyncService]] interface contains the methods for changing the
  * participant state (and potentially the state of the Daml ledger), which
  * all ledger participants must support. These methods are for example
  * exposed via the Daml Ledger API. Actual ledger participant implementations
  * likely support more implementation-specific methods. They are however not
  * exposed via the Daml Ledger API.
  */
package object state
