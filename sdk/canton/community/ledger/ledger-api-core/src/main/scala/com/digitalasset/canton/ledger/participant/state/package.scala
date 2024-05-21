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
  * [[WriteService]] interface contains the methods for changing the
  * participant state (and potentially the state of the Daml ledger), which
  * all ledger participants must support. These methods are for example
  * exposed via the Daml Ledger API. Actual ledger participant implementations
  * likely support more implementation-specific methods. They are however not
  * exposed via the Daml Ledger API. The [[ReadService]] interface contains
  * the one method [[ReadService.stateUpdates]] to read the state of a ledger
  * participant. It represents the participant state as a stream of
  * [[Update]]s to an initial participant state. The typical consumer of this
  * method is a class that subscribes to this stream of [[Update]]s and
  * reconstructs (a view of) the actual participant state. See the comments
  * on [[Update]] and [[ReadService.stateUpdates]] for details about the kind
  * of updates and the guarantees given to consumers of the stream of
  * [[Update]]s.
  */
package object state
