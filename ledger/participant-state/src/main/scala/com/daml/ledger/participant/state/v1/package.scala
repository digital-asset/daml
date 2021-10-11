// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state

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
  * [[v1.WriteService]] interface contains the methods for changing the
  * participant state (and potentially the state of the Daml ledger), which
  * all ledger participants must support. These methods are for example
  * exposed via the Daml Ledger API. Actual ledger participant implementations
  * likely support more implementation-specific methods. They are however not
  * exposed via the Daml Ledger API. The [[v1.ReadService]] interface contains
  * the one method [[v1.ReadService.stateUpdates]] to read the state of a ledger
  * participant. It represents the participant state as a stream of
  * [[v1.Update]]s to an initial participant state. The typical consumer of this
  * method is a class that subscribes to this stream of [[v1.Update]]s and
  * reconstructs (a view of) the actual participant state. See the comments
  * on [[v1.Update]] and [[v1.ReadService.stateUpdates]] for details about the kind
  * of updates and the guarantees given to consumers of the stream of
  * [[v1.Update]]s.
  *
  * We provide a reference implementation of a participant state in
  * [[com.daml.ledger.on.memory.InMemoryLedgerReaderWriter]]. There we
  * model an in-memory ledger, which has by construction a single participant,
  * which hosts all parties. See its comments for details on how that is done,
  * and how its implementation can be used as a blueprint for implementing
  * your own participant state.
  *
  * We do expect the interfaces provided in
  * [[com.daml.ledger.participant.state]] to evolve, which is why we
  * provide them all in the
  * [[com.daml.ledger.participant.state.v1]] package.  Where possible
  * we will evolve them in a backwards compatible fashion, so that a simple
  * recompile suffices to upgrade to a new version. Where that is not
  * possible, we plan to introduce new version of this API in a separate
  * package and maintain it side-by-side with the existing version if
  * possible. There can therefore potentially be multiple versions of
  * participant state APIs at the same time. We plan to deprecate and drop old
  * versions on separate and appropriate timelines.
  */
package object v1
