// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

/** Defines the different package preloading modes. */
sealed abstract class PackagePreloadingMode extends Product with Serializable

object PackagePreloadingMode {

  /** Specifies that the packages should be preloading into the engine
    * before committed.  */
  case object Synchronous extends PackagePreloadingMode

  /** Specify that the packages should be preloaded into the engine
    * asynchronously with the rest of the commit process.  This mode
    * is useful for ledger integrations that cannot handle
    * long-running submissions (> 10s).
    * Failure of the preloading process will not affect the
    * commit.  */
  case object Asynchronous extends PackagePreloadingMode

  /** Specify that the packages should not be preloaded into the
    * engine.  */
  case object No extends PackagePreloadingMode

}
