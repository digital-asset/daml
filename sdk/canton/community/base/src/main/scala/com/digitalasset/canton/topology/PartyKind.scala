// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.google.common.annotations.VisibleForTesting

/** Used in tests to run them either with local or external parties.
  */
@VisibleForTesting
private[canton] sealed trait PartyKind
private[canton] object PartyKind {

  /** A party hosted with submission permission on a participant node and existing within that
    * node's namespace
    */
  case object Local extends PartyKind

  /** A party hosted with at most confirmation permission and with their own namespace and protocol
    * signing keys
    */
  case object External extends PartyKind
}
