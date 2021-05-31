// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import scala.collection.compat.immutable.LazyList

trait LedgerDataImporter {
  def read(): LazyList[(SubmissionInfo, WriteSet)]
}
