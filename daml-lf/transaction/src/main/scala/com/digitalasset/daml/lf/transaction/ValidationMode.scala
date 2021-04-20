// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.Ref

sealed trait ValidationMode

object ValidationMode {
  final case class Submitting(readAs: Set[Ref.Party]) extends ValidationMode
  final case object Validating extends ValidationMode
}
