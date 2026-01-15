// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.daml_lf

package object synonyms:
  type Dar[A] = com.digitalasset.daml.lf.archive.Dar[A]
  val DarDecoder = com.digitalasset.daml.lf.archive.DarDecoder
