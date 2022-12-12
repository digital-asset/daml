// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import java.text.Normalizer

object EncodingUtil {
  def normalize(s: String): String = Normalizer.normalize(s, Normalizer.Form.NFD)
}
