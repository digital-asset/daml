// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftKeyId

object FingerprintKeyId {

  def toBftKeyId(fingerprint: Fingerprint): BftKeyId =
    BftKeyId(fingerprint.toProtoPrimitive)
}
