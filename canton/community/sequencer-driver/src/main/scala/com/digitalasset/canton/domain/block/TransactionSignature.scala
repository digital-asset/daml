// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

/** Signature of a BlockOrdered request.
  *
  * @param signature ed25519 signature for the content param
  * @param submitterSvNodeId id of a signing node (as in canton network)
  * @param submitterKeyId id of a key (fingerprint)
  */
final case class TransactionSignature(
    signature: Array[Byte],
    submitterSvNodeId: String,
    submitterKeyId: String,
)
