// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.Hash

final case class ExternallySignedTransaction(
    // TODO(i20660): This is used only in TransactionConfirmationRequestFactory to verify the signature
    // It should not be shipped to informees as they cannot trust it and will need to re-compute
    // the hash from the transaction themselves anyway
    hash: Hash,
    signatures: TransactionAuthorizationPartySignatures,
)
