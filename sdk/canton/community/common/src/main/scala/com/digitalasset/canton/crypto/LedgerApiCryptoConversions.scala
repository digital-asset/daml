// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss
import io.scalaland.chimney.Transformer

/** Utility methods to convert between Canton crypto classes and their equivalent on the ledger API.
  */
object LedgerApiCryptoConversions {
  implicit val cantonToLAPISignatureFormatTransformer
      : Transformer[v30.SignatureFormat, iss.SignatureFormat] = {
    case v30.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED =>
      iss.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED
    case v30.SignatureFormat.SIGNATURE_FORMAT_RAW => iss.SignatureFormat.SIGNATURE_FORMAT_RAW
    case v30.SignatureFormat.Unrecognized(unrecognizedValue) =>
      iss.SignatureFormat.Unrecognized(unrecognizedValue)
  }

  implicit val LAPIToCantonSignatureFormatTransformer
      : Transformer[iss.SignatureFormat, v30.SignatureFormat] = {
    case iss.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED =>
      v30.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED
    case iss.SignatureFormat.SIGNATURE_FORMAT_RAW => v30.SignatureFormat.SIGNATURE_FORMAT_RAW
    case iss.SignatureFormat.Unrecognized(unrecognizedValue) =>
      v30.SignatureFormat.Unrecognized(unrecognizedValue)
  }
}
