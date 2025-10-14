// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.syntax.traverse.*
import com.daml.ledger.api.v2.interactive.interactive_submission_service.CostEstimationHints as CostEstimationHintsP
import com.digitalasset.canton.crypto.SigningAlgorithmSpec
import com.digitalasset.canton.ledger.api.validation.CryptoValidator
import com.digitalasset.canton.logging.ErrorLoggingContext
import io.grpc.StatusRuntimeException

/** Class holding hints used to provide a better traffic cost estimation of prepared transactions
  * @param signingAlgorithmSpec
  *   the number of signatures and their signing algorithm expected to be used at submission time.
  */
final case class CostEstimationHints(
    signingAlgorithmSpec: Seq[SigningAlgorithmSpec] = Seq.empty
)

object CostEstimationHints {
  def fromProto(
      estimationP: CostEstimationHintsP
  )(implicit
      elc: ErrorLoggingContext
  ): Either[StatusRuntimeException, Option[CostEstimationHints]] =
    Option
      .when(!estimationP.disabled) {
        estimationP.expectedSignatures
          .traverse(
            CryptoValidator.validateSigningAlgorithmSpec(_, "estimate_traffic_cost")(elc)
          )
          .map(CostEstimationHints(_))
      }
      .sequence
}
