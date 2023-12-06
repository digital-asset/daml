// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.common.domain.ServiceAgreementId

sealed trait GrpcSequencerAuthenticationSupport

object GrpcSequencerAuthenticationSupport {
  case object Unsupported extends GrpcSequencerAuthenticationSupport
  final case class Supported(acceptedAgreementId: Option[ServiceAgreementId])
      extends GrpcSequencerAuthenticationSupport
}
