// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.synchronizer.sequencing.authentication.MemberAuthenticationService
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.SequencerAuthenticationServerInterceptor
import com.digitalasset.canton.synchronizer.sequencing.service.GrpcSequencerAuthenticationService

final case class AuthenticationServices(
    syncCryptoForAuthentication: SynchronizerCryptoClient,
    memberAuthenticationService: MemberAuthenticationService,
    sequencerAuthenticationService: GrpcSequencerAuthenticationService,
    authenticationServerInterceptor: SequencerAuthenticationServerInterceptor,
)
