// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import io.grpc.stub.ServerCallStreamObserver

trait OngoingAuthorizationFactory {
  def apply[A](
      observer: ServerCallStreamObserver[A],
      claims: ClaimSet.Claims,
  ): ServerCallStreamObserver[A]
}

final case class NoOpOngoingAuthorizationFactory() extends OngoingAuthorizationFactory {
  def apply[A](
      observer: ServerCallStreamObserver[A],
      claims: ClaimSet.Claims,
  ): ServerCallStreamObserver[A] = observer
}
