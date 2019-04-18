// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.grpcHeaders

import io.grpc.Metadata

trait GrpcHeadersDataOrigin {
  def getAdditionalHeaderData: Metadata
}

object GrpcHeadersDataOrigin {
  def apply: GrpcHeadersDataOrigin = {
    new GrpcHeadersDataOrigin {
      val emptyMetadata = new Metadata()
      def getAdditionalHeaderData: Metadata = emptyMetadata
    }
  }
}
