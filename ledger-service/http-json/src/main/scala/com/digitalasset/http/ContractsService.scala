// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient

class ContractsService(activeContractSetClient: ActiveContractSetClient) {

  def search(jwt: domain.JwtPayload, request: domain.GetActiveContractsRequest) = {}

}
