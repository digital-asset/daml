// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

object IndexConfiguration {

  val DefaultEventsPageSize: Int = 1000
  val DefaultEventsProcessingParallelism: Int = 8
  val DefaultAcsIdPageSize: Int = 20000
  val DefaultAcsIdFetchingParallelism: Int = 2
  val DefaultAcsContractFetchingParallelism: Int = 2
  val DefaultAcsGlobalParallelism: Int = 10
  val DefaultAcsIdQueueLimit: Int = 10000000
}
