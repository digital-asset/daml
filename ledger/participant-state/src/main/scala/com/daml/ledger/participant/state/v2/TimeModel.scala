// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.time.{Duration, Instant}

trait TimeModel {

  def minTransactionLatency: Duration

  def futureAcceptanceWindow: Duration

  def maxClockSkew: Duration

  def minTtl: Duration

  def maxTtl: Duration

}

trait TimeModelChecker {

  /**
    * Validates that the given ledger effective time is within an acceptable time window of the current system time.
    *
    * @param currentTime              the current time
    * @param givenLedgerEffectiveTime The ledger effective time to validate.
    * @param givenMaximumRecordTime   The maximum record time to validate.
    * @return true if successful
    */
  def checkLet(
      currentTime: Instant,
      givenLedgerEffectiveTime: Instant,
      givenMaximumRecordTime: Instant): Boolean

  /**
    * Validates that the ttl of the given times is within bounds.
    * The ttl of a command is defined as the duration between
    * the ledger effective time and maximum record time.
    *
    * @param givenLedgerEffectiveTime The given ledger effective time.
    * @param givenMaximumRecordTime   The given maximum record time.
    * @return true if successful
    */
  def checkTtl(givenLedgerEffectiveTime: Instant, givenMaximumRecordTime: Instant): Boolean
}
