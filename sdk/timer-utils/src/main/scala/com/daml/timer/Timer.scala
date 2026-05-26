// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// taken from:
// https://github.com/digital-asset/daml/blob/pre-shuffle/sdk/libs-scala/timer-utils/src/main/scala/com/daml/timer/Timer.scala

package com.daml.timer

private[timer] object Timer extends java.util.Timer("timer-utils", true)
