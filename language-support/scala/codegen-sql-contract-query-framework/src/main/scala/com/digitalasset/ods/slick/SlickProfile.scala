// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick

import slick.jdbc.JdbcProfile

trait SlickProfile {
  val profile: JdbcProfile
}

trait SlickH2Profile extends SlickProfile {
  val profile: JdbcProfile = slick.jdbc.H2Profile
}

trait SlickPostgresProfile extends SlickProfile {
  val profile: JdbcProfile = slick.jdbc.PostgresProfile
}

object SlickProfile {
  val supportedProfiles = List(slick.jdbc.H2Profile, slick.jdbc.PostgresProfile)
}
