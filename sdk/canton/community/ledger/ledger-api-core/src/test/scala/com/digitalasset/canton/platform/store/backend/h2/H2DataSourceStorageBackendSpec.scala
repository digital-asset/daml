// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class H2DataSourceStorageBackendSpec extends AnyWordSpec with Matchers {

  "H2StorageBackend" should {
    "extractUserPasswordAndRemoveFromUrl" should {

      "strip user from url with user" in {
        H2DataSourceStorageBackend.extractUserPasswordAndRemoveFromUrl(
          "url;user=harry"
        ) shouldBe (("url", Some("harry"), None))
      }

      "strip user from url with user in the middle" in {
        H2DataSourceStorageBackend.extractUserPasswordAndRemoveFromUrl(
          "url;user=harry;password=weak"
        ) shouldBe (("url", Some("harry"), Some("weak")))
      }

      "only strip password if user absent" in {
        H2DataSourceStorageBackend.extractUserPasswordAndRemoveFromUrl(
          "url;password=weak"
        ) shouldBe (("url", None, Some("weak")))
      }

      "not touch other properties" in {
        H2DataSourceStorageBackend.extractUserPasswordAndRemoveFromUrl(
          "url;alpha=1;beta=2;gamma=3"
        ) shouldBe (("url;alpha=1;beta=2;gamma=3", None, None))
      }

      "match upper-case user and password keys" in {
        H2DataSourceStorageBackend.extractUserPasswordAndRemoveFromUrl(
          "url;USER=sally;PASSWORD=supersafe"
        ) shouldBe (("url", Some("sally"), Some("supersafe")))
      }

      "match mixed-case user and password keys" in {
        H2DataSourceStorageBackend.extractUserPasswordAndRemoveFromUrl(
          "url;User=sally;Password=supersafe"
        ) shouldBe (("url", Some("sally"), Some("supersafe")))
      }
    }
  }
}
