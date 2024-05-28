// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.networking.UrlValidator.InvalidScheme
import org.scalatest.wordspec.AnyWordSpec

class UrlValidatorTest extends AnyWordSpec with BaseTest {

  "the url validator" should {

    "accept valid http urls" in {
      forAll(
        Table(
          "url",
          "http://example.com",
          "http://example.com/url",
          "http://example.com:80/url",
        )
      ) { url =>
        UrlValidator.validate(url).value.toString shouldBe url
      }
    }

    "accept valid https urls" in {
      forAll(
        Table(
          "url",
          "https://example.com",
          "https://example.com/url",
          "https://example.com:443/url",
        )
      ) { url =>
        UrlValidator.validate(url).value.toString shouldBe url
      }
    }

    "reject urls without a scheme" in {
      UrlValidator.validate("example.com").left.value shouldBe InvalidScheme(null)
    }

    "reject invalid urls" in {
      forAll(
        Table(
          "url",
          "http://",
          "https://",
          "http:/example.com",
          "https:/example.com",
          "https://:443",
          "http://:80",
          ":443",
        )
      ) { url =>
        leftOrFail(UrlValidator.validate(url))(s"expected invalid url $url")
      }
    }

  }

}
