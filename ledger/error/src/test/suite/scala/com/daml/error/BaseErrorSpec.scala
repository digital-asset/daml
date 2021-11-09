// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BaseErrorSpec extends AnyWordSpec with Matchers {
  "class field name to context key conversion" should {
    "convert camel-cased names to underscore" in {
      BaseError.contextKeyForFieldName("word") should be("word")
      BaseError.contextKeyForFieldName("oneWord") should be("one_word")
      BaseError.contextKeyForFieldName("someMoreWords") should be("some_more_words")
    }

    "only insert one underscore in case of multiple consecutive uppercase characters" in {
      BaseError.contextKeyForFieldName("anABC") should be("an_aBC")
      BaseError.contextKeyForFieldName("anABCdef") should be("an_aBCdef")
      BaseError.contextKeyForFieldName("ABCdefGhi") should be("ABCdef_ghi")
    }

    "skip converting names without an underscore" in {
      BaseError.contextKeyForFieldName("a_b") should be("a_b")
      BaseError.contextKeyForFieldName("a_b_c") should be("a_b_c")
    }
  }
}
