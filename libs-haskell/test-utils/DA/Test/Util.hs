-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Test utils
module DA.Test.Util (
    standardizeQuotes,
    standardizeEoL,
    assertInfixOf
) where

import Data.List.Extra (isInfixOf)
import qualified Data.Text as T
import Test.Tasty.HUnit

standardizeQuotes :: T.Text -> T.Text
standardizeQuotes msg = let
        repl '‘' = '\''
        repl '’' = '\''
        repl '`' = '\''
        repl  c   = c
    in  T.map repl msg

standardizeEoL :: T.Text -> T.Text
standardizeEoL = T.replace (T.singleton '\r') T.empty

assertInfixOf :: String -> String -> Assertion
assertInfixOf needle haystack = assertBool ("Expected " <> show needle <> " in output but but got " <> show haystack) (needle `isInfixOf` haystack)
