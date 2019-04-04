-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module Data.Aeson.Extra.Merge.Custom.Test
    (customMergeTests
    ) where

import qualified Data.ByteString.Char8         as BS
import           Data.Monoid                   ((<>))
import qualified Data.Yaml                     as Yaml
import           Data.Aeson.Extra.Merge.Custom (customMerge)
import           Test.Tasty
import           Test.Tasty.HUnit

customMergeTests :: TestTree
customMergeTests = testGroup "Yaml customMerge"
    [ customMergeTest "[1, 2, 3]" "['a']" "['a']"
    , customMergeTest "[1, 2, 3]" "[]"    "[]"
    ]

customMergeTest :: String   -- ^ the first argument of customMerge
                -> String   -- ^ the second argument of customMerge
                -> String   -- ^ the expected result of customMerge
                -> TestTree
customMergeTest fstArg sndArg expectedArg =
    testCase msg $ do
        fstValue      <- parseYaml fstArg
        sndValue      <- parseYaml sndArg
        expectedValue <- parseYaml expectedArg
        let mergedValue = fstValue `customMerge` sndValue
        assertEqual "" expectedValue mergedValue
  where
    msg = "customMerge should merge '" <> fstArg <> "' and '" <>
          sndArg <> "' into '" <> expectedArg <> "'"

parseYaml :: String -> IO Yaml.Value
parseYaml = either (assertFailure . show) return . Yaml.decodeEither' . BS.pack
