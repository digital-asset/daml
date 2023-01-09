-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Util
    ( assertFileExists
    , assertFileDoesNotExist
    , assertInfixOf
    ) where

import Data.List
import System.Directory
import Test.Tasty.HUnit

assertFileExists :: FilePath -> IO ()
assertFileExists file = doesFileExist file >>= assertBool (file ++ " was expected to exist, but does not exist")

assertFileDoesNotExist :: FilePath -> IO ()
assertFileDoesNotExist file = doesFileExist file >>= assertBool (file ++ " was expected to not exist, but does exist") . not

assertInfixOf :: String -> String -> Assertion
assertInfixOf needle haystack = assertBool ("Expected " <> show needle <> " in output but got " <> show haystack) (needle `isInfixOf` haystack)
