-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Test.TastyMain (main) where

import qualified DA.Daml.GHC.Damldoc.Tests as Damldoc
import qualified DA.Daml.GHC.Damldoc.Render.Tests as Render

import qualified Test.Tasty.Extended as Tasty

main :: IO ()
main = Tasty.deterministicMain =<< allTests

allTests :: IO Tasty.TestTree
allTests = Tasty.testGroup "All DAML GHC tests using Tasty" <$> sequence
  [ Damldoc.mkTestTree
  , Render.mkTestTree
  ]
