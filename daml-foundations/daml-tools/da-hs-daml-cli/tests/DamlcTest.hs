-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module DamlcTest
   ( main
   ) where

import Control.Exception
import qualified Data.Text.Extended as T
import System.Environment.Blank
import System.IO.Extra
import System.Exit
import Test.Tasty
import Test.Tasty.HUnit

import qualified DA.Cli.Damlc.Test as Damlc
import DA.Daml.GHC.Compiler.Options
import Development.IDE.Types.Diagnostics

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    defaultMain tests

-- execTest will call mkOptions internally. Since each call to mkOptions
-- appends the LF version to the package db paths, it is important that we use
-- defaultOptions instead of defaultOptionsIO since the version suffix is otherwise
-- appended twice.
opts :: Options
opts = defaultOptions Nothing

tests :: TestTree
tests = testGroup
    "damlc test"
    [ testCase "Non-existent file" $ do
        shouldThrow (Damlc.execTest ["foobar"] (Damlc.UseColor False) Nothing opts)
    , testCase "File with compile error" $ do
        withTempFile $ \path -> do
            T.writeFileUtf8 path $ T.unlines
              [ "daml 1.2"
              , "module Foo where"
              , "abc"
              ]
            shouldThrowExitFailure (Damlc.execTest [toNormalizedFilePath path] (Damlc.UseColor False) Nothing opts)
    , testCase "File with failing scenario" $ do
        withTempFile $ \path -> do
            T.writeFileUtf8 path $ T.unlines
              [ "daml 1.2"
              , "module Foo where"
              , "x = scenario $ assert False"
              ]
            shouldThrowExitFailure (Damlc.execTest [toNormalizedFilePath path] (Damlc.UseColor False) Nothing opts)
    ]

shouldThrowExitFailure :: IO () -> IO ()
shouldThrowExitFailure a = do
    r <- try a
    case r of
        Left (ExitFailure _) -> pure ()
        _ -> assertFailure "Expected program to fail with non-zero exit code."

shouldThrow :: IO () -> IO ()
shouldThrow a = do
    r <- try a
    case r of
        Left (_ :: SomeException) -> pure ()
        Right _ -> assertFailure "Expected program to throw an IOException."
