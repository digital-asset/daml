-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlDocTestIntegration (main) where

import DA.Bazel.Runfiles
import Data.List
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = do
    damlcPath <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests damlcPath

tests :: FilePath -> TestTree
tests damlcPath = testGroup "doctest integration tests"
    [ testCase "failing doctest" $
          withTempDir $ \tmpDir -> do
              let f = tmpDir </> "Main.daml"
              writeFileUTF8 f $ unlines
                  [ "daml 1.2"
                  , "module Main where"
                  , "-- | add"
                  , "-- >>> add 1 1"
                  , "-- 2"
                  , "add : Int -> Int -> Int"
                  , "add x y = 0"
                  ]
              (exit, stdout, _stderr) <- readProcessWithExitCode damlcPath ["doctest", f] ""
              exit @?= ExitFailure 1
              assertBool "doctest error" ("expected 0 === 2" `isInfixOf` stdout)
    , testCase "succeeding doctest" $
          withTempDir $ \tmpDir -> do
              let f = tmpDir </> "Main.daml"
              writeFileUTF8 f $ unlines
                  [ "daml 1.2"
                  , "module Main where"
                  , "-- | add"
                  , "-- >>> add 1 1"
                  , "-- 2"
                  , "add : Int -> Int -> Int"
                  , "add x y = x + y"
                  ]
              (exit, _stdout, _stderr) <- readProcessWithExitCode damlcPath ["doctest", f] ""
              exit @?= ExitSuccess
    ]

