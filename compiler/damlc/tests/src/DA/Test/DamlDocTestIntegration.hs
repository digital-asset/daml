-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlDocTestIntegration (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

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
    scriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")
    defaultMain $ tests damlcPath scriptDar

tests :: FilePath -> FilePath -> TestTree
tests damlcPath scriptDar = testGroup "doctest integration tests"
    [ testCase "failing doctest" $
          withTempDir $ \tmpDir -> do
              let f = tmpDir </> "Main.daml"
              writeFileUTF8 f $ unlines
                  [ "module Main where"
                  , "-- | add"
                  , "-- >>> add 1 1"
                  , "-- 2"
                  , "add : Int -> Int -> Int"
                  , "add x y = 0"
                  ]
              -- NOTE (MK) We need to change the working directory
              -- since the generated files end up in .daml/generated which
              -- is otherwise identical between the two tests.
              (exit, stdout, stderr) <-
                  readCreateProcessWithExitCode (docTestProc tmpDir f) ""
              assertBool ("error in: " <> stderr) ("expected 0 == 2" `isInfixOf` stderr)
              stdout @?= ""
              assertEqual "exit code" (ExitFailure 1) exit
    , testCase "succeeding doctest" $
          withTempDir $ \tmpDir -> do
              let f = tmpDir </> "Main.daml"
              writeFileUTF8 f $ unlines
                  [ "module Main where"
                  , "-- | add"
                  , "-- >>> add 1 1"
                  , "-- 2"
                  , "add : Int -> Int -> Int"
                  , "add x y = x + y"
                  ]
              (exit, stdout, stderr) <-
                  readCreateProcessWithExitCode (docTestProc tmpDir f) ""
              stdout @?= ""
              stderr @?= ""
              assertEqual "exit code" ExitSuccess exit
    ]
  where
    docTestProc dir f =
        let p = proc damlcPath
                [ "doctest"
                , "--script-lib"
                , scriptDar
                , f
                ]
        in p { cwd = Just dir }
