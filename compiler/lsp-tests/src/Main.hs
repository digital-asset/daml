-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
module Main (main) where

import DA.Bazel.Runfiles
import qualified Data.Text as T
import Language.Haskell.LSP.Types
import System.FilePath
import System.Info.Extra
import System.IO.Extra
import Test.Tasty
import Test.Tasty.HUnit
import System.Environment.Blank

import Daml.Lsp.Test.Util

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlcPath <- locateRunfiles $
        mainWorkspace </> "daml-foundations" </> "daml-tools" </>
        "da-hs-damlc-app" </> "da-hs-damlc-app"
    let run s = withTempDir $ \dir -> runSessionWithConfig conf (damlcPath <> " ide --scenarios=no") fullCaps dir s
        runScenarios s
            -- We are currently seeing issues with GRPC FFI calls which make everything
            -- that uses the scenario service extremely flaky and forces us to disable it on
            -- CI. Once https://github.com/digital-asset/daml/issues/1354 is fixed we can
            -- also run scenario tests on Windows.
            | isWindows = pure ()
            | otherwise = withTempDir $ \dir -> runSessionWithConfig conf (damlcPath <> " ide --scenarios=yes") fullCaps dir s
    defaultMain $ testGroup "LSP"
        [ diagnosticTests run runScenarios
        ]
    where
        conf = defaultConfig
            -- If you uncomment this you can see all messages
            -- which can be quite useful for debugging.
            -- { logMessages = True, logColor = False, logStdErr = True }

diagnosticTests
    :: (forall a. Session a -> IO a)
    -> (Session () -> IO ())
    -> TestTree
diagnosticTests run runScenarios = testGroup "diagnostics"
    [ testCase "diagnostics disappear after error is fixed" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f 1"
              ]
          expectDiagnostics [("Test.daml", [(DsError, (2, 0), "Parse error")])]
          replaceDoc test $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f = ()"
              ]
          expectDiagnostics [("Test.daml", [])]
          closeDoc test
    , testCase "diagnostics appear after introducing an error" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f = ()"
              ]
          replaceDoc test $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f 1"
              ]
          expectDiagnostics [("Test.daml", [(DsError, (2, 0), "Parse error")])]
          closeDoc test
    , testCase "failed name resolution" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "add : Int -> Int -> Int"
              , "add a b = ab + b"
              , "succ : Int -> Int"
              , "succ a = abdd 1 a"
              ]
          expectDiagnostics
              [ ( "Main.daml"
                , [ (DsError, (3, 10), "Variable not in scope: ab")
                  , (DsError, (5, 9), "Variable not in scope: abdd")
                  ]
                )
              ]
          closeDoc main'
    , testCase "import cycle" $ run $ do
          a <- openDoc' "A.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module A where"
              , "import B"
              ]
          b <- openDoc' "B.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module B where"
              , "import A"
              ]
          expectDiagnostics
              [ ( "A.daml"
                , [(DsError, (2, 7), "Cyclic module dependency between A, B")]
                )
              , ( "B.daml"
                , [(DsError, (2, 7), "Cyclic module dependency between A, B")]
                )
              ]
          closeDoc b
          closeDoc a
    , testCase "import error" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "import Oops"
              ]
          expectDiagnostics
              [("Main.daml", [(DsError, (2, 7), "Could not find module 'Oops'")])]
          closeDoc main'
    , testCase "multi module funny" $ run $ do
          libsC <- openDoc' "Libs/C.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Libs.C where"
              ]
          libsB <- openDoc' "Libs/B.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Libs.B where"
              , "import Libs.C"
              ]
          libsA <- openDoc' "Libs/A.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Libs.A where"
              , "import C"
              ]
          expectDiagnostics
              [ ( "Libs/A.daml"
                , [(DsError, (2, 7), "Could not find module 'C'")]
                )
              , ( "Libs/B.daml"
                , [(DsWarning, (2, 0), "import of 'Libs.C' is redundant")]
                )
              ]
          closeDoc libsA
          closeDoc libsB
          closeDoc libsC
    , testCase "parse error" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "f 1"
              ]
          expectDiagnostics [("Test.daml", [(DsError, (2, 0), "Parse error")])]
          closeDoc test
    , testCase "type error" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "oops = 1 + \"foo\""
              ]
          expectDiagnostics
              [ ( "Main.daml"
                , [ ( DsError
                    , (2, 11)
                    , "Couldn't match expected type 'Int' with actual type 'Text'"
                    )
                  ]
                )
              ]
          closeDoc main'
    , testCase "scenario error" $ runScenarios $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "template Agree with p1 : Party; p2 : Party where"
              , "  signatory [p1, p2]"
              , "myScenario = scenario do"
              , "  foo <- getParty \"Foo\""
              , "  alice <- getParty \"Alice\""
              , "  submit foo $ create $ Agree with p1 = foo, p2 = alice"
              ]
          expectDiagnostics
              [ ( "Main.daml"
                , [(DsError, (4, 0), "missing authorization from 'Alice'")]
                )
              ]
          closeDoc main'
    ]

