-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
module Main (main) where

import Control.Lens hiding (List)
import Control.Monad (forM, forM_, void)
import Control.Monad.IO.Class
import DA.Bazel.Runfiles
import Data.Aeson (toJSON)
import qualified Data.Text as T
import Language.Haskell.LSP.Types
import Language.Haskell.LSP.Types.Lens
import Network.URI
import System.Environment.Blank
import System.FilePath
import System.Info.Extra
import System.IO.Extra
import Test.Tasty
import Test.Tasty.HUnit

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
        , requestTests run runScenarios
        , scenarioTests runScenarios
        , stressTests run runScenarios
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
    , testCase "percent encoding does not matter" $ run $ do
          Just uri <- parseURI . T.unpack . getUri <$> getDocUri "Test.daml"
          let weirdUri = Uri $ T.pack $ "file://" <> escapeURIString (== '/') (uriPath uri)
          let item = TextDocumentItem weirdUri (T.pack damlId) 0 $ T.unlines
                  [ "daml 1.2"
                  , "module Test where"
                  , "f = ()"
                  ]
          sendNotification TextDocumentDidOpen (DidOpenTextDocumentParams item)
          let test = TextDocumentIdentifier weirdUri
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
          let aContent = T.unlines
                  [ "daml 1.2"
                  , "module A where"
                  , "import B"
                  ]
              bContent = T.unlines
                  [ "daml 1.2"
                  , "module B where"
                  , "import A"
                  ]
          [a, b] <- openDocs damlId [("A.daml", aContent), ("B.daml", bContent)]
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


requestTests
    :: (forall a. Session a -> IO a)
    -> (Session () -> IO ())
    -> TestTree
requestTests run _runScenarios = testGroup "requests"
    [ testCase "code-lenses" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "single = scenario do"
              , "  assert (True == True)"
              ]
          lenses <- getCodeLenses main'
          Just escapedFp <- pure $ escapeURIString isUnescapedInURIComponent <$> uriToFilePath (main' ^. uri)
          liftIO $ lenses @?=
              [ CodeLens
                    { _range = Range (Position 2 0) (Position 2 6)
                    , _command = Just $ Command
                          { _title = "Scenario results"
                          , _command = "daml.showResource"
                          , _arguments = Just $ List
                              [ "Scenario: single"
                              ,  toJSON $ "daml://compiler?file=" <> escapedFp <> "&top-level-decl=single"
                              ]
                          }
                    , _xdata = Nothing
                    }
              ]

          closeDoc main'
    , testCase "type on hover: name" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "add : Int -> Int -> Int"
              , "add a b = a + b"
              , "template DoStuff with party : Party where"
              , "  signatory party"
              , "  controller party can"
              , "    ChooseNumber : Int"
              , "      with number : Int"
              , "        do pure (add 5 number)"
              ]
          Just fp <- pure $ uriToFilePath (main' ^. uri)
          r <- getHover main' (Position 9 19)
          liftIO $ r @?= Just Hover
              { _contents = HoverContents $ MarkupContent MkMarkdown $ T.unlines
                    [ "```daml\nMain.add"
                    , "  : Int -> Int -> Int"
                    , "```"
                    , "*\t*\t*"
                    , "**Defined at " <> T.pack fp <> ":4:1**"
                    ]
              , _range = Just $ Range (Position 10 17) (Position 10 20)
              }
          closeDoc main'

    , testCase "type on hover: literal" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "simple arg1 = let xlocal = 1.0 in xlocal + arg1"
              ]
          r <- getHover main' (Position 2 27)
          liftIO $ r @?= Just Hover
              { _contents = HoverContents $ MarkupContent MkMarkdown "```daml\n: Decimal\n```\n"
              , _range = Just $ Range (Position 3 27) (Position 3 30)
              }
          closeDoc main'
    , testCase "definition" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Test where"
              , "answerFromTest = 42"
              ]
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "import Test"
              , "bar = answerFromTest"
              , "foo thisIsAParam = thisIsAParam <> \" concatenated with a Text.\""
              , "aFunction = let letParam = 10 in letParam"
              , "template SomeTemplate"
              , "  with"
              , "    p : Party"
              , "    c : Party"
              , "  where"
              , "    signatory p"
              , "    controller c can"
              , "      SomeChoice : ()"
              , "        do let b = bar"
              , "           pure ()"
              ]
          -- thisIsAParam
          locs <- getDefinitions main' (Position 4 24)
          liftIO $ locs @?= [Location (main' ^. uri) (Range (Position 4 4) (Position 4 16))]
          -- letParam
          locs <- getDefinitions main' (Position 5 37)
          liftIO $ locs @?= [Location (main' ^. uri) (Range (Position 5 16) (Position 5 24))]
          -- import Test
          locs <- getDefinitions main' (Position 2 10)
          liftIO $ locs @?= [Location (test ^. uri) (Range (Position 0 0) (Position 0 0))]
          -- use of `bar` in template
          locs <- getDefinitions main' (Position 14 20)
          liftIO $ locs @?= [Location (main' ^. uri) (Range (Position 3 0) (Position 3 3))]
          -- answerFromTest
          locs <- getDefinitions main' (Position 3 8)
          liftIO $ locs @?= [Location (test ^. uri) (Range (Position 2 0) (Position 2 14))]
          closeDoc main'
          closeDoc test
    ]

scenarioTests :: (Session () -> IO ()) -> TestTree
scenarioTests run = testGroup "scenarios"
    [ testCase "opening codelens produces a notification" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "main = scenario $ assert (True == True)"
              ]
          lenses <- getCodeLenses main'
          uri <- scenarioUri "Main.daml" "main"
          liftIO $ lenses @?=
              [ CodeLens
                    { _range = Range (Position 2 0) (Position 2 4)
                    , _command = Just $ Command
                          { _title = "Scenario results"
                          , _command = "daml.showResource"
                          , _arguments = Just $ List
                              [ "Scenario: main"
                              ,  toJSON uri
                              ]
                          }
                    , _xdata = Nothing
                    }
              ]
          mainScenario <- openScenario "Main.daml" "main"
          _ <- waitForScenarioDidChange
          closeDoc mainScenario
    , testCase "scenario ok" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "main = scenario $ pure \"ok\""
              ]
          scenario <- openScenario "Main.daml" "main"
          expectScenarioContent "Return value: &quot;ok&quot"
          closeDoc scenario
          closeDoc main'
    , testCase "spaces in path" $ run $ do
          main' <- openDoc' "spaces in path/Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "main = scenario $ pure \"ok\""
              ]
          scenario <- openScenario "spaces in path/Main.daml" "main"
          expectScenarioContent "Return value: &quot;ok&quot"
          closeDoc scenario
          closeDoc main'
    ]


-- | Do extreme things to the compiler service.
stressTests
  :: (forall a. Session a -> IO a)
  -> (Session () -> IO ())
  -> TestTree
stressTests run _runScenarios = testGroup "Stress tests"
  [ testCase "Modify a file 2000 times" $ run $ do
        let fooValue :: Int -> T.Text
            fooValue i = T.pack (show (i `div` 2))
                      <> if i `mod` 2 == 0 then "" else ".5"
            fooContent i = T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = " <> fooValue i
                ]
        foo <- openDoc' "Foo.daml" damlId $ fooContent 0
        forM_ [1 .. 999] $ \i ->
            replaceDoc foo $ fooContent i
        expectDiagnostics [("Foo.daml", [(DsError, (3, 6), "Couldn't match expected type")])]
        forM_ [1000 .. 2000] $ \i ->
            replaceDoc foo $ fooContent i
        expectDiagnostics [("Foo.daml", [])]
        closeDoc foo
  , testCase "Set 10 files of interest" $ run $ do
        foos <- forM [1 .. 10 :: Int] $ \i ->
            makeModule ("Foo" ++ show i) ["foo  10"]
        expectDiagnostics
            [ ("Foo" ++ show i ++ ".daml", [(DsError, (2, 0), "Parse error")])
            | i <- [1 .. 10 :: Int]
            ]
        forM_ (zip [1 .. 10 :: Int] foos) $ \(i, foo) ->
            replaceDoc foo $ moduleContent ("Foo" ++ show i) ["foo = 10"]
        withTimeout 30 $
            expectDiagnostics [("Foo" ++ show i ++ ".daml", []) | i <- [1 .. 10 :: Int]]
        mapM_ closeDoc foos
  , testCase "Type check 100-deep module chain" $ run $ do
        -- The idea for this test is we have 101 modules named Foo0 through Foo100.
        -- Foo0 imports Foo1, which imports Foo2, which imports Foo3, and so on to Foo100.
        -- Each FooN has a definition fooN that depends on fooN+1, except Foo100.
        -- But the type of foo0 doesn't match the type of foo100. So we expect a type error.
        -- Then we modify the type of foo0 to clear the type error.
        foo0 <- makeModule "Foo0"
            [ "import Foo1"
            , "foo0 : Int"
            , "foo0 = foo1"
            ]
        foos <- forM [1 .. 99 :: Int] $ \i ->
            makeModule ("Foo" ++ show i)
                [ "import Foo" <> T.pack (show (i+1))
                , "foo" <> T.pack (show i) <> " = foo" <> T.pack (show (i+1))
                ]
        foo100 <- makeModule "Foo100"
            [ "foo100 : Bool"
            , "foo100 = False"
            ]
        withTimeout 30 $ do
            expectDiagnostics [("Foo0.daml", [(DsError, (4, 7), "Couldn't match expected type")])]
            void $ replaceDoc foo0 $ moduleContent "Foo0"
                [ "import Foo1"
                , "foo0 : Bool"
                , "foo0 = foo1"
                ]
            expectDiagnostics [("Foo0.daml", [])]
        mapM_ closeDoc $ foo0:foo100:foos
  ]
  where
    moduleContent :: String -> [T.Text] -> T.Text
    moduleContent name lines = T.unlines $
        [ "daml 1.2"
        , "module " <> T.pack name <> " where"
        ] ++ lines
    makeModule :: String -> [T.Text] -> Session TextDocumentIdentifier
    makeModule name lines = openDoc' (name ++ ".daml") damlId $
        moduleContent name lines
