-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}
module Main (main) where

import Control.Applicative.Combinators
import Control.Lens hiding (List)
import Control.Monad
import Control.Monad.IO.Class
import DA.Bazel.Runfiles
import Data.Aeson (toJSON)
import Data.Char (toLower)
import Data.Foldable (toList)
import Data.List.Extra
import Data.Maybe
import qualified Data.Text as T
import qualified Language.Haskell.LSP.Test as LspTest
import Language.Haskell.LSP.Types
import Language.Haskell.LSP.Types.Capabilities
import Language.Haskell.LSP.Types.Lens
import Network.URI
import SdkVersion
import System.Directory
import System.Environment.Blank
import System.FilePath
import System.Info.Extra
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import qualified Data.Aeson as Aeson
import DA.Daml.Lsp.Test.Util
import qualified Language.Haskell.LSP.Test as LSP

fullCaps' :: ClientCapabilities
fullCaps' = fullCaps { _window = Just $ WindowClientCapabilities $ Just True }

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlcPath <- locateRunfiles $
        mainWorkspace </> "compiler" </> "damlc" </> exe "damlc"
    let run s = withTempDir $ \dir -> runSessionWithConfig conf (damlcPath <> " ide --scenarios=no") fullCaps' dir s
        runScenarios s
            -- We are currently seeing issues with GRPC FFI calls which make everything
            -- that uses the scenario service extremely flaky and forces us to disable it on
            -- CI. Once https://github.com/digital-asset/daml/issues/1354 is fixed we can
            -- also run scenario tests on Windows.
            | isWindows = pure ()
            | otherwise = withTempDir $ \dir -> runSessionWithConfig conf (damlcPath <> " ide --scenarios=yes") fullCaps' dir s
    defaultMain $ testGroup "LSP"
        [ diagnosticTests run runScenarios
        , requestTests run runScenarios
        , scenarioTests runScenarios
        , stressTests run runScenarios
        , executeCommandTests run runScenarios
        , regressionTests run runScenarios
        , multiPackageTests damlcPath
        , completionTests run runScenarios
        ]

conf :: SessionConfig
conf = defaultConfig
    -- If you uncomment this you can see all messages
    -- which can be quite useful for debugging.
    { logStdErr = True }
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
    , testCase "lower-case drive" $ run $ do
          let aContent = T.unlines
                [ "daml 1.2"
                , "module A.A where"
                , "import A.B ()"
                ]
              bContent = T.unlines
                [ "daml 1.2"
                , "module A.B where"
                , "import DA.List"
                ]
          uriB <- getDocUri "A/B.daml"
          Just pathB <- pure $ uriToFilePath uriB
          uriB <- pure $
              let (drive, suffix) = splitDrive pathB
              in filePathToUri (joinDrive (map toLower drive ) suffix)
          liftIO $ createDirectoryIfMissing True (takeDirectory pathB)
          liftIO $ writeFileUTF8 pathB $ T.unpack bContent
          uriA <- getDocUri "A/A.daml"
          Just pathA <- pure $ uriToFilePath uriA
          uriA <- pure $
              let (drive, suffix) = splitDrive pathA
              in filePathToUri (joinDrive (map toLower drive ) suffix)
          let itemA = TextDocumentItem uriA "daml" 0 aContent
          let a = TextDocumentIdentifier uriA
          sendNotification TextDocumentDidOpen (DidOpenTextDocumentParams itemA)
          diagsNot <- skipManyTill anyMessage LspTest.message :: Session PublishDiagnosticsNotification
          let fileUri = diagsNot ^. params . uri
          -- Check that if we put a lower-case drive in for A.A
          -- the diagnostics for A.B will also be lower-case.
          liftIO $ fileUri @?= uriB
          let msg = diagsNot ^?! params . diagnostics . to (\(List xs) -> xs) . _head . message
          liftIO $ unless ("redundant" `T.isInfixOf` msg) $
              assertFailure ("Expected redundant import but got " <> T.unpack msg)
          closeDoc a
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
              , "succ = abdd 1"
              ]
          expectDiagnostics
              [ ( "Main.daml"
                , [ (DsError, (3, 10), "Variable not in scope: ab")
                  , (DsError, (5, 7), "Variable not in scope: abdd")
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
    , testCase "stale code-lenses" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "daml 1.2"
              , "module Main where"
              , "single = scenario do"
              , "  assert True"
              ]
          lenses <- getCodeLenses main'
          Just escapedFp <- pure $ escapeURIString isUnescapedInURIComponent <$> uriToFilePath (main' ^. uri)
          let codeLens range =
                  CodeLens
                    { _range = range
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
          liftIO $ lenses @?= [codeLens (Range (Position 2 0) (Position 2 6))]
          changeDoc main' [TextDocumentContentChangeEvent (Just (Range (Position 3 23) (Position 3 23))) Nothing "+"]
          expectDiagnostics [("Main.daml", [(DsError, (4, 0), "Parse error")])]
          lenses <- getCodeLenses main'
          liftIO $ lenses @?= [codeLens (Range (Position 2 0) (Position 2 6))]
          -- Shift code lenses down
          changeDoc main' [TextDocumentContentChangeEvent (Just (Range (Position 1 0) (Position 1 0))) Nothing "\n\n"]
          lenses <- getCodeLenses main'
          liftIO $ lenses @?= [codeLens (Range (Position 4 0) (Position 4 6))]
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
                    [ "```daml"
                    , "Main.add"
                    , ": Int -> Int -> Int"
                    , "```"
                    , "*\t*\t*"
                    , "*Defined at " <> T.pack fp <> ":4:1*"
                    ]
              , _range = Just $ Range (Position 9 17) (Position 9 20)
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
              { _contents = HoverContents $ MarkupContent MkMarkdown $ T.unlines
                    [ "```daml"
                    , "1.0"
                    , ": NumericScale n"
                    , "=> Numeric n"
                    , "```"
                    , "*\t*\t*"
                    ]
              , _range = Just $ Range (Position 2 27) (Position 2 30)
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

executeCommandTests :: (forall a. Session a -> IO a) -> (Session () -> IO ()) -> TestTree
executeCommandTests run _ = testGroup "execute command"
    [ testCase "execute commands" $ run $ do
        main' <- openDoc' "Coin.daml" damlId $ T.unlines
            [ "daml 1.2"
            , "module Coin where"
            , "template Coin"
            , "  with"
            , "    owner : Party"
            , "  where"
            , "    signatory owner"
            , "    controller owner can"
            , "      Delete : ()"
            , "        do return ()"
            ]
        Just escapedFp <- pure $ uriToFilePath (main' ^. uri)
        actualDotString :: ExecuteCommandResponse <- LSP.request WorkspaceExecuteCommand $ ExecuteCommandParams
           "daml/damlVisualize"  (Just (List [Aeson.String $ T.pack escapedFp])) Nothing
        let expectedDotString = "digraph G {\ncompound=true;\nrankdir=LR;\nsubgraph cluster_Coin{\nn0[label=Create][color=green]; \nn1[label=Archive][color=red]; \nn2[label=Delete][color=red]; \nlabel=<<table align = \"left\" border=\"0\" cellborder=\"0\" cellspacing=\"1\">\n<tr><td align=\"center\"><b>Coin</b></td></tr><tr><td align=\"left\">owner</td></tr> \n</table>>;color=blue\n}\n}\n"
        liftIO $ assertEqual "Visulization command" (Just expectedDotString) (_result actualDotString)
        closeDoc main'
    , testCase "Invalid commands result in error"  $ run $ do
        main' <- openDoc' "Empty.daml" damlId $ T.unlines
            [ "daml 1.2"
            , "module Empty where"
            ]
        Just escapedFp <- pure $ uriToFilePath (main' ^. uri)
        actualDotString :: ExecuteCommandResponse <- LSP.request WorkspaceExecuteCommand $ ExecuteCommandParams
           "daml/NoCommand"  (Just (List [Aeson.String $ T.pack escapedFp])) Nothing
        liftIO $ _result actualDotString @?= Nothing
        liftIO $ assertBool "Expected response error but got Nothing" (isJust $ _error actualDotString)
        closeDoc main'
    , testCase "Visualization command with no arguments" $ run $ do
        actualDotString :: ExecuteCommandResponse <- LSP.request WorkspaceExecuteCommand $ ExecuteCommandParams
           "daml/damlVisualize"  Nothing Nothing
        liftIO $ _result actualDotString @?= Nothing
        liftIO $ assertBool "Expected response error but got Nothing" (isJust $ _error actualDotString)
    ]

-- | Do extreme things to the compiler service.
stressTests
  :: (forall a. Session a -> IO a)
  -> (Session () -> IO ())
  -> TestTree
stressTests run _runScenarios = testGroup "Stress tests"
  [ testCase "Modify a file 2000 times" $ run $ do

        let fooValue :: Int -> T.Text
            -- Even values should produce empty diagnostics
            -- while odd values will produce a type error.
            fooValue i = T.pack (show (i `div` 2))
                      <> if even i then "" else ".5"
            fooContent i = T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = " <> fooValue i
                ]
            expect :: Int -> Session ()
            expect i = when (odd i) $ do

                -- We do not wait for empty diagnostics on even i since debouncing
                -- causes them to only be emitted after a delay which slows down
                -- the tests.
                -- Depending on debouncing we might first get a message with an empty
                -- set of diagnostics or we might get the error diagnostics directly
                -- if debouncing supresses the empty set of diagnostics.
                diags <-
                    skipManyTill anyMessage $ do
                        m <- LSP.message :: Session PublishDiagnosticsNotification;
                        let diags = toList $ m ^. params . diagnostics
                        guard (notNull diags)
                        pure diags
                liftIO $ unless (length diags == 1) $
                    assertFailure $ "Incorrect number of diagnostics, expected 1 but got " <> show diags
                let msg = head diags ^. message
                liftIO $ assertBool ("Expected type error but got " <> T.unpack msg) $
                    "Couldn't match expected type" `T.isInfixOf` msg

        foo <- openDoc' "Foo.daml" damlId $ fooContent 0
        forM_ [1 .. 2000] $ \i -> do
            replaceDoc foo $ fooContent i
            expect i
        expectDiagnostics [("Foo.daml", [])]
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
        -- To avoid race conditions we send the modules in order 100, 99, … 0 to the
        -- server to make sure that all previous modules are already known.
        foo100 <- makeModule "Foo100"
            [ "foo100 : Bool"
            , "foo100 = False"
            ]
        foos <- forM [99, 98 .. 1 :: Int] $ \i ->
            makeModule ("Foo" ++ show i)
                [ "import Foo" <> T.pack (show (i+1))
                , "foo" <> T.pack (show i) <> " = foo" <> T.pack (show (i+1))
                ]
        foo0 <- makeModule "Foo0"
            [ "import Foo1"
            , "foo0 : Int"
            , "foo0 = foo1"
            ]
        withTimeout 90 $ do
            expectDiagnostics [("Foo0.daml", [(DsError, (4, 7), "Couldn't match expected type")])]
            void $ replaceDoc foo0 $ moduleContent "Foo0"
                [ "import Foo1"
                , "foo0 : Bool"
                , "foo0 = foo1"
                ]
            expectDiagnostics [("Foo0.daml", [])]
        mapM_ closeDoc $ (foo0 : foos) ++ [foo100]
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

regressionTests
    :: (Session () -> IO ())
    -> (Session () -> IO ())
    -> TestTree
regressionTests run _runScenarios = testGroup "regression"
  [ testCase "completion on stale file" $ run $ do
        -- This used to produce "cannot continue after interface file error"
        -- since we used a function from GHCi in ghcide.
        foo <- openDoc' "Foo.daml" damlId $ T.unlines
            [ "{-# OPTIONS_GHC -Wall #-}"
            , "daml 1.2"
            , "module Foo where"
            , "import DA.List"
            , ""
            ]
        expectDiagnostics [("Foo.daml", [(DsWarning, (3,0), "redundant")])]
        completions <- getCompletions foo (Position 3 1)
        liftIO $
            assertBool ("DA.List and DA.Internal.RebindableSyntax should be in " <> show completions) $
            mkModuleCompletion "DA.Internal.RebindableSyntax" `elem` completions &&
            mkModuleCompletion "DA.List" `elem` completions
        changeDoc foo [TextDocumentContentChangeEvent (Just (Range (Position 3 0) (Position 3 1))) Nothing "Syntax"]
        expectDiagnostics [("Foo.daml", [(DsError, (3,0), "Parse error")])]
        completions <- getCompletions foo (Position 3 6)
        liftIO $ completions @?= [mkModuleCompletion "DA.Internal.RebindableSyntax" & detail .~ Nothing]
  ]

completionTests
    :: (Session () -> IO ())
    -> (Session () -> IO ())
    -> TestTree
completionTests run _runScenarios = testGroup "completion"
    [ testCase "type signature" $ run $ do
          foo <- openDoc' "Foo.daml" damlId $ T.unlines
              [ "module Foo where"
              ]
          -- Request completions to ensure that the module has been typechecked
          _ <- getCompletions foo (Position 0 1)
          changeDoc foo
              [ TextDocumentContentChangeEvent Nothing Nothing $ T.unlines
                    [ "module Foo where"
                    , "f : Part"
                    ]
              ]
          completions <- getCompletions foo (Position 1 8)
          liftIO $
              map (set documentation Nothing) completions @?=
              [ mkTypeCompletion "Party"
              , mkTypeCompletion "IsParties"
              ]
    , testCase "with keyword" $ run $ do
          foo <- openDoc' "Foo.daml" damlId $ T.unlines
              [ "module Foo where"
              ]
          -- Request completions to ensure that the module has been typechecked
          _ <- getCompletions foo (Position 0 1)
          changeDoc foo
              [ TextDocumentContentChangeEvent Nothing Nothing $ T.unlines
                    [ "module Foo where"
                    , "f = R wit"
                    ]
              ]
          completions <- getCompletions foo (Position 1 9)
          liftIO $ assertBool ("`with` should be in " <> show completions) $
              mkKeywordCompletion "with" `elem` completions
    , testCase "no prefix" $ run $ do
            foo <- openDoc' "Foo.daml" damlId $ T.unlines
                [ "module Foo where" ]
            completions <- getCompletions foo (Position 0 0)
            -- We just want to verify that this no longer results in a
            -- crash (before haskell-lsp 0.21 it did crash).
            liftIO $ assertBool "Expected completions to be non-empty"
                (not $ null completions)
    ]

defaultCompletion :: T.Text -> CompletionItem
defaultCompletion label = CompletionItem
          { _label = label
          , _kind = Nothing
          , _detail = Nothing
          , _documentation = Nothing
          , _deprecated = Nothing
          , _preselect = Nothing
          , _sortText = Nothing
          , _filterText = Nothing
          , _insertText = Nothing
          , _insertTextFormat = Nothing
          , _textEdit = Nothing
          , _additionalTextEdits = Nothing
          , _commitCharacters = Nothing
          , _command = Nothing
          , _xdata = Nothing
          , _tags = List []
          }

mkTypeCompletion :: T.Text -> CompletionItem
mkTypeCompletion label =
    defaultCompletion label &
    insertTextFormat ?~ PlainText &
    kind ?~ CiStruct

mkModuleCompletion :: T.Text -> CompletionItem
mkModuleCompletion label =
    defaultCompletion label &
    kind ?~ CiModule &
    detail ?~ label

mkKeywordCompletion :: T.Text -> CompletionItem
mkKeywordCompletion label =
    defaultCompletion label &
    kind ?~ CiKeyword

multiPackageTests :: FilePath -> TestTree
multiPackageTests damlc = testGroup "multi-package"
    [ testCaseSteps "IDE in root directory" $ \step -> withTempDir $ \dir -> do
          step "build a"
          createDirectoryIfMissing True (dir </> "a")
          writeFileUTF8 (dir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: a"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          writeFileUTF8 (dir </> "a" </> "A.daml") $ unlines
              [ "daml 1.2 module A where"
              , "data A = A"
              , "a = A"
              ]
          withCurrentDirectory (dir </> "a") $ callProcess damlc ["build", "-o", dir </> "a" </> "a.dar"]
          step "build b"
          createDirectoryIfMissing True (dir </> "b")
          writeFileUTF8 (dir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: b"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib, " <> show (".." </> "a" </> "a.dar") <> "]"
              ]
          writeFileUTF8 (dir </> "b" </> "B.daml") $ unlines
              [ "daml 1.2 module B where"
              , "import A"
              , "f : Scenario A"
              , "f = pure a"
              ]
          withCurrentDirectory (dir </> "b") $ callProcess damlc ["build", "-o", dir </> "b" </> "b.dar"]
          step "run language server"
          writeFileUTF8 (dir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              ]
          withCurrentDirectory dir $ runSessionWithConfig conf (damlc <> " ide --scenarios=yes") fullCaps' dir $ do
              docA <- openDoc ("a" </> "A.daml") "daml"
              Just fpA <- pure $ uriToFilePath (docA ^. uri)
              r <- getHover docA (Position 2 0)
              liftIO $ r @?= Just Hover
                { _contents = HoverContents $ MarkupContent MkMarkdown $ T.unlines
                      [ "```daml"
                      , "a"
                      , ": A"
                      , "```"
                        , "*\t*\t*"
                        , "*Defined at " <> T.pack fpA <> ":3:1*"
                      ]
                , _range = Just $ Range (Position 2 0) (Position 2 1)
                }
              docB <- openDoc ("b" </> "B.daml") "daml"
              Just escapedFpB <- pure $ escapeURIString isUnescapedInURIComponent <$> uriToFilePath (docB ^. uri)
              -- code lenses are a good test since they force LF compilation
              r <- getCodeLenses docB
              liftIO $ r @?=
                  [ CodeLens
                        { _range = Range (Position 3 0) (Position 3 1)
                        , _command = Just $ Command
                              { _title = "Scenario results"
                              , _command = "daml.showResource"
                              , _arguments = Just $ List
                                  [ "Scenario: f"
                                  , toJSON $ "daml://compiler?file=" <> escapedFpB <> "&top-level-decl=f"
                                  ]
                              }
                        , _xdata = Nothing
                        }
                  ]
    , testCaseSteps "IDE in project directory" $ \step -> withTempDir $ \dir -> do
          step "build a"
          createDirectoryIfMissing True (dir </> "a")
          writeFileUTF8 (dir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: a"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          writeFileUTF8 (dir </> "a" </> "A.daml") $ unlines
              [ "daml 1.2 module A where"
              , "data A = A"
              , "a = A"
              ]
          withCurrentDirectory (dir </> "a") $ callProcess damlc ["build", "-o", dir </> "a" </> "a.dar"]
          step "create b"
          createDirectoryIfMissing True (dir </> "b")
          writeFileUTF8 (dir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: b"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib, " <> show (".." </> "a" </> "a.dar") <> "]"
              ]
          writeFileUTF8 (dir </> "b" </> "B.daml") $ unlines
              [ "daml 1.2 module B where"
              , "import A"
              , "f : Scenario A"
              , "f = pure a"
              ]
          -- When run in the project directory, the IDE will take care of initializing
          -- the package db so we do not need to build.
          step "run language server"
          withCurrentDirectory (dir </> "b") $ runSessionWithConfig conf (damlc <> " ide --scenarios=yes") fullCaps' (dir </> "b") $ do
              -- We cannot open files in a here but we can open files in b
              docB <- openDoc "B.daml" "daml"
              Just escapedFpB <- pure $ escapeURIString isUnescapedInURIComponent <$> uriToFilePath (docB ^. uri)
              -- code lenses are a good test since they force LF compilation
              r <- getCodeLenses docB
              liftIO $ r @?=
                  [ CodeLens
                        { _range = Range (Position 3 0) (Position 3 1)
                        , _command = Just $ Command
                              { _title = "Scenario results"
                              , _command = "daml.showResource"
                              , _arguments = Just $ List
                                  [ "Scenario: f"
                                  , toJSON $ "daml://compiler?file=" <> escapedFpB <> "&top-level-decl=f"
                                  ]
                              }
                        , _xdata = Nothing
                        }
                  ]
    ]
