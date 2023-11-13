-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
module Main (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Applicative.Combinators
import Control.Concurrent
import Control.Lens hiding (List, children, (.=))
import Control.Monad
import Control.Monad.IO.Class
import DA.Bazel.Runfiles
import DA.Daml.Lsp.Test.Util
import Data.Aeson (toJSON, (.=))
import Data.Aeson qualified as Aeson
import Data.Aeson.Key qualified as Aeson.Key
import Data.Char (toLower)
import Data.Either
import Data.Foldable (toList)
import Data.List.Extra
import Data.Text qualified as T
import Development.IDE.Core.Rules.Daml (VirtualResourceChangedParams(..))
import Language.LSP.Test qualified as LSP
import Language.LSP.Test qualified as LspTest
import Language.LSP.Types hiding (SemanticTokenAbsolute (..), SemanticTokenRelative (..))
import Language.LSP.Types.Capabilities
import Language.LSP.Types.Lens hiding (id, to)
import Network.URI
import SdkVersion
import System.Directory
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import Text.Regex.TDFA

fullCaps' :: ClientCapabilities
fullCaps' = fullCaps { _window = Just $ WindowClientCapabilities (Just True) Nothing Nothing }

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlcPath <- locateRunfiles $
        mainWorkspace </> "compiler" </> "damlc" </> exe "damlc"
    scriptDarPath <- locateRunfiles $
        mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar"
    let run s = withTempDir $ \dir -> runSessionWithConfig conf (damlcPath <> " ide") fullCaps' dir s
        runScripts s
            -- We are currently seeing issues with GRPC FFI calls which make everything
            -- that uses the scenario service extremely flaky and forces us to disable it on
            -- CI. Once https://github.com/digital-asset/daml/issues/1354 is fixed we can
            -- also run these tests on Windows.
            | isWindows = pure ()
            | otherwise = withTempDir $ \dir -> do
                copyFile scriptDarPath (dir </> "daml-script.dar")
                writeFileUTF8 (dir </> "daml.yaml") $ unlines
                    [ "sdk-version: " <> sdkVersion
                    , "name: script-test"
                    , "version: 0.0.1"
                    , "source: ."
                    , "dependencies:"
                    , "- daml-prim"
                    , "- daml-stdlib"
                    , "- daml-script.dar"
                    ]
                withCurrentDirectory dir $ do
                    let cmd = damlcPath
                        args = ["ide", "--debug"]
                        createProc = (proc cmd args) { std_in = CreatePipe, std_out = CreatePipe, std_err = CreatePipe }
                    withCreateProcess createProc $ \mbServerIn mbServerOut mbServerErr _serverProc -> do
                        case (mbServerIn, mbServerOut, mbServerErr) of
                            (Just serverIn, Just serverOut, Just serverErr) -> do
                                runSessionWithHandles
                                    serverIn
                                    serverOut
                                    conf
                                    fullCaps'
                                    dir
                                    (s serverErr)
                            _ -> error "runScripts: Cannot start without stdin, stdout, and stderr."

    defaultMain $ testGroup "LSP"
        [ symbolsTests run
        , diagnosticTests run runScripts
        , requestTests run runScripts
        , scriptTests runScripts
        , stressTests run
        , executeCommandTests run
        , regressionTests run
        , includePathTests damlcPath scriptDarPath
        , multiPackageTests damlcPath scriptDarPath
        , completionTests run runScripts
        , raceTests runScripts
        ]

conf :: SessionConfig
conf = defaultConfig
    -- If you uncomment this you can see all messages
    -- which can be quite useful for debugging.
    { logStdErr = True }
    -- { logMessages = True, logColor = False, logStdErr = True }

diagnosticTests
    :: (Session () -> IO ())
    -> ((Handle -> Session ()) -> IO ())
    -> TestTree
diagnosticTests run runScripts = testGroup "diagnostics"
    [ testCase "diagnostics disappear after error is fixed" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "module Test where"
              , "f 1"
              ]
          expectDiagnostics [("Test.daml", [(DsError, (1, 0), "Parse error")])]
          replaceDoc test $ T.unlines
              [ "module Test where"
              , "f = ()"
              ]
          expectDiagnostics [("Test.daml", [])]
          closeDoc test
    , testCase "lower-case drive" $ run $ do
          let aContent = T.unlines
                [ "module A.A where"
                , "import A.B ()"
                ]
              bContent = T.unlines
                [ "module A.B where"
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
          sendNotification STextDocumentDidOpen (DidOpenTextDocumentParams itemA)
          diagsNot <- skipManyTill anyMessage LspTest.publishDiagnosticsNotification
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
              [ "module Test where"
              , "f = ()"
              ]
          replaceDoc test $ T.unlines
              [ "module Test where"
              , "f 1"
              ]
          expectDiagnostics [("Test.daml", [(DsError, (1, 0), "Parse error")])]
          closeDoc test
    , testCase "percent encoding does not matter" $ run $ do
          Just uri <- parseURI . T.unpack . getUri <$> getDocUri "Test.daml"
          let weirdUri = Uri $ T.pack $ "file://" <> escapeURIString (== '/') (uriPath uri)
          let item = TextDocumentItem weirdUri (T.pack damlId) 0 $ T.unlines
                  [ "module Test where"
                  , "f = ()"
                  ]
          sendNotification STextDocumentDidOpen (DidOpenTextDocumentParams item)
          let test = TextDocumentIdentifier weirdUri
          replaceDoc test $ T.unlines
              [ "module Test where"
              , "f 1"
              ]
          expectDiagnostics [("Test.daml", [(DsError, (1, 0), "Parse error")])]
          closeDoc test
    , testCase "failed name resolution" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "add : Int -> Int -> Int"
              , "add a b = ab + b"
              , "succ : Int -> Int"
              , "succ = abdd 1"
              ]
          expectDiagnostics
              [ ( "Main.daml"
                , [ (DsError, (2, 10), "Variable not in scope: ab")
                  , (DsError, (4, 7), "Variable not in scope: abdd")
                  ]
                )
              ]
          closeDoc main'
    , testCase "import cycle" $ run $ do
          let aContent = T.unlines
                  [ "module A where"
                  , "import B"
                  ]
              bContent = T.unlines
                  [ "module B where"
                  , "import A"
                  ]
          [a, b] <- openDocs damlId [("A.daml", aContent), ("B.daml", bContent)]
          expectDiagnostics
              [ ( "A.daml"
                , [(DsError, (1, 7), "Cyclic module dependency between A, B")]
                )
              , ( "B.daml"
                , [(DsError, (1, 7), "Cyclic module dependency between A, B")]
                )
              ]
          closeDoc b
          closeDoc a
    , testCase "import error" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "import Oops"
              ]
          expectDiagnostics
              [("Main.daml", [(DsError, (1, 7), "Could not find module 'Oops'")])]
          closeDoc main'
    , testCase "multi module funny" $ run $ do
          libsC <- openDoc' "Libs/C.daml" damlId $ T.unlines
              [ "module Libs.C where"
              ]
          libsB <- openDoc' "Libs/B.daml" damlId $ T.unlines
              [ "module Libs.B where"
              , "import Libs.C"
              ]
          libsA <- openDoc' "Libs/A.daml" damlId $ T.unlines
              [ "module Libs.A where"
              , "import C"
              ]
          expectDiagnostics
              [ ( "Libs/A.daml"
                , [(DsError, (1, 7), "Could not find module 'C'")]
                )
              , ( "Libs/B.daml"
                , [(DsWarning, (1, 0), "import of 'Libs.C' is redundant")]
                )
              ]
          closeDoc libsA
          closeDoc libsB
          closeDoc libsC
    , testCase "parse error" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "module Test where"
              , "f 1"
              ]
          expectDiagnostics [("Test.daml", [(DsError, (1, 0), "Parse error")])]
          closeDoc test
    , testCase "type error" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "oops = 1 + \"foo\""
              ]
          expectDiagnostics
              [ ( "Main.daml"
                , [ ( DsError
                    , (1, 11)
                    , "Couldn't match expected type 'Int' with actual type 'Text'"
                    )
                  ]
                )
              ]
          closeDoc main'
    , testCase "script error" $ runScripts $ \_stderr -> do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "import Daml.Script"
              , "template Agree with p1 : Party; p2 : Party where"
              , "  signatory [p1, p2]"
              , "myScript = script do"
              , "  foo <- allocateParty \"Foo\""
              , "  alice <- allocateParty \"Alice\""
              , "  submit foo $ createCmd $ Agree with p1 = foo, p2 = alice"
              ]
          _ <- openScript "Main.daml" "myScript"
          expectDiagnostics
              [ ( "Main.daml"
                , [(DsError, (4, 0), "missing authorization from 'Alice'")]
                )
              ]
          closeDoc main'
    ]


requestTests
    :: (Session () -> IO ())
    -> ((Handle -> Session ()) -> IO ())
    -> TestTree
requestTests run runScripts = testGroup "requests"
    [ testCase "code-lenses" $ runScripts $ \_stderr -> do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "import Daml.Script"
              , "single : Script ()"
              , "single = script do"
              , "  assert (True == True)"
              ]
          lenses <- getCodeLenses main'
          Just escapedFp <- pure $ escapeURIString isUnescapedInURIComponent <$> uriToFilePath (main' ^. uri)
          liftIO $ lenses @?=
              [ CodeLens
                    { _range = Range (Position 3 0) (Position 3 6)
                    , _command = Just $ Command
                          { _title = "Script results"
                          , _command = "daml.showResource"
                          , _arguments = Just $ List
                              [ "Script: single"
                              ,  toJSON $ "daml://compiler?file=" <> escapedFp <> "&top-level-decl=single"
                              ]
                          }
                    , _xdata = Nothing
                    }
              ]

          closeDoc main'
    , testCase "stale code-lenses" $ runScripts $ \_stderr -> do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "import Daml.Script"
              , "single : Script ()"
              , "single = script do"
              , "  assert True"
              ]
          lenses <- getCodeLenses main'
          Just escapedFp <- pure $ escapeURIString isUnescapedInURIComponent <$> uriToFilePath (main' ^. uri)
          let codeLens range =
                  CodeLens
                    { _range = range
                    , _command = Just $ Command
                          { _title = "Script results"
                          , _command = "daml.showResource"
                          , _arguments = Just $ List
                              [ "Script: single"
                              ,  toJSON $ "daml://compiler?file=" <> escapedFp <> "&top-level-decl=single"
                              ]
                          }
                    , _xdata = Nothing
                    }
          liftIO $ lenses @?= [codeLens (Range (Position 3 0) (Position 3 6))]
          changeDoc main' [TextDocumentContentChangeEvent (Just (Range (Position 4 23) (Position 4 23))) Nothing "+"]
          expectDiagnostics [("Main.daml", [(DsError, (5, 0), "Parse error")])]
          lenses <- getCodeLenses main'
          liftIO $ lenses @?= [codeLens (Range (Position 3 0) (Position 3 6))]
          -- Shift code lenses down
          changeDoc main' [TextDocumentContentChangeEvent (Just (Range (Position 0 0) (Position 0 0))) Nothing "\n\n"]
          lenses <- getCodeLenses main'
          liftIO $ lenses @?= [codeLens (Range (Position 5 0) (Position 5 6))]
          closeDoc main'
    , testCase "add type signature code lens shows unqualified HasField" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "assetIssuer asset = asset.issuer"
              ]
          lenses <- getCodeLenses main'
          let mainUri = getUri (main' ^. uri)
              signature = "assetIssuer : HasField \"issuer\" r a => r -> a"
              addTypeSigLens =
                  CodeLens
                      { _range = Range (Position 1 0) (Position 1 11)
                      , _command = Just $ Command
                          { _title = signature
                          , _command = "typesignature.add"
                          , _arguments = Just $ List $ singleton $ Aeson.object
                              [ "changes" .= Aeson.object
                                  [ Aeson.Key.fromText mainUri .=
                                      [ Aeson.object
                                          [ "newText" .= (signature <> "\n")
                                          , "range" .= toJSON (Range (Position 1 0) (Position 1 0))
                                          ]
                                      ]
                                  ]
                              ]
                          }
                      , _xdata = Nothing
                      }
          liftIO $ lenses @?= [addTypeSigLens]
          closeDoc main'
    , testCase "type on hover: name" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "add : Int -> Int -> Int"
              , "add a b = a + b"
              , "template DoStuff with party : Party where"
              , "  signatory party"
              , "  choice ChooseNumber : Int"
              , "    with number : Int"
              , "    controller party"
              , "    do pure (add 5 number)"
              ]
          Just fp <- pure $ uriToFilePath (main' ^. uri)
          r <- getHover main' (Position 8 14)
          liftIO $ r @?= Just Hover
              { _contents = HoverContents $ MarkupContent MkMarkdown $ T.unlines
                    [ "```daml"
                    , "Main.add"
                    , ": Int -> Int -> Int"
                    , "```"
                    , "* * *"
                    , "*Defined at " <> T.pack fp <> ":3:1*"
                    ]
              , _range = Just $ Range (Position 8 13) (Position 8 16)
              }
          closeDoc main'

    , testCase "type on hover: literal" $ run $ do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "simple arg1 = let xlocal = 1.0 in xlocal + arg1"
              ]
          r <- getHover main' (Position 1 27)
          liftIO $ r @?= Just Hover
              { _contents = HoverContents $ MarkupContent MkMarkdown $ T.unlines
                    [ "```daml"
                    , "1.0"
                    , ": (Additive a, IsNumeric a)"
                    , "=> a"
                    , "```"
                    , "* * *"
                    ]
              , _range = Just $ Range (Position 1 27) (Position 1 30)
              }
          closeDoc main'
    , testCase "definition" $ run $ do
          test <- openDoc' "Test.daml" damlId $ T.unlines
              [ "module Test where"
              , "answerFromTest = 42"
              ]
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
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
              , "    choice SomeChoice : ()"
              , "      controller p"
              , "      do let b = bar"
              , "         pure ()"
              ]
          -- thisIsAParam
          locs <- getDefinitions main' (Position 3 24)
          liftIO $ collapseLocations locs @?= [Location (main' ^. uri) (Range (Position 3 4) (Position 3 16))]
          -- letParam
          locs <- getDefinitions main' (Position 4 37)
          liftIO $ collapseLocations locs @?= [Location (main' ^. uri) (Range (Position 4 16) (Position 4 24))]
          -- import Test
          locs <- getDefinitions main' (Position 1 10)
          liftIO $ collapseLocations locs @?= [Location (test ^. uri) (Range (Position 0 0) (Position 0 0))]
          -- use of `bar` in template
          locs <- getDefinitions main' (Position 13 18)
          liftIO $ collapseLocations locs @?= [Location (main' ^. uri) (Range (Position 2 0) (Position 2 3))]
          -- answerFromTest
          locs <- getDefinitions main' (Position 2 8)
          liftIO $ collapseLocations locs @?= [Location (test ^. uri) (Range (Position 1 0) (Position 1 14))]

          -- introduce syntax error
          changeDoc main' [TextDocumentContentChangeEvent (Just (Range (Position 6 6) (Position 6 6))) Nothing "+\n"]
          expectDiagnostics [("Main.daml", [(DsError, (6, 6), "Parse error")])]

          -- everything should still work because we use stale information.
          -- thisIsAParam
          locs <- getDefinitions main' (Position 3 24)
          liftIO $ collapseLocations locs @?= [Location (main' ^. uri) (Range (Position 3 4) (Position 3 16))]
          -- letParam
          locs <- getDefinitions main' (Position 4 37)
          liftIO $ collapseLocations locs @?= [Location (main' ^. uri) (Range (Position 4 16) (Position 4 24))]
          -- import Test
          locs <- getDefinitions main' (Position 1 10)
          liftIO $ collapseLocations locs @?= [Location (test ^. uri) (Range (Position 0 0) (Position 0 0))]
          -- use of `bar` in template
          locs <- getDefinitions main' (Position 14 18)
          liftIO $ collapseLocations locs @?= [Location (main' ^. uri) (Range (Position 2 0) (Position 2 3))]
          -- answerFromTest
          locs <- getDefinitions main' (Position 2 8)
          liftIO $ collapseLocations locs @?= [Location (test ^. uri) (Range (Position 1 0) (Position 1 14))]

          closeDoc main'
          closeDoc test
    ]

scriptTests :: ((Handle -> Session ()) -> IO ()) -> TestTree
scriptTests runScripts = testGroup "scripts"
    [ testCase "opening codelens produces a notification" $ runScripts $ \_stderr -> do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "{-# LANGUAGE ApplicativeDo #-}"
              , "module Main where"
              , "import Daml.Script"
              , "main : Script ()"
              , "main = assert (True == True)"
              ]
          lenses <- getCodeLenses main'
          uri <- scriptUri "Main.daml" "main"
          liftIO $ lenses @?=
              [ CodeLens
                    { _range = Range (Position 4 0) (Position 4 4)
                    , _command = Just $ Command
                          { _title = "Script results"
                          , _command = "daml.showResource"
                          , _arguments = Just $ List
                              [ "Script: main"
                              ,  toJSON uri
                              ]
                          }
                    , _xdata = Nothing
                    }
              ]
          mainScript <- openScript "Main.daml" "main"
          _ <- waitForScriptDidChange
          closeDoc mainScript
    , testCase "script ok" $ runScripts $ \_stderr -> do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "{-# LANGUAGE ApplicativeDo #-}"
              , "module Main where"
              , "import Daml.Script"
              , "main : Script String"
              , "main = pure \"ok\""
              ]
          script <- openScript "Main.daml" "main"
          expectScriptContent "Return value: &quot;ok&quot"
          closeDoc script
          closeDoc main'
    , testCase "spaces in path" $ runScripts $ \_stderr -> do
          main' <- openDoc' "spaces in path/Main.daml" damlId $ T.unlines
              [ "{-# LANGUAGE ApplicativeDo #-}"
              , "module Main where"
              , "import Daml.Script"
              , "main : Script String"
              , "main = pure \"ok\""
              ]
          script <- openScript "spaces in path/Main.daml" "main"
          expectScriptContent "Return value: &quot;ok&quot"
          closeDoc script
          closeDoc main'
    , testCase "submit location" $ runScripts $ \_stderr -> do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "{-# LANGUAGE ApplicativeDo #-}"
              , "module Main where"
              , "import Daml.Script"
              , "template T with party : Party where signatory party"
              , "main : Script (ContractId T)"
              , "main = do"
              , "  alice <- allocateParty \"Alice\""
              , "  submit alice do createCmd (T alice)"
              , "  submitCreateT alice"
              , "submitCreateT party = submit party do createCmd (T party)"
              ]
          script <- openScript "Main.daml" "main"
          expectScriptContentMatch "title=\"Main:8:3\">Main:8:3</a>.*title=\"Main:10:23\">Main:10:23</a>"
          closeDoc script
          closeDoc main'
    , testCase "changing source file causes new message" $ runScripts $ \_stderr -> do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "{-# LANGUAGE ApplicativeDo #-}"
              , "module Main where"
              , "import Daml.Script"
              , "main : Script ()"
              , "main = debug \"firstRun\""
              ]
          lenses <- getCodeLenses main'
          uri <- scriptUri "Main.daml" "main"
          liftIO $ lenses @?=
              [ CodeLens
                    { _range = Range (Position 4 0) (Position 4 4)
                    , _command = Just $ Command
                          { _title = "Script results"
                          , _command = "daml.showResource"
                          , _arguments = Just $ List
                              [ "Script: main"
                              ,  toJSON uri
                              ]
                          }
                    , _xdata = Nothing
                    }
              ]
          script <- openScript "Main.daml" "main"
          originalResult <- waitForScriptDidChange
          changeDoc main'
              [ TextDocumentContentChangeEvent Nothing Nothing $ T.unlines
                  [ "{-# LANGUAGE ApplicativeDo #-}"
                  , "module Main where"
                  , "import Daml.Script"
                  , "main : Script ()"
                  , "main = debug \"secondRun\""
                  ]
              ]
          changeResult <- waitForScriptDidChange
          liftIO $ do
              assertRegex (_vrcpContents originalResult) "Trace:[^/]+firstRun"
              assertRegex (_vrcpContents changeResult) "Trace:[^/]+secondRun"
          closeDoc script
          closeDoc main'
    , localOption (mkTimeout 30000000) $ -- 30s timeout
        testCaseSteps "scenario service interrupts outdated script runs" $ \step -> runScripts $ \_stderr -> do
          let mkDoc :: Integer -> T.Text
              mkDoc duration = T.unlines
                  [ "{-# LANGUAGE ApplicativeDo #-}"
                  , "module Main where"
                  , "import Daml.Script"
                  , "main : Script ()"
                  , "main = debug $ foldl (+) 0 [1.." <> T.pack (show duration) <> "]"
                  ]

          -- open document with long-running script
          main' <- openDoc' "Main.daml" damlId $ mkDoc 10000000
          liftIO $ step "Document opened."

          -- wait until lenses processed, open script
          lenses <- getCodeLenses main'
          uri <- scriptUri "Main.daml" "main"
          liftIO $ lenses @?=
              [ CodeLens
                    { _range = Range (Position 4 0) (Position 4 4)
                    , _command = Just $ Command
                          { _title = "Script results"
                          , _command = "daml.showResource"
                          , _arguments = Just $ List
                              [ "Script: main"
                              ,  toJSON uri
                              ]
                          }
                    , _xdata = Nothing
                    }
              ]
          script <- openScript "Main.daml" "main"
          liftIO $ step "Script opened, awaiting script start..."

          -- check that script started
          _ <- liftIO $ hTakeUntil _stderr "SCENARIO SERVICE STDOUT: Script started."
          liftIO $ step "Script has started, changing original doc..."

          -- replace with short-running script
          changeDoc main' [ TextDocumentContentChangeEvent Nothing Nothing $ mkDoc 23 ]
          liftIO $ step "Doc changes sent..."

          -- check that new script is started
          _ <- liftIO $ hTakeUntil _stderr "SCENARIO SERVICE STDOUT: Script started."
          liftIO $ step "New script started."

          -- check that previous script is cancelled
          _ <- liftIO $ hTakeUntil _stderr "SCENARIO SERVICE STDOUT: Script cancelling."
          _ <- liftIO $ hTakeUntil _stderr "SCENARIO SERVICE STDOUT: Script cancelled."
          liftIO $ step "Previous script cancelled"

          -- check that returned value is new script
          _changeResult <- waitForScriptDidChange
          liftIO $ assertRegex (_vrcpContents _changeResult) "Trace:( |<br>)*276([^0-9]|$)"
          liftIO $ step "Script results received."

          closeDoc script
          closeDoc main'
    , localOption (mkTimeout 60000000) $ -- 60s timeout
        testCaseSteps "scenario service does not interrupt on non-script messages" $ \step -> runScripts $ \_stderr -> do
          -- open document with long-running script
          main' <- openDoc' "Main.daml" damlId $
              T.unlines
                  [ "{-# LANGUAGE ApplicativeDo #-}"
                  , "module Main where"
                  , "import Daml.Script"
                  , "main : Script ()"
                  , "main = debug $ foldl (+) 0 [1..10000000]"
                  ]
          liftIO $ step "Document opened."

          -- wait until lenses processed, open script
          lenses <- getCodeLenses main'
          uri <- scriptUri "Main.daml" "main"
          liftIO $ lenses @?=
              [ CodeLens
                    { _range = Range (Position 4 0) (Position 4 4)
                    , _command = Just $ Command
                          { _title = "Script results"
                          , _command = "daml.showResource"
                          , _arguments = Just $ List
                              [ "Script: main"
                              ,  toJSON uri
                              ]
                          }
                    , _xdata = Nothing
                    }
              ]
          script <- openScript "Main.daml" "main"
          liftIO $ step "Script opened, awaiting script start..."

          -- check that script started
          _ <- liftIO $ hTakeUntil _stderr "SCENARIO SERVICE STDOUT: Script started."
          liftIO $ step "Script has started, sending document hover..."

          -- run hover event
          _ <- sendRequest STextDocumentHover (HoverParams main' (Position 4 3) Nothing)
          liftIO $ step "Hover sent..."

          -- Check that script did return and that log does not show any cancellations
          _changeResult <- waitForScriptDidChange
          _scriptFinishedMessage <- liftIO $ assertUntilWithout _stderr "SCENARIO SERVICE STDOUT: Script finished." "SCENARIO SERVICE STDOUT: Script cancelled."
          liftIO $ step "Script returned without cancellation."

          closeDoc script
          closeDoc main'
    ]

hTakeUntil :: Handle -> T.Text -> IO (Maybe String)
hTakeUntil handle regex = go
  where
    pred = matchTest (makeRegex regex :: Regex)
    go = do
      closed <- hIsClosed handle
      if closed
        then pure Nothing
        else do
          line <- hGetLine handle
          if pred line then pure (Just line) else go

-- Takes lines from the handle until matching the first pattern `until`. If any
-- of the lines before the matching line match the second pattern `without`
-- then fail the test
-- Useful for cases where we want to assert that some message has been emitted
-- without a different message being emitted in the interim, e.g. a script
-- finished without restarts in between.
assertUntilWithout :: Handle -> T.Text -> T.Text -> IO (Maybe String)
assertUntilWithout handle until without = go
  where
    untilP = matchTest (makeRegex until :: Regex)
    withoutP = matchTest (makeRegex without :: Regex)
    go = do
      closed <- hIsClosed handle
      if closed
        then pure Nothing
        else do
          line <- hGetLine handle
          if withoutP line then
            assertFailure $ "Source line: `" <> line <> "` shouldn't match regular expression `" <> T.unpack without <> "`, but it does."
          else if untilP line then
            pure (Just line)
          else go

assertRegex :: T.Text -> T.Text -> Assertion
assertRegex source regex =
    let errMsg = "Source text: `" <> T.unpack source <> "` should match regular expression `" <> T.unpack regex <> "`, but it doesn't."
    in
    assertBool
        errMsg
        (matchTest (makeRegex regex :: Regex) source)

executeCommandTests
    :: (Session () -> IO ())
    -> TestTree
executeCommandTests run = testGroup "execute command"
    [ testCase "Invalid commands result in error"  $ run $ do
        main' <- openDoc' "Empty.daml" damlId $ T.unlines
            [ "module Empty where"
            ]
        Just escapedFp <- pure $ uriToFilePath (main' ^. uri)
        actualDotString <- LSP.request SWorkspaceExecuteCommand $ ExecuteCommandParams
           Nothing "daml/NoCommand"  (Just (List [Aeson.String $ T.pack escapedFp]))
        liftIO $ assertBool "Expected response error but got success" (isLeft $ _result actualDotString)
        closeDoc main'
    ]

-- | Do extreme things to the compiler service.
stressTests
  :: (Session () -> IO ())
  -> TestTree
stressTests run = testGroup "Stress tests"
  [ testCase "Modify a file 2000 times" $ run $ do

        let fooValue :: Int -> T.Text
            -- Even values should produce empty diagnostics
            -- while odd values will produce a type error.
            fooValue i = T.pack (show (i `div` 2))
                      <> if even i then "" else ".5: Decimal"
            fooContent i = T.unlines
                [ "module Foo where"
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
                        m <- LSP.publishDiagnosticsNotification
                        let diags = toList $ m ^. params . diagnostics
                        guard (notNull diags)
                        pure diags
                liftIO $ unless (length diags == 1) $
                    assertFailure $ "Incorrect number of diagnostics, expected 1 but got " <> show diags
                let msg = head diags ^. message
                liftIO $ assertBool ("Expected type error but got " <> T.unpack msg) $
                    "Couldn't match type" `T.isInfixOf` msg

        foo <- openDoc' "Foo.daml" damlId $ fooContent 0
        forM_ [1 .. 2000] $ \i -> do
            replaceDoc foo $ fooContent i
            expect i
        expectDiagnostics [("Foo.daml", [])]
  , testCase "Set 10 files of interest" $ run $ do
        foos <- forM [1 .. 10 :: Int] $ \i ->
            makeModule ("Foo" ++ show i) ["foo  10"]
        expectDiagnostics
            [ ("Foo" ++ show i ++ ".daml", [(DsError, (1, 0), "Parse error")])
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
            expectDiagnostics [("Foo0.daml", [(DsError, (3, 7), "Couldn't match expected type")])]
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
        [ "module " <> T.pack name <> " where"
        ] ++ lines
    makeModule :: String -> [T.Text] -> Session TextDocumentIdentifier
    makeModule name lines = openDoc' (name ++ ".daml") damlId $
        moduleContent name lines

regressionTests
    :: (Session () -> IO ())
    -> TestTree
regressionTests run = testGroup "regression"
  [ testCase "completion on stale file" $ run $ do
        -- This used to produce "cannot continue after interface file error"
        -- since we used a function from GHCi in ghcide.
        foo <- openDoc' "Foo.daml" damlId $ T.unlines
            [ "{-# OPTIONS_GHC -Wall #-}"
            , "module Foo where"
            , "import DA.List"
            , ""
            ]
        expectDiagnostics [("Foo.daml", [(DsWarning, (2,0), "redundant")])]
        completions <- getCompletions foo (Position 2 1)
        liftIO $
            assertBool ("DA.List and DA.Internal.RebindableSyntax should be in " <> show completions) $
            mkModuleCompletion "DA.Internal.RebindableSyntax" `elem` completions &&
            mkModuleCompletion "DA.List" `elem` completions
        changeDoc foo [TextDocumentContentChangeEvent (Just (Range (Position 2 0) (Position 2 1))) Nothing "Syntax"]
        expectDiagnostics [("Foo.daml", [(DsError, (2,0), "Parse error")])]
        completions <- getCompletions foo (Position 2 6)
        liftIO $ completions @?= [mkModuleCompletion "DA.Internal.RebindableSyntax" & detail .~ Nothing]
  ]

symbolsTests :: (Session () -> IO ()) -> TestTree
symbolsTests run =
    testGroup
        "getDocumentSymbols"
        [ testCase "no internal imports for empty module" $
          run $ do
              foo <- openDoc' "Foo.daml" damlId $ T.unlines ["module Foo where"]
              syms <- getDocumentSymbols foo
              liftIO $ preview (_Left . _head . children . _Just) syms @?= Just (List [])
        , testCase "no internal imports for module with one template" $
          run $ do
              foo <-
                  openDoc' "Foo.daml" damlId $
                  T.unlines
                      [ "module Foo where"
                      , "template T with p1 : Party where"
                      , "  signatory p1"
                      , "  choice A : ()"
                      , "    with p : Party"
                      , "    controller p"
                      , "    do return ()"
                      ]
              syms <- getDocumentSymbols foo
              liftIO $
                  fmap (length . toList) (preview (_Left . _head . children . _Just) syms) @?=
                  Just 2
        ]

completionTests
    :: (Session () -> IO ())
    -> ((Handle -> Session ()) -> IO ())
    -> TestTree
completionTests run runScripts = testGroup "completion"
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
            -- crash (before lsp 0.21 it did crash).
            liftIO $ assertBool "Expected completions to be non-empty"
                (not $ null completions)
    , testCaseSteps "infix operator has code action and lint" $ \step -> runScripts $ \_stderr -> do
            foo <- openDoc' "Foo.daml" damlId $ T.unlines
                [ "module Foo where"
                , ""
                , "import Daml.Script"
                -- , "import DA.Assert ((=/=))"
                , ""
                , "main : Script ()"
                , "main = do"
                , "  1 =/= 2"
                , "  assert (1 /= 2)"
                , "  pure ()"
                ]

            -- Need to wait for diagnostics twice, as diagnostics get updated in two messages
            _ <- waitForDiagnostics
            expectDiagnostics
                [("Foo.daml", [(DsError, (6, 4), "Variable not in scope: (=/=)")])
                ,("Foo.daml", [(DsInfo, (7, 2), "Use === for better error messages")])
                ]
            liftIO $ step "Got expected diagnostics"

            codeActions <- getAllCodeActions foo
            liftIO $ do
                {- HLINT ignore "Use nubOrd" -}
                case nub codeActions of
                  [InR uniqueCodeAction]
                    | uniqueCodeAction ^. title == "import DA.Assert ((=/=))" -> pure ()
                    | otherwise ->
                        assertFailure ("Expected code action `import DA.Assert ((=/=))`, instead got " ++ show uniqueCodeAction)
                  [InL command] ->
                    assertFailure ("Expected a code action, instead got a command " ++ show command)
                  [] ->
                    assertFailure "Expected one unique code action, got no code actions instead."
                  otherActions ->
                    assertFailure ("Expected only one unique code action, instead got " ++ show otherActions)
                step "Got expected single code action, `import DA.Assert ((=/=))`"
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
          , _insertTextMode = Nothing
          , _textEdit = Nothing
          , _additionalTextEdits = Nothing
          , _commitCharacters = Nothing
          , _command = Nothing
          , _xdata = Nothing
          , _tags = Nothing
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

includePathTests :: FilePath -> FilePath -> TestTree
includePathTests damlc scriptDarPath = testGroup "include-path"
    [ testCase "IDE in root directory" $ withTempDir $ \dir -> do
          createDirectory (dir </> "src1")
          createDirectory (dir </> "src2")
          copyFile scriptDarPath (dir </> "daml-script.dar")
          writeFileUTF8 (dir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: a"
              , "version: 0.0.1"
              , "source: src1/Root.daml"
              , "dependencies: [daml-prim, daml-stdlib, daml-script.dar]"
              , "build-options:"
              , "- --include=src1/"
              , "- --include=src2/"
              ]
          writeFileUTF8 (dir </> "src1" </> "A.daml") $ unlines
              [ "module A where"
              , "data A = A Text"
              ]
          writeFileUTF8 (dir </> "src2" </> "B.daml") $ unlines
              [ "module B where"
              , "import Daml.Script"
              , "import A"
              , "a = A \"abc\""
              , "test = script $ assert False"
              ]
          writeFileUTF8 (dir </> "src1" </> "Root.daml") $ unlines
              [ "module Root where"
              , "import Daml.Script"
              , "import A ()"
              , "import B ()"
              , "test = script $ assert False"
              ]
          withCurrentDirectory dir $
            runSessionWithConfig conf (damlc <> " ide") fullCaps' dir $ do
              _docB <- openDoc "src2/B.daml" "daml"
              _ <- openScript "src2/B.daml" "test"
              -- If we get a script result, we managed to build a DALF which
              -- is what we really want to check here.
              expectDiagnostics [ ("src2/B.daml", [(DsError, (4,0), "Assertion failed")]) ]
              _docRoot <- openDoc "src1/Root.daml" "daml"
              _ <- openScript "src1/Root.daml" "test"
              expectDiagnostics [ ("src1/Root.daml", [(DsError, (4,0), "Assertion failed")]) ]
    ]

multiPackageTests :: FilePath -> FilePath -> TestTree
multiPackageTests damlc scriptDarPath
  | isWindows = testGroup "multi-package (skipped)" [] -- see issue #4904
  | otherwise = testGroup "multi-package"
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
              [ "module A where"
              , "data A = A"
              , "a = A"
              ]
          withCurrentDirectory (dir </> "a") $ callProcess damlc ["build", "-o", dir </> "a" </> "a.dar"]
          step "build b"
          createDirectoryIfMissing True (dir </> "b")
          copyFile scriptDarPath (dir </> "b" </> "daml-script.dar")
          writeFileUTF8 (dir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: b"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib, daml-script.dar, " <> show (".." </> "a" </> "a.dar") <> "]"
              ]
          writeFileUTF8 (dir </> "b" </> "B.daml") $ unlines
              [ "module B where"
              , "import Daml.Script"
              , "import A"
              , "f : Script A"
              , "f = pure a"
              ]
          withCurrentDirectory (dir </> "b") $ callProcess damlc ["build", "-o", dir </> "b" </> "b.dar"]
          step "run language server"
          writeFileUTF8 (dir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              ]
          withCurrentDirectory dir $ runSessionWithConfig conf (damlc <> " ide") fullCaps' dir $ do
              docA <- openDoc ("a" </> "A.daml") "daml"
              Just fpA <- pure $ uriToFilePath (docA ^. uri)
              r <- getHover docA (Position 2 0)
              liftIO $ r @?= Just Hover
                { _contents = HoverContents $ MarkupContent MkMarkdown $ T.unlines
                      [ "```daml"
                      , "a"
                      , ": A"
                      , "```"
                        , "* * *"
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
                        { _range = Range (Position 4 0) (Position 4 1)
                        , _command = Just $ Command
                              { _title = "Script results"
                              , _command = "daml.showResource"
                              , _arguments = Just $ List
                                  [ "Script: f"
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
              [ "module A where"
              , "data A = A"
              , "a = A"
              ]
          withCurrentDirectory (dir </> "a") $ callProcess damlc ["build", "-o", dir </> "a" </> "a.dar"]
          step "create b"
          createDirectoryIfMissing True (dir </> "b")
          copyFile scriptDarPath (dir </> "b" </> "daml-script.dar")
          writeFileUTF8 (dir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: b"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib, daml-script.dar, " <> show (".." </> "a" </> "a.dar") <> "]"
              ]
          writeFileUTF8 (dir </> "b" </> "B.daml") $ unlines
              [ "module B where"
              , "import Daml.Script"
              , "import A"
              , "f : Script A"
              , "f = pure a"
              ]
          -- When run in the project directory, the IDE will take care of initializing
          -- the package db so we do not need to build.
          step "run language server"
          withCurrentDirectory (dir </> "b") $ runSessionWithConfig conf (damlc <> " ide") fullCaps' (dir </> "b") $ do
              -- We cannot open files in a here but we can open files in b
              docB <- openDoc "B.daml" "daml"
              Just escapedFpB <- pure $ escapeURIString isUnescapedInURIComponent <$> uriToFilePath (docB ^. uri)
              -- code lenses are a good test since they force LF compilation
              r <- getCodeLenses docB
              liftIO $ r @?=
                  [ CodeLens
                        { _range = Range (Position 4 0) (Position 4 1)
                        , _command = Just $ Command
                              { _title = "Script results"
                              , _command = "daml.showResource"
                              , _arguments = Just $ List
                                  [ "Script: f"
                                  , toJSON $ "daml://compiler?file=" <> escapedFpB <> "&top-level-decl=f"
                                  ]
                              }
                        , _xdata = Nothing
                        }
                  ]
    ]

raceTests
    :: ((Handle -> Session ()) -> IO ())
    -> TestTree
raceTests runScripts = testGroup "race tests"
    [ testCaseSteps "race code lens & hover requests" $ \step -> runScripts $ \_stderr -> do
          main' <- openDoc' "Main.daml" damlId $ T.unlines
              [ "module Main where"
              , "import Daml.Script"
              , "single : Script ()"
              , "single = script do"
              , "  assert (True == True)"
              ]
          lensId <- sendRequest STextDocumentCodeLens (CodeLensParams Nothing Nothing main')
          liftIO $ threadDelay (5*1000000)
          hoverId <- sendRequest STextDocumentHover (HoverParams main' (Position 3 13) Nothing)
          liftIO $ step "waiting for lens response"
          _ <- skipManyTill anyMessage (responseForId STextDocumentCodeLens lensId)
          liftIO $ step "waiting for hover response"
          _ <- skipManyTill anyMessage (responseForId STextDocumentHover hoverId)
          closeDoc main'
    ]

linkToLocation :: [LocationLink] -> [Location]
linkToLocation = map (\LocationLink{_targetUri,_targetRange} -> Location _targetUri _targetRange)

collapseLocations :: [Location] |? [LocationLink] -> [Location]
collapseLocations = either id linkToLocation . toEither
