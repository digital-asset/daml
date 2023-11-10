-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Repl (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Exception
import Control.Monad.Extra
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import DA.Test.Sandbox
import Data.Aeson
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString.Char8 as BS
import qualified Data.Map.Strict as Map
import Data.List
import qualified Data.Text as T
import DA.Test.Util
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import Text.Regex.TDFA
import qualified Web.JWT as JWT

testSecret :: String
testSecret = "I_CAN_HAZ_SECRET"

testLedgerId :: String
testLedgerId = "replledger"

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    limitJvmMemory defaultJvmMemoryLimits{maxHeapSize = "1g"}
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    certDir <- locateRunfiles (mainWorkspace </> "test-common" </> "test-certificates")
    tests <- forM [minBound @LF.MajorVersion .. maxBound] $ \major -> do
        let lfVersion =
             case major of
                 LF.V1 -> LF.versionDefault
                 -- TODO(#17366): test with the latest stable version of LF2 once there is one
                 LF.V2 -> LF.version2_dev
        let prettyMajor = LF.renderMajorVersion major
        scriptDar <- locateRunfiles $ case major of
            LF.V1 -> mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar"
            LF.V2 -> mainWorkspace </> "daml-script" </> "daml3" </> "daml3-script.dar"
        testDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "repl-test-v" <> prettyMajor <.> "dar")
        multiTestDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "repl-multi-test-v"<> prettyMajor <.>"dar")
        pure $ withCantonSandbox
            defaultSandboxConf
                { dars = [testDar]
                , timeMode = Static
                , devVersionSupport = LF.isDevVersion lfVersion
                }
            $ \getSandboxPort ->
                testGroup ("LF v" <> prettyMajor)
                    [ withCantonSandbox
                        defaultSandboxConf
                            { mbSharedSecret = Just testSecret
                            , mbLedgerId = Just testLedgerId
                            , devVersionSupport = LF.isDevVersion lfVersion
                            }
                        $ \getSandboxPort ->
                            withTokenFile $ \getTokenFile ->
                                authTests lfVersion damlc scriptDar getSandboxPort getTokenFile
                    , withCantonSandbox
                        defaultSandboxConf
                            { enableTls = True
                            , mbClientAuth = Just None
                            , devVersionSupport = LF.isDevVersion lfVersion
                            }
                        $ \getSandboxPort ->
                            tlsTests lfVersion damlc scriptDar getSandboxPort certDir
                    , staticTimeTests lfVersion damlc scriptDar getSandboxPort
                    , inboundMessageSizeTests lfVersion damlc scriptDar testDar getSandboxPort
                    , noPackageTests lfVersion damlc scriptDar
                    , importTests lfVersion damlc scriptDar testDar
                    , multiPackageTests lfVersion damlc scriptDar multiTestDar
                    ]
    defaultMain $ testGroup "repl" tests

withTokenFile :: (IO FilePath -> TestTree) -> TestTree
withTokenFile f = withResource acquire release (f . fmap fst)
  where
    acquire = mask_ $ do
        (file, delete) <- newTempFile
        writeFile file jwtToken
        pure (file, delete)
    release = snd

jwtToken :: String
jwtToken = T.unpack $ JWT.encodeSigned (JWT.EncodeHMACSecret $ BS.pack testSecret) mempty mempty
    { JWT.unregisteredClaims = JWT.ClaimsMap $ Map.fromList
          [ ( "https://daml.com/ledger-api"
            , Object $ KM.fromList
                  [ ("actAs", toJSON ["Alice" :: T.Text])
                  , ("ledgerId", toJSON testLedgerId)
                  , ("applicationId", "foobar")
                  , ("admin", toJSON True)
                  ]
            )
          ]
    }


authTests :: LF.Version -> FilePath -> FilePath -> IO Int -> IO FilePath -> TestTree
authTests lfVersion damlc scriptDar getSandboxPort getTokenFile = testGroup "auth"
    [ testCase "successful connection" $ do
        port <- getSandboxPort
        tokenFile <- getTokenFile
        testConnection lfVersion damlc scriptDar port (Just tokenFile) Nothing
    ]

tlsTests :: LF.Version -> FilePath -> FilePath -> IO Int -> FilePath -> TestTree
tlsTests lfVersion damlc scriptDar getSandboxPort certDir = testGroup "tls"
    [ testCase "successful connection" $ do
        port <- getSandboxPort
        testConnection lfVersion damlc scriptDar port Nothing (Just (certDir </> "ca.crt"))
    ]


-- | A simple test to ensure that the connection works, functional tests
-- should go in //compiler/damlc/tests:repl-functests
testConnection
    :: LF.Version
    -> FilePath
    -> FilePath
    -> Int
    -> Maybe FilePath
    -> Maybe FilePath
    -> Assertion
testConnection lfVersion damlc scriptDar ledgerPort mbTokenFile mbCaCrt = do
    out <- readCreateProcess cp $ unlines
        [ "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"Alice\")"
        , "debug alice"
        ]
    let regexString = "^.*daml>.*: 'Alice::[a-f0-9]+'\ndaml> Goodbye.\n$" :: String
    let regex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexString
    unless (matchTest regex out) $
        assertFailure (show out <> " did not match " <> show regexString <> ".")
    where cp = proc damlc $ concat
                   [ [ "repl"
                     , "--target=" <> LF.renderVersion lfVersion
                     , "--ledger-host=localhost"
                     , "--ledger-port"
                     , show ledgerPort
                     , "--script-lib"
                     , scriptDar
                     ]
                   , [ "--access-token-file=" <> tokenFile | Just tokenFile <- [mbTokenFile] ]
                   , [ "--cacrt=" <> cacrt | Just cacrt <- [mbCaCrt] ]
                   ]

staticTimeTests :: LF.Version -> FilePath -> FilePath -> IO Int -> TestTree
staticTimeTests lfVersion damlc scriptDar getSandboxPort = testGroup "static-time"
    [ testCase "setTime" $ do
        port <- getSandboxPort
        testSetTime lfVersion damlc scriptDar port
    ]

noPackageTests :: LF.Version -> FilePath -> FilePath -> TestTree
noPackageTests lfVersion damlc scriptDar = testGroup "no package"
    [ testCase "no package" $ do
        out <- readCreateProcess cp $ unlines
            [ "debug (1 + 1)"
            ]
        let regexString = "daml> \\[[^]]+\\]: 2\ndaml> Goodbye.\n$" :: String
        let regex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexString
        unless (matchTest regex out) $
            assertFailure (show out <> " did not match " <> show regexString <> ".")
    ]
    where cp = proc damlc
                   [ "repl"
                   , "--target=" <> LF.renderVersion lfVersion
                   , "--script-lib"
                   , scriptDar
                   ]

testSetTime
    :: LF.Version
    -> FilePath
    -> FilePath
    -> Int
    -> Assertion
testSetTime lfVersion damlc scriptDar ledgerPort = do
    out <- readCreateProcess cp $ unlines
        [ "import DA.Assert"
        , "import DA.Date"
        , "import DA.Time"
        , "expected <- pure (time (date 2000 Feb 2) 0 1 2)"
        , "setTime expected"
        , "actual <- getTime"
        , "assertEq actual expected"
        ]
    let regexString = "^daml> daml> daml> daml> daml> daml> daml> daml> Goodbye.\n$" :: String
    let regex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexString
    unless (matchTest regex out) $
        assertFailure (show out <> " did not match " <> show regexString <> ".")
    where cp = proc damlc
                   [ "repl"
                   , "--target=" <> LF.renderVersion lfVersion
                   , "--static-time"
                   , "--ledger-host=localhost"
                   , "--ledger-port"
                   , show ledgerPort
                   , "--script-lib"
                   , scriptDar
                   ]

-- | Test the @--import@ flag
importTests :: LF.Version -> FilePath -> FilePath -> FilePath -> TestTree
importTests lfVersion damlc scriptDar testDar = testGroup "import"
    [ testCase "none" $
      testImport lfVersion damlc scriptDar testDar [] False
    , testCase "unversioned" $
      testImport lfVersion damlc scriptDar testDar ["repl-test"] True
    , testCase "versioned" $
      testImport lfVersion damlc scriptDar testDar ["repl-test-0.1.0"] True
    ]

testImport
    :: LF.Version
    -> FilePath
    -> FilePath
    -> FilePath
    -> [String]
    -> Bool
    -> Assertion
testImport lfVersion damlc scriptDar testDar imports successful = do
    out <- readCreateProcess cp $ unlines
        [ "let Some alice = partyFromText \"alice\""
        , "debug (T alice alice)"
        ]
    let regexString :: String
        regexString
          | successful = "^daml> daml> .*: T {proposer = '.*', accepter = '.*'}\ndaml> Goodbye.\n$"
          | otherwise  = "^daml> daml> File: .*\nHidden: .*\nRange: .*\nSource: .*\nSeverity: DsError\nMessage: \n.*: error:\n    Data constructor not in scope: T : Party -> Party -> .*\ndaml> Goodbye.\n$"
    let regex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexString
    unless (matchTest regex out) $
        assertFailure (show out <> " did not match " <> show regexString <> ".")
    where cp = proc damlc $ concat
                   [ [ "repl"
                     , "--target=" <> LF.renderVersion lfVersion
                     , "--script-lib"
                     , scriptDar
                     , testDar
                     ]
                   , [ "--import=" <> pkg | pkg <- imports ]
                   ]

multiPackageTests :: LF.Version -> FilePath -> FilePath -> FilePath -> TestTree
multiPackageTests lfVersion damlc scriptDar multiTestDar = testGroup "multi-package"
  [ testCase "import both unversioned" $ do
      out <- readCreateProcess (cp ["repl-test", "repl-test-two"]) $ unlines
        [ "let Some alice = partyFromText \"p\""
        , "let x = T alice alice"
        , "let y = T2 alice"
        ]
      out @?= "daml> daml> daml> daml> Goodbye.\n"
  , testCase "import both versioned" $ do
      out <- readCreateProcess (cp ["repl-test-0.1.0", "repl-test-two-0.1.0"]) $ unlines
        [ "let Some alice = partyFromText \"p\""
        , "let x = T alice alice"
        , "let y = T2 alice"
        ]
      out @?= "daml> daml> daml> daml> Goodbye.\n"
  , testCase "import only repl-test" $ do
      out <- readCreateProcess (cp ["repl-test-0.1.0"]) $ unlines
        [ "let Some alice = partyFromText \"p\""
        , "let x = T alice alice"
        , "let y = T2 alice"
        ]
      unless ("Data constructor not in scope: T2" `isInfixOf` out) $
        assertFailure ("Unexpected output: " <> show out)
  ]
   where cp imports = proc damlc $ concat
                   [ [ "repl"
                     , "--target=" <> LF.renderVersion lfVersion
                     , "--script-lib"
                     , scriptDar
                     , multiTestDar
                     ]
                   , [ "--import=" <> pkg | pkg <- imports ]
                   ]

inboundMessageSizeTests :: LF.Version -> FilePath -> FilePath -> FilePath -> IO Int -> TestTree
inboundMessageSizeTests lfVersion damlc scriptDar testDar getSandboxPort = testGroup "max-inbound-message-size"
    [ testCase "large transaction succeeds" $ do
          port <- getSandboxPort
          out <- readCreateProcess (cp port) $ unlines
              [ "import DA.Text"
              , "alice <- allocateParty \"Alice\""
              , "_ <- submit alice $ createAndExerciseCmd (MessageSize alice) (CreateN 4000)"
              , "1 + 1"
              ]
          let regexString = "gRPC message exceeds maximum size 1000" :: String
          let regex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexString
          unless (matchTest regex out) $
              assertFailure (show out <> " did not match " <> show regexString <> ".")
    ]
  where
    cp port = proc damlc
        [ "repl"
        , "--target=" <> LF.renderVersion lfVersion
        , "--ledger-host=localhost"
        , "--ledger-port"
        , show port
        , "--script-lib"
        , scriptDar
        , testDar
        , "--import=repl-test"
        -- Limit size to make it fail and assert on expected error
        , "--max-inbound-message-size=1000"
        ]
