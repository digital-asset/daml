-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Repl (main) where

import Control.Exception
import Control.Monad.Extra
import DA.Bazel.Runfiles
import DA.Test.Sandbox
import Data.Aeson
import qualified Data.ByteString.Char8 as BS
import qualified Data.HashMap.Strict as HashMap
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
    limitJvmMemory defaultJvmMemoryLimits
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    scriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")
    testDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "repl-test.dar")
    multiTestDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "repl-multi-test.dar")
    certDir <- locateRunfiles (mainWorkspace </> "ledger" </> "test-common" </> "test-certificates")
    defaultMain $
        testGroup "repl"
            [ withSandbox defaultSandboxConf
                  { dars = [testDar]
                  , mbSharedSecret = Just testSecret
                  , mbLedgerId = Just testLedgerId
                  } $ \getSandboxPort ->
              withTokenFile $ \getTokenFile ->
                  authTests damlc scriptDar testDar getSandboxPort getTokenFile
            , withSandbox defaultSandboxConf
                  { dars = [testDar]
                  , enableTls = True
                  , mbClientAuth = Just None
                  } $ \getSandboxPort ->
                  tlsTests damlc scriptDar testDar getSandboxPort certDir
            , withSandbox defaultSandboxConf
                  { dars = [testDar]
                  , mbLedgerId = Just testLedgerId
                  , timeMode = Static
                  } $ \getSandboxPort ->
              staticTimeTests damlc scriptDar testDar getSandboxPort
            , withSandbox defaultSandboxConf $ \getSandboxPort ->
                  noPackageTests damlc scriptDar getSandboxPort
            , withSandbox defaultSandboxConf
                  { dars = [testDar]
                  , mbLedgerId = Just testLedgerId
                  } $ \getSandboxPort ->
              importTests damlc scriptDar testDar getSandboxPort
            , multiPackageTests damlc scriptDar multiTestDar
            , noLedgerTests damlc scriptDar
            ]

withTokenFile :: (IO FilePath -> TestTree) -> TestTree
withTokenFile f = withResource acquire release (f . fmap fst)
  where
    acquire = mask_ $ do
        (file, delete) <- newTempFile
        writeFile file jwtToken
        pure (file, delete)
    release = snd

jwtToken :: String
jwtToken = T.unpack $ JWT.encodeSigned (JWT.HMACSecret $ BS.pack testSecret) mempty mempty
    { JWT.unregisteredClaims = JWT.ClaimsMap $ Map.fromList
          [ ( "https://daml.com/ledger-api"
            , Object $ HashMap.fromList
                  [ ("actAs", toJSON ["Alice" :: T.Text])
                  , ("ledgerId", toJSON testLedgerId)
                  , ("applicationId", "foobar")
                  , ("admin", toJSON True)
                  ]
            )
          ]
    }


authTests :: FilePath -> FilePath -> FilePath -> IO Int -> IO FilePath -> TestTree
authTests damlc scriptDar testDar getSandboxPort getTokenFile = testGroup "auth"
    [ testCase "successful connection" $ do
        port <- getSandboxPort
        tokenFile <- getTokenFile
        testConnection damlc scriptDar testDar port (Just tokenFile) Nothing
    ]

tlsTests :: FilePath -> FilePath -> FilePath -> IO Int -> FilePath -> TestTree
tlsTests damlc scriptDar testDar getSandboxPort certDir = testGroup "tls"
    [ testCase "successful connection" $ do
        port <- getSandboxPort
        testConnection damlc scriptDar testDar port Nothing (Just (certDir </> "ca.crt"))
    ]


-- | A simple test to ensure that the connection works, functional tests
-- should go in //compiler/damlc/tests:repl-functests
testConnection
    :: FilePath
    -> FilePath
    -> FilePath
    -> Int
    -> Maybe FilePath
    -> Maybe FilePath
    -> Assertion
testConnection damlc scriptDar testDar ledgerPort mbTokenFile mbCaCrt = do
    out <- readCreateProcess cp $ unlines
        [ "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"Alice\")"
        , "debug =<< query @T alice"
        ]
    let regexString = "^daml> daml>.*: \\[\\]\ndaml> Goodbye.\n$" :: String
    let regex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexString
    unless (matchTest regex out) $
        assertFailure (show out <> " did not match " <> show regexString <> ".")
    where cp = proc damlc $ concat
                   [ [ "repl"
                     , "--ledger-host=localhost"
                     , "--ledger-port"
                     , show ledgerPort
                     , "--script-lib"
                     , scriptDar
                     , testDar
                     , "--import"
                     , "repl-test"
                     ]
                   , [ "--access-token-file=" <> tokenFile | Just tokenFile <- [mbTokenFile] ]
                   , [ "--cacrt=" <> cacrt | Just cacrt <- [mbCaCrt] ]
                   ]

staticTimeTests :: FilePath -> FilePath -> FilePath -> IO Int -> TestTree
staticTimeTests damlc scriptDar testDar getSandboxPort = testGroup "static-time"
    [ testCase "setTime" $ do
        port <- getSandboxPort
        testSetTime damlc scriptDar testDar port
    ]

noPackageTests :: FilePath -> FilePath -> IO Int -> TestTree
noPackageTests damlc scriptDar getSandboxPort = testGroup "static-time"
    [ testCase "no package" $ do
        port <- getSandboxPort
        out <- readCreateProcess (cp port) $ unlines
            [ "debug (1 + 1)"
            ]
        let regexString = "daml> \\[[^]]+\\]: 2\ndaml> Goodbye.\n$" :: String
        let regex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexString
        unless (matchTest regex out) $
            assertFailure (show out <> " did not match " <> show regexString <> ".")
    ]
    where cp port = proc damlc
                   [ "repl"
                   , "--ledger-host=localhost"
                   , "--ledger-port"
                   , show port
                   , "--script-lib"
                   , scriptDar
                   ]

noLedgerTests :: FilePath -> FilePath -> TestTree
noLedgerTests damlc scriptDar = testGroup "no ledger"
    [ testCase "no ledger" $ do
          out <- readCreateProcess cp $ unlines
              [ "1 + 1"
              , "listKnownParties"
              , "2 + 2"
              ]
          out @?= unlines
            [ "daml> 2"
            , "daml> java.lang.RuntimeException: No default participant"
            , "daml> 4"
            , "daml> Goodbye."
            ]
    ]
  where
    cp = proc damlc
        [ "repl"
        , "--script-lib"
        , scriptDar
        ]

testSetTime
    :: FilePath
    -> FilePath
    -> FilePath
    -> Int
    -> Assertion
testSetTime damlc scriptDar testDar ledgerPort = do
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
                   , "--static-time"
                   , "--ledger-host=localhost"
                   , "--ledger-port"
                   , show ledgerPort
                   , "--script-lib"
                   , scriptDar
                   , testDar
                   , "--import"
                   , "repl-test"
                   ]

-- | Test the @--import@ flag
importTests :: FilePath -> FilePath -> FilePath -> IO Int -> TestTree
importTests damlc scriptDar testDar getSandboxPort = testGroup "import"
    [ testCase "none" $ do
        port <- getSandboxPort
        testImport damlc scriptDar testDar port [] False
    , testCase "unversioned" $ do
        port <- getSandboxPort
        testImport damlc scriptDar testDar port ["repl-test"] True
    , testCase "versioned" $ do
        port <- getSandboxPort
        testImport damlc scriptDar testDar port ["repl-test-0.1.0"] True
    ]

testImport
    :: FilePath
    -> FilePath
    -> FilePath
    -> Int
    -> [String]
    -> Bool
    -> Assertion
testImport damlc scriptDar testDar ledgerPort imports successful = do
    out <- readCreateProcess cp $ unlines
        [ "alice <- allocateParty \"Alice\""
        , "debug (T alice alice)"
        ]
    let regexString :: String
        regexString
          | successful = "^daml> daml> .*: T {proposer = '.*', accepter = '.*'}\ndaml> Goodbye.\n$"
          | otherwise  = "^daml> daml> File: .*\nHidden: .*\nRange: .*\nSource: .*\nSeverity: DsError\nMessage: .*: error:Data constructor not in scope: T : Party -> Party -> .*\ndaml> Goodbye.\n$"
    let regex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexString
    unless (matchTest regex out) $
        assertFailure (show out <> " did not match " <> show regexString <> ".")
    where cp = proc damlc $ concat
                   [ [ "repl"
                     , "--ledger-host=localhost"
                     , "--ledger-port"
                     , show ledgerPort
                     , "--script-lib"
                     , scriptDar
                     , testDar
                     ]
                   , [ "--import=" <> pkg | pkg <- imports ]
                   ]

multiPackageTests :: FilePath -> FilePath -> FilePath -> TestTree
multiPackageTests damlc scriptDar multiTestDar = testGroup "multi-package"
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
                     , "--script-lib"
                     , scriptDar
                     , multiTestDar
                     ]
                   , [ "--import=" <> pkg | pkg <- imports ]
                   ]
