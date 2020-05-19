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
import qualified Data.Text as T
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
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    scriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")
    testDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "repl-test.dar")
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
                     ]
                   , [ "--access-token-file=" <> tokenFile | Just tokenFile <- [mbTokenFile] ]
                   , [ "--cacrt=" <> cacrt | Just cacrt <- [mbCaCrt] ]
                   ]
