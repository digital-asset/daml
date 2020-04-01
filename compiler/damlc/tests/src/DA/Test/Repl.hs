-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Repl (main) where

import Control.Exception
import Control.Monad.Extra
import DA.Bazel.Runfiles
import DA.Test.Sandbox
import Data.Aeson
import Data.Foldable
import qualified Data.ByteString.Char8 as BS
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import System.Environment.Blank
import System.Exit
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

testParty :: String
testParty = "Alice"

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    scriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")
    testDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "repl-test.dar")
    certDir <- locateRunfiles (mainWorkspace </> "ledger" </> "test-common" </> "test-certificates")
    defaultMain $
        testGroup "repl"
            [ withSandbox defaultSandboxConf { dars = [testDar] } $ \getSandboxPort ->
                  functionalTests damlc scriptDar testDar getSandboxPort
            , withSandbox defaultSandboxConf
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
        writeFile file ("Bearer " <> jwtToken)
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

functionalTests :: FilePath -> FilePath -> FilePath -> IO Int -> TestTree
functionalTests damlc scriptDar testDar getSandboxPort = testGroup "functional"
    [ testInteraction' "create and query"
          [ input "alice <- allocateParty \"Alice\""
          , input "debug =<< query @T alice"
          , matchOutput "^.*: \\[\\]$"
          , input "submit alice $ createCmd (T alice alice)"
          , input "debug =<< query @T alice"
          , matchOutput "^.*: \\[\\(<contract-id>,T {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
          ]
    , testInteraction' "propose and accept"
          [ input "alice <- allocateParty \"Alice\""
          , input "bob <- allocateParty \"Bob\""
          , input "submit alice $ createCmd (TProposal alice bob)"
          , input "props <- query @TProposal bob"
          , input "debug props"
          , matchOutput "^.*: \\[\\(<contract-id>,TProposal {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
          , input "forA props $ \\(prop, _) -> submit bob $ exerciseCmd prop Accept"
          , input "debug =<< query @T bob"
          , matchOutput "^.*: \\[\\(<contract-id>,T {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
          , input "debug =<< query @TProposal bob"
          , matchOutput "^.*: \\[\\]$"
          ]
    , testInteraction' "shadowing"
          [ input "x <- pure 1"
          , input "debug x"
          , matchOutput "^.*: 1$"
          , input "x <- pure $ x + x"
          , input "debug x"
          , matchOutput "^.*: 2$"
          ]
    , testInteraction' "parse error"
          [ input "eaiu\\1"
          , matchOutput "^parse error.*$"
          , input "debug 1"
          , matchOutput "^.*: 1"
          ]
    , testInteraction' "Tuple patterns"
          [ input "(a, b) <- pure (1, 2)"
          , input "(b, c) <- pure (3, 4)"
          , input "debug a"
          , matchOutput "^.*: 1"
          , input "debug b"
          , matchOutput "^.*: 3"
          , input "debug c"
          , matchOutput "^.*: 4"
          ]
    , testInteraction' "Partial patterns"
          -- TODO (MK) We do not test failing patterns yet
          -- since `error` calls aren’t handled nicely atm.
          [ input "Some (x, Some y) <- pure (Some (1, Some 2))"
          , input "debug x"
          , matchOutput "^.*: 1"
          , input "debug y"
          , matchOutput "^.*: 2"
          ]
    , testInteraction' "type error"
          [ input "1"
          -- TODO Make this less noisy
          , matchOutput "^File:.*$"
          , matchOutput "^Hidden:.*$"
          , matchOutput "^Range:.*$"
          , matchOutput "^Source:.*$"
          , matchOutput "^Severity:.*$"
          , matchOutput "^Message:.*$"
          , matchOutput "^.*error.*$"
          , matchOutput "^.*expected type .*Script .* with actual type .*Int.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , input "debug 1"
          , matchOutput "^.*: 1"
          ]
    , testInteraction' "script error"
          [ input "alice <- allocateParty \"Alice\""
          , input "bob <- allocateParty \"Bob\""
          , input "submit alice (createCmd (T alice bob))"
          , matchOutput "^.*Submit failed.*requires authorizers.*but only.*were given.*$"
          , input "debug 1"
          , matchOutput "^.*: 1"
          ]
    , testInteraction' "server error"
          [ input "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"alice_doubly_allocated\")"
          , input "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"alice_doubly_allocated\")"
          , matchOutput "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid argument: Party already exists"
          , input "debug 1"
          , matchOutput "^.*: 1"
          ]
    ]
  where
    testInteraction' testName steps = testCase testName $ do
        port <- getSandboxPort
        testInteraction damlc scriptDar testDar steps port Nothing Nothing

authTests :: FilePath -> FilePath -> FilePath -> IO Int -> IO FilePath -> TestTree
authTests damlc scriptDar testDar getSandboxPort getTokenFile = testGroup "auth"
    [ testInteraction' "allocate and query"
          [ input $ "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint " <> show testParty <> ")"
          , input "debug =<< query @T alice"
          , matchOutput "^.*: \\[\\]$"
          ]
    ]
  where
    testInteraction' testName steps = testCase testName $ do
        port <- getSandboxPort
        tokenFile <- getTokenFile
        testInteraction damlc scriptDar testDar steps port (Just tokenFile) Nothing

tlsTests :: FilePath -> FilePath -> FilePath -> IO Int -> FilePath -> TestTree
tlsTests damlc scriptDar testDar getSandboxPort certDir = testGroup "tls"
    [ testInteraction' "allocate and query"
          [ input "alice <- allocateParty \"Alice\""
          , input "debug =<< query @T alice"
          , matchOutput "^.*: \\[\\]$"
          ]
    ]
  where
    testInteraction' testName steps = testCase testName $ do
        port <- getSandboxPort
        testInteraction damlc scriptDar testDar steps port Nothing (Just $ certDir </> "ca.crt")

testInteraction
    :: FilePath
    -> FilePath
    -> FilePath
    -> [Step]
    -> Int
    -> Maybe FilePath
    -> Maybe FilePath
    -> Assertion
testInteraction damlc scriptDar testDar steps ledgerPort mbTokenFile mbCaCrt = withCreateProcess cp $ \mbIn mbOut _mbErr ph -> do
    Just hIn <- pure mbIn
    Just hOut <- pure mbOut
    for_ [hIn, hOut] $ \h -> hSetBuffering h LineBuffering
    for_ steps $ \step -> do
        case step of
            Input s -> do
                readPrompt hOut
                hPutStrLn hIn s
            MatchOutput regex regexStr -> do
                line <- hGetLine hOut
                assertBool
                    (show line <> " did not match " <> show regexStr)
                    (matchTest regex line)
    hClose hIn
    exit <- waitForProcess ph
    exit @?= ExitSuccess
    where cp = (proc damlc $ concat
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
               ) { std_in = CreatePipe, std_out = CreatePipe }

data Step
    = Input String
    -- ^ Input a line into the repl
    | MatchOutput Regex String
    -- ^ Match a line of output against a given regex.
    -- The String is used for error messages since Regex
    -- does not have a Show instance.

input :: String -> Step
input = Input

matchOutput :: String -> Step
matchOutput s = MatchOutput (makeRegex s) s

readPrompt :: Handle -> Assertion
readPrompt h = do
    res <- replicateM 6 (hGetChar h)
    res @?= "daml> "
