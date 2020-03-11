-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Test.Tls (main) where

import Control.Exception
import DA.Bazel.Runfiles
import DA.PortFile
import Data.List.Extra (isInfixOf)
import System.Environment.Blank
import System.FilePath
import System.Info.Extra
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlHelper <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> exe "daml-helper")
    sandbox <- locateRunfiles (mainWorkspace </> "ledger" </> "sandbox" </> exe "sandbox-binary")
    certDir <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> "test-certificates")
    withTempFile $ \portFile ->
        withBinaryFile nullDevice ReadWriteMode $ \devNull ->
        defaultMain $ withResource (createSandbox devNull sandbox portFile (certDir </> "server.crt", certDir </> "server.pem") (certDir </> "ca.crt")) destroySandbox $ \getSandbox ->
        testGroup "TLS"
            [ testCase "Party management" $ do
                  p <- sandboxPort <$> getSandbox
                  let ledgerOpts =
                          [ "--host=localhost", "--port", show p
                          , "--cacrt", certDir </> "ca.crt"
                          , "--pem", certDir </> "client.pem"
                          , "--crt", certDir </> "client.crt"
                          ]
                  out <- readProcess damlHelper
                      ("ledger" : "list-parties" : ledgerOpts)
                      ""
                  assertInfixOf "no parties are known" out
                  out <- readProcess damlHelper
                      ("ledger" : "allocate-party" : "Alice" : ledgerOpts)
                      ""
                  assertInfixOf "Allocated 'Alice' for 'Alice'" out
                  out <- readProcess damlHelper
                      ("ledger" : "list-parties" : ledgerOpts)
                      ""
                  assertInfixOf "PartyDetails {party = 'Alice', displayName = \"Alice\", isLocal = True}" out
            -- TODO (MK) Once we have a ledger server (e.g. sandbox) that supports
            -- TLS without client auth, we should test this here as well.
            ]

-- TODO Factor this out into a shared module across tests
data SandboxResource = SandboxResource
    { sandboxProcess :: (Maybe Handle, Maybe Handle, Maybe Handle, ProcessHandle)
    , sandboxPort :: Int
    }

createSandbox :: Handle -> FilePath -> FilePath -> (FilePath, FilePath) -> FilePath -> IO SandboxResource
createSandbox devNull sandbox portFile (crt, key) cacrt = mask $ \unmask -> do
    ph <- createProcess
        (proc sandbox ["--port=0", "--port-file", portFile, "--crt", crt, "--pem", key, "--cacrt", cacrt])
        { std_out = UseHandle devNull }
    let waitForStart = do
            port <- readPortFile maxRetries portFile
            pure (SandboxResource ph port)
    unmask (waitForStart `onException` cleanupProcess ph)

destroySandbox :: SandboxResource -> IO ()
destroySandbox = cleanupProcess . sandboxProcess

nullDevice :: FilePath
nullDevice
    -- taken from typed-process
    | isWindows = "\\\\.\\NUL"
    | otherwise =  "/dev/null"

assertInfixOf :: String -> String -> Assertion
assertInfixOf needle haystack = assertBool ("Expected " <> show needle <> " in output but but got " <> show haystack) (needle `isInfixOf` haystack)

