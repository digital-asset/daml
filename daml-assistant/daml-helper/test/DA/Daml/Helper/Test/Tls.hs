-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Test.Tls (main) where

import Control.Exception
import DA.Bazel.Runfiles
import DA.PortFile
import Data.List.Extra (isInfixOf)
import System.Environment.Blank
import System.Exit
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
    certDir <- locateRunfiles (mainWorkspace </> "ledger" </> "test-common" </> "test-certificates")
    defaultMain $
        testGroup "TLS"
           [ withSandbox sandbox certDir "none" $ \getSandboxPort ->
                 testGroup "client-auth: none"
                     [ testCase "succeeds without client cert" $ do
                           p <- sandboxPort <$> getSandboxPort
                           let ledgerOpts =
                                   [ "--host=localhost" , "--port", show p
                                   , "--cacrt", certDir </> "ca.crt"
                                   ]
                           out <- readProcess damlHelper
                               ("ledger" : "list-parties" : ledgerOpts)
                               ""
                           assertInfixOf "no parties are known" out
                     ]
           , withSandbox sandbox certDir "optional" $ \getSandboxPort ->
                 testGroup "client-auth: optional"
                     [ testCase "succeeds without client cert" $ do
                           p <- sandboxPort <$> getSandboxPort
                           let ledgerOpts =
                                   [ "--host=localhost" , "--port", show p
                                   , "--cacrt", certDir </> "ca.crt"
                                   ]
                           out <- readProcess damlHelper
                               ("ledger" : "list-parties" : ledgerOpts)
                               ""
                           assertInfixOf "no parties are known" out
                     ]
           , withSandbox sandbox certDir "require" $ \getSandboxPort ->
                 testGroup "client-auth: require"
                     [ testCase "fails without client cert" $ do
                           p <- sandboxPort <$> getSandboxPort
                           let ledgerOpts =
                                   [ "--host=localhost" , "--port", show p
                                   , "--cacrt", certDir </> "ca.crt"
                                   ]
                           (exit, stderr, stdout) <- readProcessWithExitCode damlHelper
                               ("ledger" : "list-parties" : ledgerOpts)
                               ""
                           assertInfixOf "Listing parties" stderr
                           -- Sadly we do not seem to get a better error for this.
                           assertInfixOf "GRPCIOTimeout" stdout
                           exit @?= ExitFailure 1
                     , testCase "succeeds with client cert" $ do
                           p <- sandboxPort <$> getSandboxPort
                           let ledgerOpts =
                                   [ "--host=localhost" , "--port", show p
                                   , "--cacrt", certDir </> "ca.crt"
                                   , "--pem", certDir </> "client.pem"
                                   , "--crt", certDir </> "client.crt"
                                   ]
                           out <- readProcess damlHelper
                               ("ledger" : "list-parties" : ledgerOpts)
                               ""
                           assertInfixOf "no parties are known" out
                     ]
           ]
  where withSandbox sandbox certDir auth f =
            withResource (openBinaryFile nullDevice ReadWriteMode) hClose $ \getDevNull ->
            withResource newTempFile snd $ \getPortFile ->
            let createSandbox' = do
                    devNull <- getDevNull
                    (portFile, _) <- getPortFile
                    createSandbox devNull sandbox portFile
                        (certDir </> "server.crt", certDir </> "server.pem")
                        (certDir </> "ca.crt")
                        auth
            in withResource createSandbox' destroySandbox f


-- TODO Factor this out into a shared module across tests
data SandboxResource = SandboxResource
    { sandboxProcess :: (Maybe Handle, Maybe Handle, Maybe Handle, ProcessHandle)
    , sandboxPort :: Int
    }

createSandbox :: Handle -> FilePath -> FilePath -> (FilePath, FilePath) -> FilePath -> String -> IO SandboxResource
createSandbox _devNull sandbox portFile (crt, key) cacrt clientAuth = mask $ \unmask -> do
    ph <- createProcess
        (proc sandbox ["--port=0", "--port-file", portFile, "--crt", crt, "--pem", key, "--cacrt", cacrt, "--client-auth", clientAuth])
--        { std_out = UseHandle devNull }
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

