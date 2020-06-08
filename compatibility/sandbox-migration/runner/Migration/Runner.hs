-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Migration.Runner (main) where

-- This test runs through the following steps:
-- 1. Start postgres
-- 2. Start the oldest version of sandbox.
-- 3. Upload a DAR using the same SDK version.
-- 4. Stop sandbox.
-- 5. In a loop over all versions:
--    1. Start sandbox of the given version.
--    2. Run a custom scala binary for querying and creating new contracts.
--    3. Stop sandbox.
-- 6. Stop postgres.

import Control.Exception
import Control.Monad
import qualified Data.Text as T
import Options.Applicative
import Sandbox
    ( createSandbox
    , defaultSandboxConf
    , destroySandbox
    , nullDevice
    , sandboxPort
    , SandboxConfig(..)
    )
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Process
import WithPostgres (withPostgres)
import qualified Bazel.Runfiles

import qualified Migration.ProposeAccept as ProposeAccept
import Migration.Types

data Options = Options
  { modelDar :: FilePath
  , platformAssistants :: [FilePath]
  -- ^ Ordered list of assistant binaries that will be used to run sandbox.
  -- We run through migrations in the order of the list
  }

optsParser :: Parser Options
optsParser = Options
    <$> strOption (long "model-dar")
    <*> many (strArgument mempty)

main :: IO ()
main = do
    -- Limit sandbox and model-step memory.
    setEnv "_JAVA_OPTIONS" "-Xms128m -Xmx1g" True
    Options{..} <- execParser (info optsParser fullDesc)
    runfiles <- Bazel.Runfiles.create
    let step = Bazel.Runfiles.rlocation
            runfiles
            ("compatibility" </> "sandbox-migration" </> "migration-step")
    withPostgres $ \jdbcUrl -> do
        initialPlatform : _ <- pure platformAssistants
        hPutStrLn stderr "--> Uploading model DAR"
        withSandbox initialPlatform jdbcUrl $ \p ->
            callProcess initialPlatform
                [ "ledger"
                , "upload-dar", modelDar
                , "--host=localhost", "--port=" <> show p
                ]
        hPutStrLn stderr "<-- Uploaded model DAR"
        runTest jdbcUrl platformAssistants (ProposeAccept.test step modelDar)

runTest :: forall s r. T.Text -> [FilePath] -> Test s r -> IO ()
runTest jdbcUrl platformAssistants Test{..} = foldM_ step initialState platformAssistants
  where step :: s -> FilePath -> IO s
        step state assistant = do
            let version = takeFileName (takeDirectory assistant)
            hPutStrLn stderr ("--> Testing " <> version)
            r <- withSandbox assistant jdbcUrl $ \port ->
              executeStep (SdkVersion version) "localhost" port state
            case validateStep (SdkVersion version) state r of
                Left err -> fail err
                Right state' -> do
                    hPutStrLn stderr ("<-- Tested " <> version)
                    pure state'

withSandbox :: FilePath -> T.Text -> (Int -> IO a) -> IO a
withSandbox assistant jdbcUrl f =
    withBinaryFile nullDevice ReadWriteMode $ \handle ->
    withTempFile $ \portFile ->
    bracket (createSandbox portFile handle sandboxConfig) destroySandbox $ \resource ->
      f (sandboxPort resource)
  where
    sandboxConfig = defaultSandboxConf
        { sandboxBinary = assistant
        , sandboxArgs = ["sandbox-classic", "--jdbcurl=" <> T.unpack jdbcUrl]
        }
