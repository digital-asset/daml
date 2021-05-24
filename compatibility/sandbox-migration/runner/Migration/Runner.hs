-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import Data.Either
import Data.List
import Data.Maybe
import qualified Data.SemVer as SemVer
import qualified Data.Text as T
import Options.Applicative
import Sandbox
    ( createSandbox
    , defaultSandboxConf
    , destroySandbox
    , sandboxPort
    , SandboxConfig(..)
    )
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Process
import WithPostgres (withPostgres)
import qualified Bazel.Runfiles

import qualified Migration.Divulgence as Divulgence
import qualified Migration.KeyTransfer as KeyTransfer
import qualified Migration.ProposeAccept as ProposeAccept
import Migration.Types

data Options = Options
  { modelDar :: FilePath
  , platformAssistants :: [FilePath]
  -- ^ Ordered list of assistant binaries that will be used to run sandbox.
  -- We run through migrations in the order of the list
  , appendOnly :: AppendOnly
  }

newtype AppendOnly = AppendOnly Bool

optsParser :: Parser Options
optsParser = Options
    <$> strOption (long "model-dar")
    <*> many (strArgument mempty)
    <*> fmap AppendOnly (switch (long "append-only"))

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
        withSandbox appendOnly initialPlatform jdbcUrl $ \p ->
            callProcess initialPlatform
                [ "ledger"
                , "upload-dar", modelDar
                , "--host=localhost", "--port=" <> show p
                ]
        runTest appendOnly jdbcUrl platformAssistants
            (ProposeAccept.test step modelDar `interleave` KeyTransfer.test step modelDar `interleave` Divulgence.test step modelDar)

supportsAppendOnly :: SemVer.Version -> Bool
supportsAppendOnly v = v == SemVer.initial
-- Note: until the append-only migration is frozen, only the head version of it should be used
--supportsAppendOnly v = v == SemVer.initial || v > prev
--  where
--    prev = fromRight (error "invalid version") (SemVer.fromText "<first version that has a frozen append-only migration>")

runTest :: forall s r. AppendOnly -> T.Text -> [FilePath] -> Test s r -> IO ()
runTest appendOnly jdbcUrl platformAssistants Test{..} = do
        hPutStrLn stderr "<-- Uploaded model DAR"
        foldM_ (step jdbcUrl) initialState platformAssistants
            where step :: T.Text -> s -> FilePath -> IO s
                  step jdbcUrl state assistant = do
                      let version = assistantVersion assistant
                      hPutStrLn stderr ("--> Testing: " <> "SDK version: " <> SemVer.toString version)
                      r <- withSandbox appendOnly assistant jdbcUrl $ \port ->
                           executeStep (SdkVersion version) "localhost" port state
                      case validateStep (SdkVersion version) state r of
                          Left err -> fail err
                          Right state' -> do
                              hPutStrLn stderr ("<-- Tested: SDK version: " <> SemVer.toString version)
                              pure state'

assistantVersion :: FilePath -> SemVer.Version
assistantVersion path =
    let ver = fromJust (stripPrefix "daml-sdk-" (takeFileName (takeDirectory path)))
    in fromRight (error $ "Invalid version: " <> show ver) (SemVer.fromText $ T.pack ver)

withSandbox :: AppendOnly -> FilePath -> T.Text -> (Int -> IO a) -> IO a
withSandbox (AppendOnly appendOnly) assistant jdbcUrl f =
    withTempFile $ \portFile ->
    bracket (createSandbox portFile stderr sandboxConfig) destroySandbox $ \resource ->
      f (sandboxPort resource)
  where
    version = assistantVersion assistant
    sandboxConfig = defaultSandboxConf
        { sandboxBinary = assistant
        , sandboxArgs =
          [ "sandbox-classic"
          , "--jdbcurl=" <> T.unpack jdbcUrl
          , "--contract-id-seeding=testing-weak"
          ] <>
          [ "--enable-append-only-schema" | supportsAppendOnly version && appendOnly ]
        }
