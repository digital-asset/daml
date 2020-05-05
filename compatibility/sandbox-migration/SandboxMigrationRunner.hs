-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
module Main (main) where

-- This test runs through the following steps:
-- 1. Start postgres
-- 2. Start the oldest version of sandbox.
-- 3. Upload a DAR using the same SDK version.
-- 4. Stop sandbox.
-- 5. In a loop over all versions:
--    1. Start sandbox of the given version.
--    2. Run a script for querying and creating new contracts.
--    3. Stop sandbox.
-- 6. Stop postgres.

import Control.Exception
import Control.Monad
import qualified Data.Aeson as A
import Data.Foldable
import qualified Data.Text as T
import GHC.Generics
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

data Options = Options
  { scriptDar :: FilePath
  , modelDar :: FilePath
  , scriptAssistant :: FilePath
  -- ^ Assistant binary used to run DAML Script
  , platformAssistants :: [FilePath]
  -- ^ Ordered list of assistant binaries that will be used to run sandbox.
  -- We run through migrations in the order of the list
  }

optsParser :: Parser Options
optsParser = Options
    <$> strOption (long "script-dar")
    <*> strOption (long "model-dar")
    <*> strOption (long "script-assistant")
    <*> many (strArgument mempty)

main :: IO ()
main = do
    -- Limit sandbox and DAML Script memory.
    setEnv "_JAVA_OPTIONS" "-Xms128m -Xmx1g" True
    Options{..} <- execParser (info optsParser fullDesc)
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
        void $ foldlM (testVersion scriptAssistant scriptDar jdbcUrl) [] platformAssistants

testVersion :: FilePath -> FilePath -> T.Text -> [Tuple2 (ContractId T) T] -> FilePath -> IO [Tuple2 (ContractId T) T]
testVersion scriptAssistant scriptDar jdbcUrl prevTs assistant = do
    let note = takeFileName (takeDirectory assistant)
    hPutStrLn stderr ("--> Testing " <> note)
    withSandbox assistant jdbcUrl $ \port ->
        withTempFile $ \inputFile ->
        withTempFile $ \outputFile -> do
        A.encodeFile inputFile (ScriptInput testProposer testAccepter note)
        callProcess scriptAssistant
            [ "script"
            , "--dar"
            , scriptDar
            , "--ledger-host=localhost"
            , "--ledger-port=" <> show port
            , "--input-file", inputFile
            , "--output-file", outputFile
            , "--script-name=Script:run"
            ]
        Just Result{..} <- A.decodeFileStrict' outputFile
        -- Test that all proposals are archived.
        unless (null oldTProposals) $
            fail ("Expected no old TProposals but got " <> show oldTProposals)
        unless (null newTProposals) $
            fail ("Expected no new TProposals but got " <> show newTProposals)
        unless (prevTs == oldTs) $
            fail ("Active ts should not have changed after migration: " <> show (prevTs, oldTs))
        -- Test that no T contracts got archived.
        let missingTs = filter (`notElem` newTs) oldTs
        unless (null missingTs) $
            fail ("The following contracts got lost during the migration: " <> show missingTs)
        -- Test that only one new T contract is not archived.
        let addedTs = filter (`notElem` oldTs) newTs
        case addedTs of
            [Tuple2 _ t] -> do
                let expected = T testProposer testAccepter note
                unless (t == expected) $
                    fail ("Expected " <> show expected <> " but got " <> show t)
            _ -> fail ("Expected 1 new T contract but got: " <> show addedTs)
        hPutStrLn stderr ("<-- Tested " <> note)
        pure newTs

testProposer :: Party
testProposer = Party "proposer"

testAccepter :: Party
testAccepter = Party "accepter"

-- The datatypes are defined such that the autoderived Aeson instances
-- match the DAML-LF JSON encoding.

newtype ContractId t = ContractId T.Text
  deriving newtype A.FromJSON
  deriving stock (Eq, Show)
newtype Party = Party T.Text
  deriving newtype (A.FromJSON, A.ToJSON)
  deriving stock (Eq, Show)

data T = T
  { proposer :: Party
  , accepter :: Party
  , note :: String
  } deriving (Eq, Generic, Show)

instance A.FromJSON T

data TProposal = TProposal
  { proposer :: Party
  , accepter :: Party
  , note :: T.Text
  } deriving (Generic, Show)

instance A.FromJSON TProposal

data ScriptInput = ScriptInput
  { _1 :: Party
  , _2 :: Party
  , _3 :: String
  } deriving Generic

instance A.ToJSON ScriptInput

data Tuple2 a b = Tuple2
  { _1 :: a
  , _2 :: b
  } deriving (Eq, Generic, Show)

instance (A.FromJSON a, A.FromJSON b) => A.FromJSON (Tuple2 a b)

data Result = Result
  { oldTProposals :: [Tuple2 (ContractId TProposal) TProposal]
  , newTProposals :: [Tuple2 (ContractId TProposal) TProposal]
  , oldTs :: [Tuple2 (ContractId T) T]
  , newTs :: [Tuple2 (ContractId T) T]
  } deriving Generic

instance A.FromJSON Result

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
