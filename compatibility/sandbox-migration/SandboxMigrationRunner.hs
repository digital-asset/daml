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
--    2. Run a custom scala binary for querying and creating new contracts.
--    3. Stop sandbox.
-- 6. Stop postgres.

import Control.Exception
import Control.Monad
import qualified Data.Aeson as A
import Data.Foldable
import qualified Data.Text as T
import GHC.Generics (Generic)
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
        void $ foldlM (testVersion step modelDar jdbcUrl) ([], []) platformAssistants

testVersion
    :: FilePath
    -> FilePath
    -> T.Text
    -> ([Tuple2 (ContractId T) T], [Transaction])
    -> FilePath
    -> IO ([Tuple2 (ContractId T) T], [Transaction])
testVersion step modelDar jdbcUrl (prevTs, prevTransactions) assistant = do
    let note = takeFileName (takeDirectory assistant)
    hPutStrLn stderr ("--> Testing " <> note)
    withSandbox assistant jdbcUrl $ \port ->
        withTempFile $ \outputFile -> do
        callProcess step
            [ "--output", outputFile
            , "--host=localhost"
            , "--port=" <> show port
            , "--proposer=" <> T.unpack (getParty testProposer)
            , "--accepter=" <> T.unpack (getParty testAccepter)
            , "--note=" <> note
            , "--dar=" <> modelDar
            ]
        Result{..} <- either fail pure =<< A.eitherDecodeFileStrict' outputFile
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
        -- verify that the stream before and after the migration are the same.
        unless (prevTransactions == oldTransactions) $
            fail $ "Transaction stream changed after migration "
                <> show (prevTransactions, oldTransactions)
        -- verify that we created the right number of new transactions.
        unless (length newTransactions == length oldTransactions + 5) $
            fail $ "Expected 3 new transactions but got "
                <> show (length newTransactions - length oldTransactions)
        let (newStart, newEnd) = splitAt (length oldTransactions) newTransactions
        -- verify that the new stream is identical to the old if we cut off the new transactions.
        unless (newStart == oldTransactions) $
            fail $ "New transaction stream does not contain old transactions "
                <> show (oldTransactions, newStart)
        -- verify that the new transactions are what we expect.
        validateNewTransactions (map events newEnd)
        hPutStrLn stderr ("<-- Tested " <> note)
        pure (newTs, newTransactions)

validateNewTransactions :: [[Event]] -> IO ()
validateNewTransactions
  [ [CreatedTProposal prop1 _]
  , [CreatedTProposal prop2 _]
  , [ArchivedTProposal prop1',CreatedT t1 _]
  , [ArchivedTProposal prop2',CreatedT _t2 _]
  , [ArchivedT t1']
  ] = do
    checkArchive prop1 prop1'
    checkArchive prop2 prop2'
    checkArchive t1 t1'
  where
    checkArchive cid cid' = unless (cid == cid') $
      fail ("Expected " <> show cid <> " to be archived but got " <> show cid')
validateNewTransactions events = fail ("Unexpected events: " <> show events)

testProposer :: Party
testProposer = Party "proposer"

testAccepter :: Party
testAccepter = Party "accepter"

-- The datatypes are defined such that the autoderived Aeson instances
-- match the DAML-LF JSON encoding.

newtype ContractId t = ContractId T.Text
  deriving newtype A.FromJSON
  deriving stock (Eq, Show)
newtype Party = Party { getParty :: T.Text }
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
  } deriving (Generic, Show, Eq)

instance A.FromJSON TProposal

data Tuple2 a b = Tuple2
  { _1 :: a
  , _2 :: b
  } deriving (Eq, Generic, Show)

instance (A.FromJSON a, A.FromJSON b) => A.FromJSON (Tuple2 a b)

data Event
  = CreatedT (ContractId T) T
  | ArchivedT (ContractId T)
  | CreatedTProposal (ContractId TProposal) TProposal
  | ArchivedTProposal (ContractId TProposal)
  deriving (Eq, Show)

instance A.FromJSON Event where
    parseJSON = A.withObject "Event" $ \o -> do
        ty <- o A..: "type"
        tpl <- o A..: "template"
        case ty of
            "created" -> case tpl of
                "Model:T" -> CreatedT <$> o A..: "cid" <*> o A..: "argument"
                "Model:TProposal" -> CreatedTProposal <$> o A..: "cid" <*> o A..: "argument"
                _ -> fail ("Invalid template: " <> tpl)
            "archived" -> case tpl of
                "Model:T" -> ArchivedT <$> o A..: "cid"
                "Model:TProposal" -> ArchivedTProposal <$> o A..: "cid"
                _ -> fail ("Invalid template: " <> tpl)
            _ -> fail ("Invalid type: " <> ty)

data Transaction = Transaction
  { transactionId :: T.Text
  , events :: [Event]
  } deriving (Generic, Eq, Show)

instance A.FromJSON Transaction

data Result = Result
  { oldTProposals :: [Tuple2 (ContractId TProposal) TProposal]
  , newTProposals :: [Tuple2 (ContractId TProposal) TProposal]
  , oldTs :: [Tuple2 (ContractId T) T]
  , newTs :: [Tuple2 (ContractId T) T]
  , oldTransactions :: [Transaction]
  , newTransactions :: [Transaction]
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
