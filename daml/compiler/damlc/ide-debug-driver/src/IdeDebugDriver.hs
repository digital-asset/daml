-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE GADTs #-}
module IdeDebugDriver (main) where

import Control.Applicative.Combinators
import Control.Monad
import Data.Aeson
import Data.Foldable
import qualified Data.Text as T
import qualified Data.Yaml as Yaml
import qualified Language.LSP.Test as LSP
import Language.LSP.Types hiding (Command)
import Language.LSP.Types.Capabilities
import Options.Applicative

-- | We all love programming in YAML, don’t we? :)
data Command
    = OpenFile FilePath
    | CloseFile FilePath
    | WaitForCompletion
    | InsertLine FilePath Int T.Text
    | DeleteLine FilePath Int
    | Repeat Int [Command]
    deriving Show

instance FromJSON Command where
    parseJSON = withObject "Command" $ \o -> do
        cmd <- o .: "cmd"
        case cmd :: T.Text of
            "open" -> OpenFile <$> o .: "file"
            "close" -> CloseFile <$> o.: "file"
            "wait" -> pure WaitForCompletion
            "repeat" -> Repeat <$> o .: "count" <*> o .: "cmds"
            "insert-line" -> InsertLine <$> o .: "file" <*> o .: "line" <*> o .: "content"
            "delete-line" -> DeleteLine <$> o .: "file" <*> o .: "line"
            _ -> fail $ "Unknown command " <> show cmd

data SessionConfig = SessionConfig
    { ideShellCommand :: String
    , ideRoot :: FilePath
    , ideCommands :: [Command]
    } deriving Show

instance FromJSON SessionConfig where
    parseJSON = withObject "SessionConfig" $ \o ->
        SessionConfig
             <$> o .: "ide-cmd"
             <*> o .: "project-root"
             <*> o .: "commands"

data Opts = Opts
    { optConfigPath :: FilePath
    , optVerbose :: Verbose
    } deriving Show

newtype Verbose = Verbose Bool
    deriving Show

optsInfo :: ParserInfo Opts
optsInfo = info (parser <**> helper) fullDesc
  where
    parser = Opts
        <$> strOption (long "config" <> short 'c' <> metavar "FILE" <> help "Path to config file")
        <*> flag (Verbose False) (Verbose True) (long "verbose" <> short 'v' <> help "Enable verbose output")

main :: IO ()
main = do
    opts <- execParser optsInfo
    conf <- Yaml.decodeFileThrow (optConfigPath opts)
    runSession (optVerbose opts) (conf :: SessionConfig)

damlLanguageId :: T.Text
damlLanguageId = "daml"

runSession :: Verbose -> SessionConfig -> IO ()
runSession (Verbose verbose) SessionConfig{..} =
    LSP.runSessionWithConfig cnf ideShellCommand fullCaps' ideRoot $ traverse_ interpretCommand ideCommands
    where cnf = LSP.defaultConfig { LSP.logStdErr = verbose, LSP.logMessages = verbose }
          fullCaps' = LSP.fullCaps { _window = Just $ WindowClientCapabilities (Just True) Nothing Nothing }

progressStart :: LSP.Session ProgressToken
progressStart = LSP.satisfyMaybe $ \case
  FromServerMess SProgress (NotificationMessage _ _ (ProgressParams token (Begin _))) -> Just token
  _ -> Nothing

progressDone :: LSP.Session ProgressToken
progressDone = LSP.satisfyMaybe $ \case
  FromServerMess SProgress (NotificationMessage _ _ (ProgressParams token (End _))) -> Just token
  _ -> Nothing


interpretCommand :: Command -> LSP.Session ()
interpretCommand = \case
    OpenFile f -> void $ LSP.openDoc f damlLanguageId
    CloseFile f -> do
        uri <- LSP.getDocUri f
        LSP.closeDoc (TextDocumentIdentifier uri)
    WaitForCompletion -> do
        start <- progressStart
        skipManyTill LSP.anyMessage $ do
            done <- progressDone
            guard $ done == start
    Repeat count cmds -> replicateM_ count $ traverse_ interpretCommand cmds
    InsertLine f (fromIntegral -> l) t -> do
        uri <- LSP.getDocUri f
        let p = Position l 0
        LSP.changeDoc (TextDocumentIdentifier uri)
            [TextDocumentContentChangeEvent (Just $ Range p p) Nothing (t <> "\n")]
    DeleteLine f (fromIntegral -> l) -> do
        uri <- LSP.getDocUri f
        let pStart = Position l 0
        let pEnd = Position (l + 1) 0
        LSP.changeDoc (TextDocumentIdentifier uri)
            [TextDocumentContentChangeEvent (Just $ Range pStart pEnd) Nothing ""]
