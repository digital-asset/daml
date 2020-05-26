-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module IdeDebugDriver (main) where

import Control.Applicative.Combinators
import Control.Lens
import Control.Monad
import Data.Aeson
import Data.Foldable
import qualified Data.Text as T
import qualified Data.Yaml as Yaml
import qualified Language.Haskell.LSP.Test as LSP
import Language.Haskell.LSP.Messages
import Language.Haskell.LSP.Types hiding (Command)
import Language.Haskell.LSP.Types.Capabilities
import Language.Haskell.LSP.Types.Lens
import qualified Language.Haskell.LSP.Types.Lens as LSP
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

damlLanguageId :: String
damlLanguageId = "daml"

runSession :: Verbose -> SessionConfig -> IO ()
runSession (Verbose verbose) SessionConfig{..} =
    LSP.runSessionWithConfig cnf ideShellCommand fullCaps' ideRoot $ traverse_ interpretCommand ideCommands
    where cnf = LSP.defaultConfig { LSP.logStdErr = verbose, LSP.logMessages = verbose }
          fullCaps' = LSP.fullCaps { _window = Just $ WindowClientCapabilities $ Just True }

progressStart :: LSP.Session WorkDoneProgressBeginNotification
progressStart = do
    NotWorkDoneProgressBegin not <- LSP.satisfy $ \case
      NotWorkDoneProgressBegin _ -> True
      _ -> False
    pure not

progressDone :: LSP.Session WorkDoneProgressEndNotification
progressDone = do
    NotWorkDoneProgressEnd not <- LSP.satisfy $ \case
      NotWorkDoneProgressEnd _ -> True
      _ -> False
    pure not

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
            guard $ done ^. params . LSP.token == start ^. params . LSP.token
    Repeat count cmds -> replicateM_ count $ traverse_ interpretCommand cmds
    InsertLine f l t -> do
        uri <- LSP.getDocUri f
        let p = Position l 0
        LSP.changeDoc (TextDocumentIdentifier uri)
            [TextDocumentContentChangeEvent (Just $ Range p p) Nothing (t <> "\n")]
    DeleteLine f l -> do
        uri <- LSP.getDocUri f
        let pStart = Position l 0
        let pEnd = Position (l + 1) 0
        LSP.changeDoc (TextDocumentIdentifier uri)
            [TextDocumentContentChangeEvent (Just $ Range pStart pEnd) Nothing ""]
