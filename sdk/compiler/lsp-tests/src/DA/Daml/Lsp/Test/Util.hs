-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE GADTs #-}
module DA.Daml.Lsp.Test.Util
    ( Cursor
    , cursorPosition
    , expectDiagnostics
    , damlId

    , openDocs
    , openDoc'
    , replaceDoc
    , waitForScriptDidChange
    , scriptUri
    , openScript
    , expectScriptContent
    , expectScriptContentMatch
    , module Language.LSP.Test
    ) where

import Control.Applicative.Combinators
import Control.Lens hiding (List)
import Control.Monad
import Control.Monad.IO.Class
import Data.Aeson (Result(..), fromJSON)
import qualified Data.Text as T
import Language.LSP.Test hiding (message)
import qualified Language.LSP.Test as LspTest
import Language.LSP.Types
import Language.LSP.Types.Lens as Lsp
import Network.URI
import System.Directory
import System.FilePath
import System.IO.Extra
import Test.Tasty.HUnit
import Text.Regex.TDFA

import Development.IDE.Test
import Development.IDE.Core.Rules.Daml (VirtualResourceChangedParams(..))


damlId :: String
damlId = "daml"

replaceDoc :: TextDocumentIdentifier -> T.Text -> Session ()
replaceDoc docId contents =
    changeDoc docId [TextDocumentContentChangeEvent Nothing Nothing contents]

-- | Wrapper around openDoc' that writes files to disc. This is
-- important for things like cyclic imports where we otherwise get an
-- error about the module not existing.
openDocs :: String -> [(FilePath, T.Text)] -> Session [TextDocumentIdentifier]
openDocs languageId files = do
    files' <- forM files $ \(file, contents) -> do
        uri <- getDocUri file
        Just path <- pure $ uriToFilePath uri
        liftIO $ createDirectoryIfMissing True (takeDirectory path)
        liftIO $ writeFileUTF8 path $ T.unpack contents
        let item = TextDocumentItem uri (T.pack languageId) 0 contents
        pure (TextDocumentIdentifier uri, item)
    forM_ files' $ \(_, item) -> sendNotification STextDocumentDidOpen (DidOpenTextDocumentParams item)
    pure (map fst files')

openDoc' :: FilePath -> String -> T.Text -> Session TextDocumentIdentifier
openDoc' file languageId contents = do
    uri <- getDocUri file
    let item = TextDocumentItem uri (T.pack languageId) 0 contents
    sendNotification STextDocumentDidOpen (DidOpenTextDocumentParams item)
    pure $ TextDocumentIdentifier uri

waitForScriptDidChange :: Session VirtualResourceChangedParams
waitForScriptDidChange = do
  NotMess script <- skipManyTill anyMessage scriptDidChange
  case fromJSON $ script ^. params of
      Success p -> pure p
      Error s -> fail $ "Failed to parse daml/virtualResource/didChange params: " <> s
  where scriptDidChange = LspTest.customNotification "daml/virtualResource/didChange"

scriptUri :: FilePath -> String -> Session Uri
scriptUri fp name = do
    Just fp' <- uriToFilePath <$> getDocUri fp
    pure $ Uri $ T.pack $
        "daml://compiler?file=" <> escapeURIString isUnescapedInURIComponent fp' <>
        "&top-level-decl=" <> name

openScript :: FilePath -> String -> Session TextDocumentIdentifier
openScript fp name = do
    uri <- scriptUri fp name
    sendNotification STextDocumentDidOpen $ DidOpenTextDocumentParams $
        TextDocumentItem uri (T.pack damlId) 0 ""
    pure $ TextDocumentIdentifier uri

expectScriptContent :: T.Text -> Session ()
expectScriptContent needle = do
    m <- waitForScriptDidChange
    liftIO $ assertBool
        ("Expected " <> show needle  <> " in " <> show (_vrcpContents m))
        (needle `T.isInfixOf` _vrcpContents m)

expectScriptContentMatch :: String -> Session ()
expectScriptContentMatch regexS = do
    (regex :: Regex) <- makeRegexM regexS
    m <- waitForScriptDidChange
    liftIO $ assertBool
        ("Expected " <> regexS  <> " to match " <> show (_vrcpContents m))
        (matchTest regex (_vrcpContents m))
