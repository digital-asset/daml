-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module Daml.Lsp.Test.Util
    ( Cursor
    , cursorPosition
    , expectDiagnostics
    , damlId

    , openDocs
    , openDoc'
    , replaceDoc
    , waitForScenarioDidChange
    , scenarioUri
    , openScenario
    , expectScenarioContent
    , module Language.Haskell.LSP.Test
    ) where

import Control.Applicative.Combinators
import Control.Lens hiding (List)
import Control.Monad
import Control.Monad.IO.Class
import Data.Aeson (Result(..), fromJSON)
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Language.Haskell.LSP.Test hiding (message, openDoc')
import qualified Language.Haskell.LSP.Test as LspTest
import Language.Haskell.LSP.Types
import Language.Haskell.LSP.Types.Lens as Lsp
import Network.URI
import System.IO.Extra
import Test.Tasty.HUnit

import DA.Test.Util
import Development.IDE.State.Rules.Daml (VirtualResourceChangedParams(..))

-- | (0-based line number, 0-based column number)
type Cursor = (Int, Int)

cursorPosition :: Cursor -> Position
cursorPosition (line,  col) = Position line col

requireDiagnostic :: List Diagnostic -> (DiagnosticSeverity, Cursor, T.Text) -> Assertion
requireDiagnostic actuals expected@(severity, cursor, expectedMsg) = do
    unless (any match actuals) $
        assertFailure $
            "Could not find " <> show expected <>
            " in " <> show actuals
  where
    match :: Diagnostic -> Bool
    match d =
        Just severity == _severity d
        && cursorPosition cursor == d ^. range . start
        && standardizeQuotes (T.toLower expectedMsg) `T.isInfixOf`
           standardizeQuotes (T.toLower $ d ^. message)

expectDiagnostics :: [(FilePath, [(DiagnosticSeverity, Cursor, T.Text)])] -> Session ()
expectDiagnostics expected = do
    expected' <- Map.fromListWith (<>) <$> traverseOf (traverse . _1) (fmap toNormalizedUri . getDocUri) expected
    go expected'
    where
        go m
            | Map.null m = pure ()
            | otherwise = do
                  diagsNot <- skipManyTill anyMessage LspTest.message :: Session PublishDiagnosticsNotification
                  let fileUri = diagsNot ^. params . uri
                  case Map.lookup (diagsNot ^. params . uri . to toNormalizedUri) m of
                      Nothing -> liftIO $ assertFailure $
                          "Got diagnostics for " <> show fileUri <>
                          " but only expected diagnostics for " <> show (Map.keys m)
                      Just expected -> do
                          let actual = diagsNot ^. params . diagnostics
                          liftIO $ mapM_ (requireDiagnostic actual) expected
                          liftIO $ unless (length expected == length actual) $
                              assertFailure $
                              "Incorrect number of diagnostics for " <> show fileUri <>
                              ", expected " <> show expected <>
                              " but got " <> show actual
                          go $ Map.delete (diagsNot ^. params . uri . to toNormalizedUri) m

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
        liftIO $ writeFileUTF8 path $ T.unpack contents
        let item = TextDocumentItem uri (T.pack languageId) 0 contents
        pure (TextDocumentIdentifier uri, item)
    forM_ files' $ \(_, item) -> sendNotification TextDocumentDidOpen (DidOpenTextDocumentParams item)
    pure (map fst files')

openDoc' :: FilePath -> String -> T.Text -> Session TextDocumentIdentifier
openDoc' file languageId contents = do
    uri <- getDocUri file
    let item = TextDocumentItem uri (T.pack languageId) 0 contents
    sendNotification TextDocumentDidOpen (DidOpenTextDocumentParams item)
    pure $ TextDocumentIdentifier uri

waitForScenarioDidChange :: Session VirtualResourceChangedParams
waitForScenarioDidChange = do
  scenario <- skipManyTill anyMessage scenarioDidChange
  case fromJSON $ scenario ^. params of
      Success p -> pure p
      Error s -> fail $ "Failed to parse daml/virtualResource/didChange params: " <> s
  where scenarioDidChange = do
            m <- LspTest.message :: Session CustomServerNotification
            guard (m ^. method == CustomServerMethod "daml/virtualResource/didChange")
            pure m

scenarioUri :: FilePath -> String -> Session Uri
scenarioUri fp name = do
    Just fp' <- uriToFilePath <$> getDocUri fp
    pure $ Uri $ T.pack $
        "daml://compiler?file=" <> escapeURIString isUnescapedInURIComponent fp' <>
        "&top-level-decl=" <> name

openScenario :: FilePath -> String -> Session TextDocumentIdentifier
openScenario fp name = do
    uri <- scenarioUri fp name
    sendNotification TextDocumentDidOpen $ DidOpenTextDocumentParams $
        TextDocumentItem uri (T.pack damlId) 0 ""
    pure $ TextDocumentIdentifier uri

expectScenarioContent :: T.Text -> Session ()
expectScenarioContent needle = do
    m <- waitForScenarioDidChange
    liftIO $ assertBool
        ("Expected " <> show needle  <> " in " <> show (_vrcpContents m))
        (needle `T.isInfixOf` _vrcpContents m)
