-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module Daml.Lsp.Test.Util
    ( Cursor
    , cursorPosition
    , expectDiagnostics
    , damlId
    , openDoc'
    , replaceDoc
    , module Language.Haskell.LSP.Test
    ) where

import Control.Applicative.Combinators
import Control.Lens
import Control.Monad
import Control.Monad.IO.Class
import qualified Data.Text as T
import Language.Haskell.LSP.Test hiding (message, openDoc')
import qualified Language.Haskell.LSP.Test as LspTest
import Language.Haskell.LSP.Types
import Language.Haskell.LSP.Types.Lens
import Test.Tasty.HUnit

import DA.Test.Util
import Development.IDE.Types.Diagnostics

-- | Convenient grouping of 0-based line number, 0-based column number.
type Cursor = (Int, Int)

cursorPosition :: Cursor -> Position
cursorPosition (line,  col) = Position line col

searchDiagnostics :: (DiagnosticSeverity, Cursor, T.Text) -> PublishDiagnosticsParams -> Assertion
searchDiagnostics expected@(severity, cursor, expectedMsg) actuals =
    unless (any match (actuals ^. diagnostics)) $
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

expectDiagnostics :: [(DiagnosticSeverity, Cursor, T.Text)] -> Session ()
expectDiagnostics expected = do
    diagsNot <- skipManyTill anyMessage LspTest.message :: Session PublishDiagnosticsNotification
    let actuals = diagsNot ^. params
    liftIO $ forM_ expected $ \e -> searchDiagnostics e actuals
    liftIO $ unless (length expected == length (actuals ^. diagnostics)) $
        assertFailure $
            "Incorrect number of diagnostics, expected " <>
            show expected <> " but got " <> show actuals

damlId :: String
damlId = "daml"

replaceDoc :: TextDocumentIdentifier -> T.Text -> Session ()
replaceDoc docId contents =
    changeDoc docId [TextDocumentContentChangeEvent Nothing Nothing contents]


openDoc' :: FilePath -> String -> T.Text -> Session TextDocumentIdentifier
openDoc' file languageId contents = do
    uri <- getDocUri file
    Just fp <- pure $ uriToFilePath uri
    -- We have our own version of openDoc to ensure that it uses filePathToUri'
    let uri = filePathToUri' fp
    let item = TextDocumentItem uri (T.pack languageId) 0 contents
    sendNotification TextDocumentDidOpen (DidOpenTextDocumentParams item)
    pure $ TextDocumentIdentifier uri
