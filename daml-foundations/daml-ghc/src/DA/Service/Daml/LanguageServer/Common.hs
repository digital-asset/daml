-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude  #-}
{-# LANGUAGE OverloadedStrings  #-}

-- | Defaults and conversions between Ast types and protocol types
module DA.Service.Daml.LanguageServer.Common
    ( -- Defaults
      damlLanguageIdentifier

      -- * Conversions
    , documentUriToFilePath
    , absoluteFilePathToDocumentUri
    , convertDiagnostic

    -- * Location conversions
    , fromAstLocation
    , fromAstRange
    , fromAstPosition
    , toAstPosition

    , virtualResourceToCodeLens

      -- * Pretty printing
    , Pretty.renderPretty
    ) where

import           DA.LanguageServer.Protocol

import           DA.Prelude
import qualified DA.Pretty                        as Pretty

import qualified Data.Aeson                       as Aeson
import qualified Data.Text.Extended               as T

import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import qualified Development.IDE.Types.Diagnostics as Base
import qualified Development.IDE.Types.LSP as Compiler

import qualified Network.URI.Encode               as URI

------------------------------------------------------------------------------
-- Defaults
------------------------------------------------------------------------------

-- | DAML-Language identifier. Used to tell VS Code to highlight
--  certain things as DAML Code.
damlLanguageIdentifier :: LanguageIdentifier
damlLanguageIdentifier = Tagged "daml"


------------------------------------------------------------------------------
-- Conversions
------------------------------------------------------------------------------

-- | Convert a document URI back to a file path.
documentUriToFilePath :: DocumentUri -> Maybe FilePath
documentUriToFilePath (Tagged u)
  = adjust . URI.decode . T.unpack <$> T.stripPrefix "file://" u
  where
    adjust ('/':xs@(_:':':_)) = xs -- handle Windows paths
    adjust xs = xs


-- | Convert a file path for a URI for display.
absoluteFilePathToDocumentUri :: FilePath -> DocumentUri
absoluteFilePathToDocumentUri p
  = Tagged $ T.pack $ "file://" ++ URI.encode (adjust p)
  where
    adjust xs@('/':_) = xs
    adjust xs = '/' : xs -- handle Windows paths


-- | Convert an compiler build diagnostic.
convertDiagnostic :: Base.Diagnostic -> Diagnostic
convertDiagnostic Base.Diagnostic{..} =
    Diagnostic
        (convRange _range)
        (convSeverity <$> _severity)
        Nothing
        _source
        _message
  where
    convSeverity = \case
        Base.DsError -> Error
        Base.DsWarning -> Warning
        Base.DsInfo -> Information
        Base.DsHint -> Hint

    convRange (Base.Range start end) = Range (convPosition start) (convPosition end)
    convPosition (Base.Position line char) = Position line char


-- | Convert an AST location value.
fromAstLocation :: Base.Location -> Location
fromAstLocation (Base.Location uri range)
 = Location
    (Tagged $ Base.getUri uri)
    (fromAstRange range)


-- | Convert an AST range value.
fromAstRange :: Base.Range -> Range
fromAstRange (Base.Range startPos endPos) =
    Range (fromAstPosition startPos) (fromAstPosition endPos)


-- | Convert an AST position value.
fromAstPosition :: Base.Position -> Position
fromAstPosition (Base.Position line char) = Position line char


-- | Produce an AST position value.
toAstPosition :: Position -> Base.Position
toAstPosition (Position line char) = Base.Position line char


-- | Convert a compiler virtual resource into a code lens.
virtualResourceToCodeLens
    :: (Base.Range, T.Text, Compiler.VirtualResource)
    -> Maybe CodeLensEntry
virtualResourceToCodeLens (range, title, vr) =
 Just CodeLensEntry
    { cleRange = fromAstRange range
    , cleCommand = Just $ Command
        "Scenario results"
        "daml.showResource"
        (Just [ Aeson.String title
              , Aeson.String $ Compiler.virtualResourceToUri vr])
    , cleData = Nothing
    }
