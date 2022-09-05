-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}

module Development.IDE.Core.Rules.Daml.SpanInfo
    ( SpansInfo (..)
    , SpanInfo (..)
    , SpanType (..)
    , SpanSource (..)
    , SpanDoc (..)
    , getSpanInfo
    ) where

import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import Development.IDE.Core.Shake (use_)
import Development.IDE.Types.Location (NormalizedFilePath)
import Development.Shake (Action)
import GHC.Generics (Generic)

import "ghc-lib-parser" DynFlags (DynFlags)
import "ghc-lib-parser" Outputable (showPpr)
import "ghc-lib-parser" TyCoRep (Type)

import qualified Data.Text as T
import qualified Development.IDE.Core.RuleTypes as IDE
import qualified Development.IDE.Spans.Common as IDE
import qualified Development.IDE.Spans.Type as IDE

getSpanInfo :: DynFlags -> NormalizedFilePath -> Action SpansInfo
getSpanInfo dflags inputFile =
  convertSpansInfo dflags <$> use_ IDE.GetSpanInfo inputFile

convertSpansInfo :: DynFlags -> IDE.SpansInfo -> SpansInfo
convertSpansInfo dflags = convertSpansInfo'
  where
    convertSpansInfo' :: IDE.SpansInfo -> SpansInfo
    convertSpansInfo' spansInfo = SpansInfo
      { expressions = convertSpanInfo <$> IDE.spansExprs spansInfo
      , constraints = convertSpanInfo <$> IDE.spansConstraints spansInfo
      }

    convertSpanInfo :: IDE.SpanInfo -> SpanInfo
    convertSpanInfo spanInfo = SpanInfo
      { startLine = IDE.spaninfoStartLine spanInfo
      , startCol = IDE.spaninfoStartCol spanInfo
      , endLine = IDE.spaninfoEndLine spanInfo
      , endCol = IDE.spaninfoEndCol spanInfo
      , type_ = convertSpanType (IDE.spaninfoType spanInfo)
      , source = convertSpanSource (IDE.spaninfoSource spanInfo)
      , doc = convertSpanDoc (IDE.spaninfoDocs spanInfo)
      }

    convertSpanType :: Maybe Type -> SpanType
    convertSpanType = SpanType . \case
      Nothing -> "<missing>"
      Just ty -> T.pack (showPpr dflags ty)

    convertSpanSource :: IDE.SpanSource -> SpanSource
    convertSpanSource = \case
      IDE.Named name -> Named (T.pack (showPpr dflags name))
      IDE.SpanS span -> Span (T.pack (showPpr dflags span))
      IDE.Lit lit -> Lit (T.pack lit)
      IDE.NoSource -> NoSource

    convertSpanDoc :: IDE.SpanDoc -> SpanDoc
    convertSpanDoc = SpanDoc . \case
      IDE.SpanDocString ds -> T.lines (T.pack (showPpr dflags ds))
      IDE.SpanDocText ts -> ts

data SpansInfo = SpansInfo
  { expressions :: ![SpanInfo]
  , constraints :: ![SpanInfo]
  }
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (FromJSON, ToJSON)

data SpanInfo = SpanInfo
  { startLine :: !Int
  , startCol :: !Int
  , endLine :: !Int
  , endCol :: !Int
  , type_ :: !SpanType
  , source :: !SpanSource
  , doc :: !SpanDoc
  }
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (FromJSON, ToJSON)

newtype SpanType = SpanType Text
  deriving stock (Eq, Ord, Show, Generic)
  deriving newtype (FromJSON, ToJSON)

data SpanSource
  = Named !Text
  | Span !Text
  | Lit !Text
  | NoSource
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (FromJSON, ToJSON)

newtype SpanDoc = SpanDoc [Text]
  deriving stock (Eq, Ord, Show, Generic)
  deriving newtype (FromJSON, ToJSON)
