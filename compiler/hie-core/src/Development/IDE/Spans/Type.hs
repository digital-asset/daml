-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- ORIGINALLY COPIED FROM https://github.com/commercialhaskell/intero

-- | Types used separate to GHCi vanilla.

module Development.IDE.Spans.Type(
    SpanInfo(..)
  , SpanSource(..)
  , getNameM
  ) where

import GHC
import Control.DeepSeq
import Data.Maybe
import OccName


-- | Type of some span of source code. Most of these fields are
-- unboxed but Haddock doesn't show that.
data SpanInfo =
  SpanInfo {spaninfoStartLine :: {-# UNPACK #-} !Int
            -- ^ Start line of the span, zero-based.
           ,spaninfoStartCol :: {-# UNPACK #-} !Int
            -- ^ Start column of the span, zero-based.
           ,spaninfoEndLine :: {-# UNPACK #-} !Int
            -- ^ End line of the span (absolute), zero-based.
           ,spaninfoEndCol :: {-# UNPACK #-} !Int
            -- ^ End column of the span (absolute), zero-based.
           ,spaninfoType :: !(Maybe Type)
            -- ^ A pretty-printed representation fo the type.
           ,spaninfoSource :: !SpanSource
           -- ^ The actutal 'Name' associated with the span, if
            -- any. This can be useful for accessing a variety of
            -- information about the identifier such as module,
            -- locality, definition location, etc.
           }
instance Show SpanInfo where
  show (SpanInfo sl sc el ec t n) = show [show sl, show sc, show el, show ec, show $ isJust t, show n]

instance NFData SpanInfo where
    rnf = rwhnf


-- we don't always get a name out so sometimes manually annotating source is more appropriate
data SpanSource = Named Name
                | SpanS SrcSpan
                | NoSource
  deriving (Eq)

instance Show SpanSource where
  show = \case
    Named n -> "Named " ++ occNameString (occName n)
    SpanS sp -> "Span " ++ show sp
    NoSource -> "NoSource"

getNameM :: SpanSource -> Maybe Name
getNameM = \case
  Named name -> Just name
  _ -> Nothing
