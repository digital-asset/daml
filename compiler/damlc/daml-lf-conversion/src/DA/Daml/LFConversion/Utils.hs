-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Utils used by both LFConversion and warnings
module DA.Daml.LFConversion.Utils (
  convNameLoc,
  convRealSrcSpan,
) where

import           "ghc-lib" GHC
import           DA.Daml.LF.Ast as LF

convNameLoc :: NamedThing a => a -> Maybe LF.SourceLoc
convNameLoc n = case nameSrcSpan (getName n) of
  -- NOTE(MH): Locations are 1-based in GHC and 0-based in Daml-LF.
  -- Hence all the @- 1@s below.
  RealSrcSpan srcSpan -> Just (convRealSrcSpan srcSpan)
  UnhelpfulSpan{} -> Nothing

convRealSrcSpan :: RealSrcSpan -> LF.SourceLoc
convRealSrcSpan srcSpan = SourceLoc
  { slocModuleRef = Nothing
  , slocStartLine = srcSpanStartLine srcSpan - 1
  , slocStartCol = srcSpanStartCol srcSpan - 1
  , slocEndLine = srcSpanEndLine srcSpan - 1
  , slocEndCol = srcSpanEndCol srcSpan - 1
  }
