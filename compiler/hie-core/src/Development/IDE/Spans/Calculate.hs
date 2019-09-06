-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- ORIGINALLY COPIED FROM https://github.com/commercialhaskell/intero

{-# LANGUAGE CPP #-}
{-# LANGUAGE RankNTypes #-}

-- | Get information on modules, identifiers, etc.

module Development.IDE.Spans.Calculate(getSrcSpanInfos,listifyAllSpans) where

import           ConLike
import           Control.Monad
import qualified CoreUtils
import           Data.Data
import qualified Data.Generics
import           Data.List
import           Data.Maybe
import           DataCon
import           Desugar
import           GHC
import           GhcMonad
import           FastString (mkFastString)
import           Development.IDE.Types.Location
import           Development.IDE.Spans.Type
import           Development.IDE.GHC.Error (zeroSpan)
import           Prelude hiding (mod)
import           TcHsSyn
import           Var
import Development.IDE.Core.Compile
import Development.IDE.GHC.Util


-- A lot of things gained an extra X argument in GHC 8.6, which we mostly ignore
-- this U ignores that arg in 8.6, but is hidden in 8.4
#if __GLASGOW_HASKELL__ >= 806
#define U _
#else
#define U
#endif

-- | Get source span info, used for e.g. AtPoint and Goto Definition.
getSrcSpanInfos
    :: HscEnv
    -> [(Located ModuleName, Maybe NormalizedFilePath)]
    -> TcModuleResult
    -> IO [SpanInfo]
getSrcSpanInfos env imports tc =
    runGhcEnv env
        . getSpanInfo imports
        $ tmrModule tc

-- | Get ALL source spans in the module.
getSpanInfo :: GhcMonad m
            => [(Located ModuleName, Maybe NormalizedFilePath)] -- ^ imports
            -> TypecheckedModule
            -> m [SpanInfo]
getSpanInfo mods tcm =
  do let tcs = tm_typechecked_source tcm
         bs  = listifyAllSpans  tcs :: [LHsBind GhcTc]
         es  = listifyAllSpans  tcs :: [LHsExpr GhcTc]
         ps  = listifyAllSpans' tcs :: [Pat GhcTc]
         ts  = listifyAllSpans $ tm_renamed_source tcm :: [LHsType GhcRn]
     bts <- mapM (getTypeLHsBind tcm) bs -- binds
     ets <- mapM (getTypeLHsExpr tcm) es -- expressions
     pts <- mapM (getTypeLPat tcm)    ps -- patterns
     tts <- mapM (getLHsType tcm)     ts -- types
     let imports = importInfo mods
     let exports = getExports tcm
     let exprs = exports ++ imports ++ concat bts ++ concat tts ++ catMaybes (ets ++ pts)
     return (mapMaybe toSpanInfo (sortBy cmp exprs))
  where cmp (_,a,_) (_,b,_)
          | a `isSubspanOf` b = LT
          | b `isSubspanOf` a = GT
          | otherwise = EQ

getExports :: TypecheckedModule -> [(SpanSource, SrcSpan, Maybe Type)]
getExports m
    | Just (_, _, Just exports, _) <- renamedSource m =
    [ (Named $ unLoc n, getLoc n, Nothing)
    | (e, _) <- exports
    , n <- ieLNames $ unLoc e
    ]
getExports _ = []

-- | Variant of GHC's ieNames that produces LIdP instead of IdP
ieLNames :: IE pass -> [Located (IdP pass)]
ieLNames (IEVar       U n   )     = [ieLWrappedName n]
ieLNames (IEThingAbs  U n   )     = [ieLWrappedName n]
ieLNames (IEThingAll  U n   )     = [ieLWrappedName n]
ieLNames (IEThingWith U n _ ns _) = ieLWrappedName n : map ieLWrappedName ns
ieLNames _ = []

-- | Get the name and type of a binding.
getTypeLHsBind :: (GhcMonad m)
               => TypecheckedModule
               -> LHsBind GhcTc
               -> m [(SpanSource, SrcSpan, Maybe Type)]
getTypeLHsBind _ (L _spn FunBind{fun_id = pid,fun_matches = MG{}}) =
  return [(Named $ getName (unLoc pid), getLoc pid, Just (varType (unLoc pid)))]
getTypeLHsBind _ _ = return []

-- | Get the name and type of an expression.
getTypeLHsExpr :: (GhcMonad m)
               => TypecheckedModule
               -> LHsExpr GhcTc
               -> m (Maybe (SpanSource, SrcSpan, Maybe Type))
getTypeLHsExpr _ e = do
  hs_env <- getSession
  (_, mbe) <- liftIO (deSugarExpr hs_env e)
  return $
    case mbe of
      Just expr ->
        Just (getSpanSource (unLoc e), getLoc e, Just (CoreUtils.exprType expr))
      Nothing -> Nothing
  where
    getSpanSource :: HsExpr GhcTc -> SpanSource
    getSpanSource (HsVar U (L _ i)) = Named (getName i)
    getSpanSource (HsConLikeOut U (RealDataCon dc)) = Named (dataConName dc)
    getSpanSource RecordCon {rcon_con_name} = Named (getName rcon_con_name)
    getSpanSource (HsWrap U _ xpr) = getSpanSource xpr
    getSpanSource (HsPar U xpr) = getSpanSource (unLoc xpr)
    getSpanSource _ =  NoSource

-- | Get the name and type of a pattern.
getTypeLPat :: (GhcMonad m)
            => TypecheckedModule
            -> Pat GhcTc
            -> m (Maybe (SpanSource, SrcSpan, Maybe Type))
getTypeLPat _ pat =
  let (src, spn) = getSpanSource pat in
  return $ Just (src, spn, Just (hsPatType pat))
  where
    getSpanSource :: Pat GhcTc -> (SpanSource, SrcSpan)
    getSpanSource (VarPat U (L spn vid)) = (Named (getName vid), spn)
    getSpanSource (ConPatOut (L spn (RealDataCon dc)) _ _ _ _ _ _) =
      (Named (dataConName dc), spn)
    getSpanSource _ = (NoSource, noSrcSpan)

getLHsType
    :: GhcMonad m
    => TypecheckedModule
    -> LHsType GhcRn
    -> m [(SpanSource, SrcSpan, Maybe Type)]
getLHsType _ (L spn (HsTyVar U _ v)) = pure [(Named $ unLoc v, spn, Nothing)]
getLHsType _ _ = pure []

importInfo :: [(Located ModuleName, Maybe NormalizedFilePath)]
           -> [(SpanSource, SrcSpan, Maybe Type)]
importInfo = mapMaybe (uncurry wrk) where
  wrk :: Located ModuleName -> Maybe NormalizedFilePath -> Maybe (SpanSource, SrcSpan, Maybe Type)
  wrk modName = \case
    Nothing -> Nothing
    Just fp -> Just (fpToSpanSource $ fromNormalizedFilePath fp, getLoc modName, Nothing)

  -- TODO make this point to the module name
  fpToSpanSource :: FilePath -> SpanSource
  fpToSpanSource fp = SpanS $ RealSrcSpan $ zeroSpan $ mkFastString fp

-- | Get ALL source spans in the source.
listifyAllSpans :: (Typeable a, Data m) => m -> [Located a]
listifyAllSpans tcs =
  Data.Generics.listify p tcs
  where p (L spn _) = isGoodSrcSpan spn
-- This is a version of `listifyAllSpans` specialized on picking out
-- patterns.  It comes about since GHC now defines `type LPat p = Pat
-- p` (no top-level locations).
listifyAllSpans' :: Typeable a
                   => TypecheckedSource -> [Pat a]
listifyAllSpans' tcs = Data.Generics.listify (const True) tcs


-- | Pretty print the types into a 'SpanInfo'.
toSpanInfo :: (SpanSource, SrcSpan, Maybe Type) -> Maybe SpanInfo
toSpanInfo (name,mspan,typ) =
  case mspan of
    RealSrcSpan spn ->
      -- GHC’s line and column numbers are 1-based while LSP’s line and column
      -- numbers are 0-based.
      Just (SpanInfo (srcSpanStartLine spn - 1)
                     (srcSpanStartCol spn - 1)
                     (srcSpanEndLine spn - 1)
                     (srcSpanEndCol spn - 1)
                     typ
                     name)
    _ -> Nothing
