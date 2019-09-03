-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MagicHash #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PatternSynonyms #-}
{-# OPTIONS_GHC -Wno-missing-fields #-} -- to enable prettyPrint
{-# OPTIONS_GHC -Wno-orphans #-}

-- | GHC utility functions. Importantly, code using our GHC should never:
--
-- * Call runGhc, use runGhcFast instead. It's faster and doesn't require config we don't have.
--
-- * Call setSessionDynFlags, use modifyDynFlags instead. It's faster and avoids loading packages.
module DA.Daml.LFConversion.UtilGHC(
    module DA.Daml.LFConversion.UtilGHC
    ) where

import           "ghc-lib" GHC                         hiding (convertLit)
import           "ghc-lib" GhcPlugins                  as GHC hiding (fst3, (<>))

import           Data.Generics.Uniplate.Data
import           Data.List
import           Data.Tuple.Extra
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Control.Exception
import GHC.Ptr(Ptr(..))
import System.IO.Unsafe


----------------------------------------------------------------------
-- GHC utility functions

getOccText :: NamedThing a => a -> T.Text
getOccText = fsToText . getOccFS

fsToText :: FastString -> T.Text
fsToText = T.decodeUtf8 . fastStringToByteString

pattern Is :: NamedThing a => FastString -> a
pattern Is x <- (occNameFS . getOccName -> x)

pattern VarIs :: FastString -> GHC.Expr Var
pattern VarIs x <- Var (Is x)

pattern TypeCon :: TyCon -> [GHC.Type] -> GHC.Type
pattern TypeCon c ts <- (splitTyConApp_maybe -> Just (c, ts))
  where TypeCon = mkTyConApp

pattern StrLitTy :: T.Text -> Type
pattern StrLitTy x <- (fmap fsToText . isStrLitTy -> Just x)

subst :: [(TyVar, GHC.Type)] -> GHC.Type -> GHC.Type
subst env = transform $ \t ->
    case getTyVar_maybe t of
        Just v | Just t <- lookup v env -> t
        _                               -> t

fromApps :: GHC.Expr b -> [Arg b]
fromApps (App x y) = fromApps x ++ [y]
fromApps x         = [x]

fromCast :: GHC.Expr a -> GHC.Expr a
fromCast (Cast x _) = x
fromCast x          = x

isType_maybe :: GHC.Expr b -> Maybe Type
isType_maybe = \case
    Type t -> Just t
    _ -> Nothing

isSingleConType :: TyCon -> Bool
isSingleConType t = case algTyConRhs t of
    DataTyCon [_] _ _ -> True
    NewTyCon{} -> True
    TupleTyCon{} -> True
    _ -> False

-- Pretty printing is very expensive, so clone the logic for when to add unique suffix
varPrettyPrint :: Var -> T.Text
varPrettyPrint (varName -> x) = getOccText x <> (if isSystemName x then "_" <> T.pack (show $ nameUnique x) else "")

defaultLast :: [Alt Var] -> [Alt Var]
defaultLast = uncurry (++) . partition ((/=) DEFAULT . fst3)

isLitAlt :: Alt Var -> Bool
isLitAlt (LitAlt{},_,_) = True
isLitAlt _              = False

untick :: GHC.Expr b -> GHC.Expr b
untick = \case
    Tick _ e -> untick e
    e -> e

-- | An argument to a function together with the location information of the
-- _function_.
type LArg b = (Maybe RealSrcSpan, Arg b)

pattern LType :: Type -> LArg b
pattern LType x <- (_, Type x)

pattern LExpr :: GHC.Expr b -> LArg b
pattern LExpr x <- (_, x)


-- | GHC uses unpackCString only when there are only
--   characters in the range 1..127 - so compatible with UTF8 too.
--   We just use the UTF8 decoder anyway.
unpackCString :: BS.ByteString -> T.Text
unpackCString = unpackCStringUtf8

-- | Convert a GHC encoded String literal to Text.
--   Problematic since GHC uses a weird encoding where NUL becomes
--   "\xc0\x80" and the string is NUL terminated.
--   Fortunately Text contains a helper for rewrite rules that deals with it.
unpackCStringUtf8 :: BS.ByteString -> T.Text
-- A "safe" unsafePerformIO since we copy into a strict result Text string
unpackCStringUtf8 bs = unsafePerformIO $
    BS.useAsCString bs $ \(Ptr a) -> do
        evaluate $ T.unpackCString# a
