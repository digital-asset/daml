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

import "ghc-lib" GHC hiding (convertLit)
import "ghc-lib" GhcPlugins as GHC hiding (fst3, (<>))

import Data.Generics.Uniplate.Data
import Data.Maybe
import Data.List
import Data.Tuple.Extra
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Control.Exception
import GHC.Ptr(Ptr(..))
import System.IO.Unsafe
import Text.Read (readMaybe)
import Control.Monad (guard)


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

pattern NameIn :: NamedThing a => GHC.Module -> FastString -> a
pattern NameIn m x <- (\n -> (nameModule_maybe (getName n), getOccFS n) -> (Just m, x))

pattern VarIn :: GHC.Module -> FastString -> GHC.Expr Var
pattern VarIn m x <- Var (NameIn m x)

pattern ModuleIn :: GHC.UnitId -> FastString -> GHC.Module
pattern ModuleIn u n <- (\m -> (moduleUnitId m, GHC.moduleNameFS (GHC.moduleName m)) -> (u, n))

-- builtin unit id patterns
pattern DamlPrim, DamlStdlib :: GHC.UnitId
pattern DamlPrim <- ((== primUnitId) -> True)
pattern DamlStdlib <- (T.stripPrefix "daml-stdlib-" . fsToText . unitIdFS -> Just _)
    -- The unit name for daml-stdlib includes the SDK version.
    -- This pattern accepts all SDK versions for daml-stdlib.

pattern IgnoreWorkerPrefix :: T.Text -> T.Text
pattern IgnoreWorkerPrefix n <- (\w -> fromMaybe w (T.stripPrefix "$W" w) -> n)

pattern IgnoreWorkerPrefixFS :: T.Text -> FastString
pattern IgnoreWorkerPrefixFS n <- (fsToText -> IgnoreWorkerPrefix n)

-- daml-prim module patterns
pattern Control_Exception_Base, Data_String, GHC_Base, GHC_Classes, GHC_CString, GHC_Integer_Type, GHC_Num, GHC_Prim, GHC_Real, GHC_Tuple, GHC_Types :: GHC.Module
pattern Control_Exception_Base <- ModuleIn DamlPrim "Control.Exception.Base"
pattern Data_String <- ModuleIn DamlPrim "Data.String"
pattern GHC_Base <- ModuleIn DamlPrim "GHC.Base"
pattern GHC_Classes <- ModuleIn DamlPrim "GHC.Classes"
pattern GHC_CString <- ModuleIn DamlPrim "GHC.CString"
pattern GHC_Integer_Type <- ModuleIn DamlPrim "GHC.Integer.Type"
pattern GHC_Num <- ModuleIn DamlPrim "GHC.Num"
pattern GHC_Prim <- ModuleIn DamlPrim "GHC.Prim" -- wired-in by GHC
pattern GHC_Real <- ModuleIn DamlPrim "GHC.Real"
pattern GHC_Tuple <- ModuleIn DamlPrim "GHC.Tuple"
pattern GHC_Types <- ModuleIn DamlPrim "GHC.Types"

-- daml-stdlib module patterns
pattern DA_Action, DA_Generics, DA_Internal_LF, DA_Internal_Prelude, DA_Internal_Record :: GHC.Module
pattern DA_Action <- ModuleIn DamlStdlib "DA.Action"
pattern DA_Generics <- ModuleIn DamlStdlib "DA.Generics"
pattern DA_Internal_LF <- ModuleIn DamlStdlib "DA.Internal.LF"
pattern DA_Internal_Prelude <- ModuleIn DamlStdlib "DA.Internal.Prelude"
pattern DA_Internal_Record <- ModuleIn DamlStdlib "DA.Internal.Record"

-- | Break down a constraint tuple projection function name.
-- These have the form "$p1(%,%)" "$p2(%,%)" "$p1(%,,%)" etc.
constraintTupleProjection_maybe :: T.Text -> Maybe (Int, Int)
constraintTupleProjection_maybe t1 = do
    t2 <- T.stripPrefix "$p" t1
    t3 <- T.stripSuffix "%)" t2
    let (tIndex, tRest) = T.breakOn "(%" t3
    tCommas <- T.stripPrefix "(%" tRest
    guard (all (== ',') (T.unpack tCommas))
    index <- readMaybe (T.unpack tIndex)
    pure (index, T.length tCommas + 1)

pattern ConstraintTupleProjectionFS :: Int -> Int -> FastString
pattern ConstraintTupleProjectionFS i j <-
    (constraintTupleProjection_maybe . fsToText -> Just (i,j))

pattern ConstraintTupleProjectionName :: Int -> Int -> Var
pattern ConstraintTupleProjectionName i j <-
    NameIn GHC_Classes (ConstraintTupleProjectionFS i j)

pattern ConstraintTupleProjection :: Int -> Int -> GHC.Expr Var
pattern ConstraintTupleProjection i j <-
    Var (ConstraintTupleProjectionName i j)


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

hasDamlEnumCtx :: TyCon -> Bool
hasDamlEnumCtx t
    | [theta] <- tyConStupidTheta t
    , TypeCon tycon [] <- theta
    , NameIn GHC_Types "DamlEnum" <- tycon
    = True

    | otherwise
    = False

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
