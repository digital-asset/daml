-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-} -- Because the pattern match checker is garbage


-- | The DAML-LF primitives, matched with their type, and using 'primitive' on the libraries side.
module DA.Daml.GHC.Compiler.Primitives(convertPrim) where

import           DA.Daml.GHC.Compiler.UtilLF
import           DA.Daml.LF.Ast
import           DA.Pretty (renderPretty)
import           DA.Prelude (Tagged(..))
import qualified Data.Text as T

convertPrim :: Version -> String -> Type -> Expr
-- Update
convertPrim _ "UPure" (a1 :-> TUpdate a2) | a1 == a2 =
    ETmLam (varV1, a1) $ EUpdate $ UPure a1 $ EVar varV1
convertPrim _ "UBind" (t1@(TUpdate a1) :-> t2@(a2 :-> TUpdate b1) :-> TUpdate b2) | a1 == a2, b1 == b2 =
    ETmLam (varV1, t1) $ ETmLam (varV2, t2) $ EUpdate $ UBind (Binding (varV3, a1) (EVar varV1)) (EVar varV2 `ETmApp` EVar varV3)
convertPrim _ "UAbort" (TText :-> t@(TUpdate a)) =
    ETmLam (varV1, TText) $ EUpdate (UEmbedExpr a (EBuiltin BEError `ETyApp` t `ETmApp` EVar varV1))
convertPrim _ "UGetTime" (TUpdate TTimestamp) =
    EUpdate UGetTime

-- Scenario
convertPrim _ "SPure" (a1 :-> TScenario a2) | a1 == a2 =
    ETmLam (varV1, a1) $ EScenario $ SPure a1 $ EVar varV1
convertPrim _ "SBind" (t1@(TScenario a1) :-> t2@(a2 :-> TScenario b1) :-> TScenario b2) | a1 == a2, b1 == b2 =
    ETmLam (varV1, t1) $ ETmLam (varV2, t2) $ EScenario $ SBind (Binding (varV3, a1) (EVar varV1)) (EVar varV2 `ETmApp` EVar varV3)
convertPrim _ "SAbort" (TText :-> t@(TScenario a)) =
    ETmLam (varV1, TText) $ EScenario (SEmbedExpr a (EBuiltin BEError `ETyApp` t `ETmApp` EVar varV1))
convertPrim _ "SCommit" (t1@TParty :-> t2@(TUpdate a1) :-> TScenario a2) | a1 == a2 =
    ETmLam (varV1, t1) $ ETmLam (varV2, t2) $ EScenario $ SCommit a1 (EVar varV1) (EVar varV2)
convertPrim _ "SMustFailAt" (t1@TParty :-> t2@(TUpdate a1) :-> TScenario TUnit) =
    ETmLam (varV1, t1) $ ETmLam (varV2, t2) $ EScenario $ SMustFailAt a1 (EVar varV1) (EVar varV2)
convertPrim _ "SPass" (t1@TInt64 :-> TScenario TTimestamp) =
    ETmLam (varV1, t1) $ EScenario $ SPass $ EVar varV1
convertPrim _ "SGetTime" (TScenario TTimestamp) =
    EScenario SGetTime
convertPrim _ "SGetParty" (t1@TText :-> TScenario TParty) =
    ETmLam (varV1, t1) $ EScenario $ SGetParty $ EVar varV1

-- Comparison
convertPrim _"BEEqual" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    EBuiltin $ BEEqual a1
convertPrim version "BELess" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    convertCompare version BELess a1
convertPrim version "BELessEq" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    convertCompare version BELessEq a1
convertPrim version "BEGreaterEq" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    convertCompare version BEGreaterEq a1
convertPrim version "BEGreater" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    convertCompare version BEGreater a1
convertPrim _ "BEEqualList" ((a1 :-> a2 :-> TBool) :-> TList a3 :-> TList a4 :-> TBool) | a1 == a2, a2 == a3, a3 == a4 =
    EBuiltin BEEqualList `ETyApp` a1
convertPrim _ "BEEqualContractId" (TContractId a1 :-> TContractId a2 :-> TBool) | a1 == a2 =
    EBuiltin BEEqualContractId `ETyApp` a1

-- Decimal arithmetic
convertPrim _ "BEAddDecimal" (TDecimal :-> TDecimal :-> TDecimal) =
    EBuiltin BEAddDecimal
convertPrim _ "BESubDecimal" (TDecimal :-> TDecimal :-> TDecimal) =
    EBuiltin BESubDecimal
convertPrim _ "BEMulDecimal" (TDecimal :-> TDecimal :-> TDecimal) =
    EBuiltin BEMulDecimal
convertPrim _ "BEDivDecimal" (TDecimal :-> TDecimal :-> TDecimal) =
    EBuiltin BEDivDecimal
convertPrim _ "BERoundDecimal" (TInt64 :-> TDecimal :-> TDecimal) =
    EBuiltin BERoundDecimal

-- Integer arithmetic
convertPrim _ "BEAddInt64" (TInt64 :-> TInt64 :-> TInt64) =
    EBuiltin BEAddInt64
convertPrim _ "BESubInt64" (TInt64 :-> TInt64 :-> TInt64) =
    EBuiltin BESubInt64
convertPrim _ "BEMulInt64" (TInt64 :-> TInt64 :-> TInt64) =
    EBuiltin BEMulInt64
convertPrim _ "BEDivInt64" (TInt64 :-> TInt64 :-> TInt64) =
    EBuiltin BEDivInt64
convertPrim _ "BEModInt64" (TInt64 :-> TInt64 :-> TInt64) =
    EBuiltin BEModInt64
convertPrim _ "BEExpInt64" (TInt64 :-> TInt64 :-> TInt64) =
    EBuiltin BEExpInt64

-- Time arithmetic
convertPrim _ "BETimestampToUnixMicroseconds" (TTimestamp :-> TInt64) =
    EBuiltin BETimestampToUnixMicroseconds
convertPrim _ "BEUnixMicrosecondsToTimestamp" (TInt64 :-> TTimestamp) =
    EBuiltin BEUnixMicrosecondsToTimestamp
convertPrim _ "BEDateToUnixDays" (TDate :-> TInt64) =
    EBuiltin BEDateToUnixDays
convertPrim _ "BEUnixDaysToDate" (TInt64 :-> TDate) =
    EBuiltin BEUnixDaysToDate

-- Conversion to and from Decimal
convertPrim _ "BEInt64ToDecimal" (TInt64 :-> TDecimal) =
    EBuiltin BEInt64ToDecimal
convertPrim _ "BEDecimalToInt64" (TDecimal :-> TInt64) =
    EBuiltin BEDecimalToInt64

-- List operations
convertPrim _ "BEFoldl" ((b1 :-> a1 :-> b2) :-> b3 :-> TList a2 :-> b4) | a1 == a2, b1 == b2, b2 == b3, b3 == b4 =
    EBuiltin BEFoldl `ETyApp` a1 `ETyApp` b1
convertPrim _ "BEFoldr" ((a1 :-> b1 :-> b2) :-> b3 :-> TList a2 :-> b4) | a1 == a2, b1 == b2, b2 == b3, b3 == b4 =
    EBuiltin BEFoldr `ETyApp` a1 `ETyApp` b1

-- Error
convertPrim _ "BEError" (TText :-> t2) =
    ETyApp (EBuiltin BEError) t2

-- Text operations
convertPrim _ "BEToText" (TBuiltin x :-> TText) =
    EBuiltin $ BEToText x
convertPrim _ "BEExplodeText" (TText :-> TList TText) =
    EBuiltin BEExplodeText
convertPrim _ "BEImplodeText" (TList TText :-> TText) =
    EBuiltin BEImplodeText
convertPrim _ "BEAppendText" (TText :-> TText :-> TText) =
    EBuiltin BEAppendText
convertPrim _ "BETrace" (TText :-> a1 :-> a2) | a1 == a2 =
    EBuiltin BETrace `ETyApp` a1
convertPrim version "BESha256Text" (TText :-> TText) =
    if supportsSha256Text version
      then EBuiltin BESha256Text
      -- compile also if we do not support it, so that we can have DA.Text.sha256
      -- compiled without generating invalid packages regardless.
      else mkETmLams
        [(varV1, TText)]
        (ETmApp
          (ETyApp (EBuiltin BEError) TText)
          (EBuiltin (BEText "SHA256_TEXT only supported when compiling to DAML-LF >= 1.2")))
convertPrim _ "BEPartyToQuotedText" (TParty :-> TText) =
    EBuiltin BEPartyToQuotedText
convertPrim version "BEPartyFromText" (TText :-> TOptional TParty) =
    if supportsPartyFromText version
      then EBuiltin BEPartyFromText
      -- compile also if we do not support it, so that we can have DA.Internal.LF.partyFromText
      -- compiled without generating invalid packages regardless.
      else runtimeUnsupportedPartyFromText (TOptional TParty)
convertPrim version "BEPartyFromText" (TText :-> optionalParty)
  | not (supportsOptional version)
  , TApp (TCon (Qualified _pkg mod_ typ)) TParty <- optionalParty
  , mod_ == Tagged ["DA", "Internal", "Prelude"]
  , typ == Tagged ["Optional"] =
    -- before DAML-LF 1.1 we didn't even have a built in optional optional yet,
    -- so we need an additional case here.
    runtimeUnsupportedPartyFromText optionalParty

-- Map operations

convertPrim _ "BEMapEmpty" (TMap a) =
  EBuiltin BEMapEmpty `ETyApp` a
convertPrim _ "BEMapEmpty" t@(TextMap_ _ _) =
  runtimeUnsupported "BEMapEmpty" "1.3" t
convertPrim _ "BEMapInsert"  (TText :-> a1 :-> TMap a2 :-> TMap a3) | a1 == a2, a2 == a3 =
  EBuiltin BEMapInsert `ETyApp` a1
convertPrim _ "BEMapInsert"  t@(TText :-> a1 :-> TextMap_ _ a2 :-> TextMap_ _ a3) | a1 == a2, a2 == a3 =
  runtimeUnsupported "BAMapInsert" "1.3" t
convertPrim _ "BEMapLookup" (TText :-> TMap a1 :-> TOptional a2) | a1 == a2 =
  EBuiltin BEMapLookup `ETyApp` a1
convertPrim _ "BEMapLookup" t@(TText :-> TextMap_ _ a1 :-> TOptional a2) | a1 == a2 =
  runtimeUnsupported "BEMapLookup" "1.3" t
convertPrim _ "BEMapLookup" t@(TText :-> TextMap_ _ a1 :-> TOptional_ _ a2) | a1 == a2 =
    runtimeUnsupported "BEMapLookup" "1.3" t
convertPrim _ "BEMapDelete" (TText :-> TMap a1 :-> TMap a2) | a1 == a2 =
  EBuiltin BEMapDelete `ETyApp` a1
convertPrim _ "BEMapDelete" t@(TText :-> TextMap_ _ a1 :-> TextMap_ _ a2) | a1 == a2 =
  runtimeUnsupported "BAMapDelete" "1.3" t
convertPrim _ "BEMapToList" (TMap a1 :-> TList (TMapEntry a2)) | a1 == a2  =
  EBuiltin BEMapToList `ETyApp` a1
convertPrim _ "BEMapToList" t@(TextMap_ _ a1 :-> TList (TMapEntry a2)) | a1 == a2 =
  runtimeUnsupported "BEMapToList" "1.3" t
convertPrim _ "BEMapSize" (TMap a :-> TInt64) =
  EBuiltin BEMapSize `ETyApp` a
convertPrim _ "BEMapSize" t@(TextMap_ _ _ :-> TInt64) =
  runtimeUnsupported "BAMapSize" "1.3" t

convertPrim _ x ty = error $ "Unknown primitive " ++ show x ++ " at type " ++ renderPretty ty

pattern TextMap_ :: PackageRef -> Type -> Type
pattern TextMap_ pkg a =
  TApp
  (TCon (Qualified pkg (Tagged ["DA", "Internal", "Prelude"]) (Tagged ["TextMap"])))
  a

pattern TOptional_ :: PackageRef -> Type -> Type
pattern TOptional_ pkg a =
  TApp
  (TCon (Qualified pkg (Tagged ["DA", "Internal", "Prelude"]) (Tagged ["Optional"])))
  a


runtimeUnsupported :: T.Text -> T.Text -> Type -> Expr
runtimeUnsupported msg v (tArg :-> tFun) =
  mkETmLams
    [(varV1, tArg)]
    (runtimeUnsupported msg v tFun)
runtimeUnsupported msg v t =
  ETmApp
  (ETyApp (EBuiltin BEError) t)
  (EBuiltin (BEText (msg <> " only supported when compiling to DAML-LF >= " <> v)))

runtimeUnsupportedPartyFromText :: Type -> Expr
runtimeUnsupportedPartyFromText retType =
  mkETmLams
    [(varV1, TText)]
    (ETmApp
      (ETyApp (EBuiltin BEError) retType)
      (EBuiltin (BEText "PARTY_FROM_TEXT only supported when compiling to DAML-LF >= 1.2")))

-- NOTE(MH): DAML-LF 1.0 does not support the comparison operators for Party.
-- We rewrite them using ToText and comparison on Text when targeting
-- DAML-LF 1.0.
convertCompare :: Version -> (BuiltinType -> BuiltinExpr) -> BuiltinType -> Expr
convertCompare version cmp typ
    | typ /= BTParty || supportsPartyOrd version = EBuiltin (cmp typ)
    | otherwise =
        mkETmLams [(varV1, TParty), (varV2, TParty)] $
          mkETmApps (EBuiltin (cmp BTText)) $
            map (\v -> EBuiltin (BEToText BTParty) `ETmApp` EVar v) [varV1, varV2]
