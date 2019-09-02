-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PatternSynonyms     #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-} -- Because the pattern match checker is garbage


-- | The DAML-LF primitives, matched with their type, and using 'primitive' on the libraries side.
module DA.Daml.LFConversion.Primitives(convertPrim) where

import           DA.Daml.LFConversion.UtilLF
import           DA.Daml.LF.Ast
import           DA.Pretty (renderPretty)
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
convertPrim _ "BEEqual" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    EBuiltin $ BEEqual a1
convertPrim _ "BELess" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    EBuiltin $ BELess a1
convertPrim _ "BELessEq" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    EBuiltin $ BELessEq a1
convertPrim _ "BEGreaterEq" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    EBuiltin $ BEGreaterEq a1
convertPrim _ "BEGreater" (TBuiltin a1 :-> TBuiltin a2 :-> TBool) | a1 == a2 =
    EBuiltin $ BEGreater a1
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
convertPrim _ "BESha256Text" (TText :-> TText) =
    EBuiltin BESha256Text
convertPrim _ "BEPartyToQuotedText" (TParty :-> TText) =
    EBuiltin BEPartyToQuotedText
convertPrim _ "BEPartyFromText" (TText :-> TOptional TParty) =
    EBuiltin BEPartyFromText
convertPrim _ "BEInt64FromText" (TText :-> TOptional TInt64) =
    EBuiltin BEInt64FromText
convertPrim _ "BEDecimalFromText" (TText :-> TOptional TDecimal) =
    EBuiltin BEDecimalFromText
convertPrim _ "BETextToCodePoints" (TText :-> TList TInt64) =
    EBuiltin BETextToCodePoints
convertPrim _ "BETextFromCodePoints" (TList TInt64 :-> TText) =
    EBuiltin BETextFromCodePoints

-- Map operations

convertPrim _ "BEMapEmpty" (TMap a) =
  EBuiltin BEMapEmpty `ETyApp` a
convertPrim _ "BEMapInsert"  (TText :-> a1 :-> TMap a2 :-> TMap a3) | a1 == a2, a2 == a3 =
  EBuiltin BEMapInsert `ETyApp` a1
convertPrim _ "BEMapLookup" (TText :-> TMap a1 :-> TOptional a2) | a1 == a2 =
  EBuiltin BEMapLookup `ETyApp` a1
convertPrim _ "BEMapDelete" (TText :-> TMap a1 :-> TMap a2) | a1 == a2 =
  EBuiltin BEMapDelete `ETyApp` a1
convertPrim _ "BEMapToList" (TMap a1 :-> TList (TMapEntry a2)) | a1 == a2  =
  EBuiltin BEMapToList `ETyApp` a1
convertPrim _ "BEMapSize" (TMap a :-> TInt64) =
  EBuiltin BEMapSize `ETyApp` a

convertPrim _ "BECoerceContractId" (TContractId a :-> TContractId b) =
    EBuiltin BECoerceContractId `ETyApp` a `ETyApp` b

-- Decimal->Numeric compatibility. These will only be invoked when
-- Numeric is available as a feature (otherwise it would not appear
-- in the type) but Decimal primitives are still used (from the
-- stdlib). Eventually the Decimal primitives will be phased out.
convertPrim _ "BEAddDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    ETyApp (EBuiltin BEAddNumeric) (TNat 10)
convertPrim _ "BESubDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    ETyApp (EBuiltin BESubNumeric) (TNat 10)
convertPrim _ "BEMulDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    ETyApp (EBuiltin BEMulNumeric) (TNat 10)
convertPrim _ "BEDivDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    ETyApp (EBuiltin BEDivNumeric) (TNat 10)
convertPrim _ "BERoundDecimal" (TInt64 :-> TNumeric10 :-> TNumeric10) =
    ETyApp (EBuiltin BERoundNumeric) (TNat 10)
convertPrim _ "BEEqual" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BEEqualNumeric) (TNat 10)
convertPrim _ "BELess" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BELessNumeric) (TNat 10)
convertPrim _ "BELessEq" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BELessEqNumeric) (TNat 10)
convertPrim _ "BEGreaterEq" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BEGreaterEqNumeric) (TNat 10)
convertPrim _ "BEGreater" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BEGreaterNumeric) (TNat 10)
convertPrim _ "BEInt64ToDecimal" (TInt64 :-> TNumeric10) =
    ETyApp (EBuiltin BEInt64ToNumeric) (TNat 10)
convertPrim _ "BEDecimalToInt64" (TNumeric10 :-> TInt64) =
    ETyApp (EBuiltin BENumericToInt64) (TNat 10)
convertPrim _ "BEToText" (TNumeric10 :-> TText) =
    ETyApp (EBuiltin BEToTextNumeric) (TNat 10)
convertPrim _ "BEDecimalFromText" (TText :-> TOptional TNumeric10) =
    ETyApp (EBuiltin BENumericFromText) (TNat 10)

convertPrim _ x ty = error $ "Unknown primitive " ++ show x ++ " at type " ++ renderPretty ty

-- | Some builtins are only supported in specific versions of DAML-LF.
-- Since we don't have conditional compilation in daml-stdlib, we compile
-- them to calls to `error` in unsupported versions.
_whenRuntimeSupports :: Version -> Feature -> Type -> Expr -> Expr
_whenRuntimeSupports version feature t e
    | version `supports` feature = e
    | otherwise = runtimeUnsupported feature t

runtimeUnsupported :: Feature -> Type -> Expr
runtimeUnsupported (Feature name version) t =
  ETmApp
  (ETyApp (EBuiltin BEError) t)
  (EBuiltin (BEText (name <> " only supported when compiling to DAML-LF " <> T.pack (renderVersion version) <> " or later")))
