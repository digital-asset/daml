-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-} -- Because the pattern match checker is garbage


-- | The DAML-LF primitives, matched with their type, and using 'primitive' on the libraries side.
module DA.Daml.LFConversion.Primitives(convertPrim) where

import           DA.Daml.LFConversion.UtilLF
import           DA.Daml.LF.Ast
import           DA.Pretty (renderPretty)
import qualified Data.Text as T
import qualified Data.List as L

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
convertPrim v "BEEqual" (a1 :-> a2 :-> TBool) | a1 == a2, v `supports` featureGenericComparison =
    EBuiltin BEEqualGeneric `ETyApp` a1
convertPrim v "BELess" (a1 :-> a2 :-> TBool) | a1 == a2, v `supports` featureGenericComparison =
    EBuiltin BELessGeneric `ETyApp` a1
convertPrim v "BELessEq" (a1 :-> a2 :-> TBool) | a1 == a2, v `supports` featureGenericComparison =
    EBuiltin BELessEqGeneric `ETyApp` a1
convertPrim v "BEGreater" (a1 :-> a2 :-> TBool) | a1 == a2, v `supports` featureGenericComparison =
    EBuiltin BEGreaterGeneric `ETyApp` a1
convertPrim v "BEGreaterEq" (a1 :-> a2 :-> TBool) | a1 == a2, v `supports` featureGenericComparison =
    EBuiltin BEGreaterEqGeneric `ETyApp` a1
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
convertPrim v "BEEqualContractId" (TContractId a1 :-> TContractId a2 :-> TBool) | a1 == a2 =
    if v `supports` featureGenericComparison
        then EBuiltin BEEqualGeneric `ETyApp` TContractId a1
        else EBuiltin BEEqualContractId `ETyApp` a1

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

convertPrim _ "BETextMapEmpty" (TTextMap a) =
  EBuiltin BETextMapEmpty `ETyApp` a
convertPrim _ "BETextMapInsert"  (TText :-> a1 :-> TTextMap a2 :-> TTextMap a3) | a1 == a2, a2 == a3 =
  EBuiltin BETextMapInsert `ETyApp` a1
convertPrim _ "BETextMapLookup" (TText :-> TTextMap a1 :-> TOptional a2) | a1 == a2 =
  EBuiltin BETextMapLookup `ETyApp` a1
convertPrim _ "BETextMapDelete" (TText :-> TTextMap a1 :-> TTextMap a2) | a1 == a2 =
  EBuiltin BETextMapDelete `ETyApp` a1
convertPrim _ "BETextMapToList" (TTextMap a1 :-> TList (TTextMapEntry a2)) | a1 == a2  =
  EBuiltin BETextMapToList `ETyApp` a1
convertPrim _ "BETextMapSize" (TTextMap a :-> TInt64) =
  EBuiltin BETextMapSize `ETyApp` a


convertPrim _ "BEGenMapEmpty" (TGenMap a b) =
  EBuiltin BEGenMapEmpty `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapInsert"  (a :-> b :-> TGenMap a1 b1 :-> TGenMap a2 b2) | a == a1, a == a2, b == b1, b == b2 =
  EBuiltin BEGenMapInsert `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapLookup" (a1 :-> TGenMap a b :-> TOptional b1) | a == a1, b == b1 =
  EBuiltin BEGenMapLookup `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapDelete" (a2 :-> TGenMap a b :-> TGenMap a1 b1) | a == a1, a == a2, b == b1 =
  EBuiltin BEGenMapDelete `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapKeys" (TGenMap a b :-> TList a1) | a == a1 =
  EBuiltin BEGenMapKeys `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapValues" (TGenMap a b :-> TList b1) | b == b1 =
  EBuiltin BEGenMapValues `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapSize" (TGenMap a b :-> TInt64) =
  EBuiltin BEGenMapSize `ETyApp` a `ETyApp` b

convertPrim _ "BECoerceContractId" (TContractId a :-> TContractId b) =
    EBuiltin BECoerceContractId `ETyApp` a `ETyApp` b

-- Decimal->Numeric compatibility. These will only be invoked when
-- Numeric is available as a feature (otherwise it would not appear
-- in the type) but Decimal primitives are still used (from the
-- stdlib). Eventually the Decimal primitives will be phased out.
convertPrim _ "BEAddDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    ETyApp (EBuiltin BEAddNumeric) TNat10
convertPrim _ "BESubDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    ETyApp (EBuiltin BESubNumeric) TNat10
convertPrim _ "BEMulDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    EBuiltin BEMulNumeric `ETyApp` TNat10 `ETyApp` TNat10 `ETyApp` TNat10
convertPrim _ "BEDivDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    EBuiltin BEDivNumeric `ETyApp` TNat10 `ETyApp` TNat10 `ETyApp` TNat10
convertPrim _ "BERoundDecimal" (TInt64 :-> TNumeric10 :-> TNumeric10) =
    ETyApp (EBuiltin BERoundNumeric) TNat10
convertPrim _ "BEEqual" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BEEqualNumeric) TNat10
convertPrim _ "BELess" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BELessNumeric) TNat10
convertPrim _ "BELessEq" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BELessEqNumeric) TNat10
convertPrim _ "BEGreaterEq" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BEGreaterEqNumeric) TNat10
convertPrim _ "BEGreater" (TNumeric10 :-> TNumeric10 :-> TBool) =
    ETyApp (EBuiltin BEGreaterNumeric) TNat10
convertPrim _ "BEInt64ToDecimal" (TInt64 :-> TNumeric10) =
    ETyApp (EBuiltin BEInt64ToNumeric) TNat10
convertPrim _ "BEDecimalToInt64" (TNumeric10 :-> TInt64) =
    ETyApp (EBuiltin BENumericToInt64) TNat10
convertPrim _ "BEToText" (TNumeric10 :-> TText) =
    ETyApp (EBuiltin BEToTextNumeric) TNat10
convertPrim _ "BEDecimalFromText" (TText :-> TOptional TNumeric10) =
    ETyApp (EBuiltin BENumericFromText) TNat10

-- Numeric primitives. These are polymorphic in the scale.
convertPrim _ "BEAddNumeric" (TNumeric n1 :-> TNumeric n2 :-> TNumeric n3) | n1 == n2, n1 == n3 =
    ETyApp (EBuiltin BEAddNumeric) n1
convertPrim _ "BESubNumeric" (TNumeric n1 :-> TNumeric n2 :-> TNumeric n3) | n1 == n2, n1 == n3 =
    ETyApp (EBuiltin BESubNumeric) n1
convertPrim _ "BEMulNumeric" (TNumeric n1 :-> TNumeric n2 :-> TNumeric n3) =
    EBuiltin BEMulNumeric `ETyApp` n1 `ETyApp` n2 `ETyApp` n3
convertPrim _ "BEDivNumeric" (TNumeric n1 :-> TNumeric n2 :-> TNumeric n3) =
    EBuiltin BEDivNumeric `ETyApp` n1 `ETyApp` n2 `ETyApp` n3
convertPrim _ "BERoundNumeric" (TInt64 :-> TNumeric n1 :-> TNumeric n2) | n1 == n2 =
    ETyApp (EBuiltin BERoundNumeric) n1
convertPrim _ "BECastNumeric" (TNumeric n1 :-> TNumeric n2) =
    EBuiltin BECastNumeric `ETyApp` n1 `ETyApp` n2
convertPrim _ "BEShiftNumeric" (TNumeric n1 :-> TNumeric n2) =
    EBuiltin BEShiftNumeric `ETyApp` n1 `ETyApp` n2
convertPrim v "BEEqualNumeric" (TNumeric n1 :-> TNumeric n2 :-> TBool) | n1 == n2 =
    if v `supports` featureGenericComparison
        then ETyApp (EBuiltin BEEqualGeneric) (TNumeric n1)
        else ETyApp (EBuiltin BEEqualNumeric) n1
convertPrim v "BELessNumeric" (TNumeric n1 :-> TNumeric n2 :-> TBool) | n1 == n2 =
    if v `supports` featureGenericComparison
        then ETyApp (EBuiltin BELessGeneric) (TNumeric n1)
        else ETyApp (EBuiltin BELessNumeric) n1
convertPrim v "BELessEqNumeric" (TNumeric n1 :-> TNumeric n2 :-> TBool) | n1 == n2 =
    if v `supports` featureGenericComparison
        then ETyApp (EBuiltin BELessEqGeneric) (TNumeric n1)
        else ETyApp (EBuiltin BELessEqNumeric) n1
convertPrim v "BEGreaterEqNumeric" (TNumeric n1 :-> TNumeric n2 :-> TBool) | n1 == n2 =
    if v `supports` featureGenericComparison
        then ETyApp (EBuiltin BEGreaterEqGeneric) (TNumeric n1)
        else ETyApp (EBuiltin BEGreaterEqNumeric) n1
convertPrim v "BEGreaterNumeric" (TNumeric n1 :-> TNumeric n2 :-> TBool) | n1 == n2 =
    if v `supports` featureGenericComparison
        then ETyApp (EBuiltin BEGreaterGeneric) (TNumeric n1)
        else ETyApp (EBuiltin BEGreaterNumeric) n1
convertPrim _ "BEInt64ToNumeric" (TInt64 :-> TNumeric n) =
    ETyApp (EBuiltin BEInt64ToNumeric) n
convertPrim _ "BENumericToInt64" (TNumeric n :-> TInt64) =
    ETyApp (EBuiltin BENumericToInt64) n
convertPrim _ "BEToTextNumeric" (TNumeric n :-> TText) =
    ETyApp (EBuiltin BEToTextNumeric) n
convertPrim _ "BENumericFromText" (TText :-> TOptional (TNumeric n)) =
    ETyApp (EBuiltin BENumericFromText) n

-- Experimental text primitives.
convertPrim _ "BETextToUpper" (TText :-> TText) = EBuiltin BETextToUpper
convertPrim _ "BETextToLower" (TText :-> TText) = EBuiltin BETextToLower
convertPrim _ "BETextSlice" (TInt64 :-> TInt64 :-> TText :-> TText) = EBuiltin BETextSlice
convertPrim _ "BETextSliceIndex" (TText :-> TText :-> TOptional TInt64) = EBuiltin BETextSliceIndex
convertPrim _ "BETextContainsOnly" (TText :-> TText :-> TBool) = EBuiltin BETextContainsOnly
convertPrim _ "BETextReplicate" (TInt64 :-> TText :-> TText) = EBuiltin BETextReplicate
convertPrim _ "BETextSplitOn" (TText :-> TText :-> TList TText) = EBuiltin BETextSplitOn
convertPrim _ "BETextIntercalate" (TText :-> TList TText :-> TText) = EBuiltin BETextIntercalate

-- Conversion from ContractId to Text

convertPrim _ "BEToTextContractId" (TContractId t :-> TOptional TText) =
    ETyApp (EBuiltin BEToTextContractId) t


-- Template Desugaring.

convertPrim _ "UCreate" (TCon template :-> TUpdate (TContractId (TCon template')))
    | template == template' =
    ETmLam (mkVar "this", TCon template) $
    EUpdate $ UCreate template (EVar (mkVar "this"))

convertPrim _ "UFetch" (TContractId (TCon template) :-> TUpdate (TCon template'))
    | template == template' =
    ETmLam (mkVar "this", TContractId (TCon template)) $
    EUpdate $ UFetch template (EVar (mkVar "this"))

convertPrim _ "UExercise"
    (TContractId (TCon template) :-> TCon choice :-> TUpdate _returnTy) =
    ETmLam (mkVar "this", TContractId (TCon template)) $
    ETmLam (mkVar "arg", TCon choice) $
    EUpdate $ UExercise template choiceName (EVar (mkVar "this")) (EVar (mkVar "arg"))
  where
    choiceName = ChoiceName (T.intercalate "." $ unTypeConName $ qualObject choice)

convertPrim _ "ULookupByKey" (key :-> TUpdate (TOptional (TContractId (TCon template)))) =
    ETmLam (mkVar "key", key) $ EUpdate $
        ULookupByKey $ RetrieveByKey template (EVar $ mkVar "key")

convertPrim _ "UFetchByKey"
    (key :-> TUpdate ty@(TApp (TApp (TCon tuple) ty1@(TContractId (TCon template))) ty2))
    | ty2 == TCon template =
    ETmLam (mkVar "key", key) $
    EUpdate $ UBind
        (Binding (mkVar "res", TStruct
            [ (FieldName "contractId", ty1)
            , (FieldName "contract", ty2)])
            (EUpdate $ UFetchByKey (RetrieveByKey template (EVar $ mkVar "key"))))
        (EUpdate $ UPure ty $ ERecCon (TypeConApp tuple [ty1, ty2])
            [ (mkIndexedField 1, EStructProj (FieldName "contractId") (EVar (mkVar "res")))
            , (mkIndexedField 2, EStructProj (FieldName "contract") (EVar (mkVar "res")))
            ])

convertPrim version "ETemplateTypeRep"
    ty@(TApp proxy (TCon template) :-> tTypeRep)
    | tTypeRep `elem` [TTypeRep, TUnit] =
    -- TODO: restrict to known templates
    whenRuntimeSupports version featureTypeRep ty $
        ETmLam (mkVar "_", TApp proxy (TCon template)) $
        ETypeRep (TCon template)

convertPrim version "EFromAnyTemplate"
    ty@(tAny :-> TOptional (TCon template))
    | tAny `elem` [TAny, TUnit] =
    -- TODO: restrict to known templates
    whenRuntimeSupports version featureAnyType ty $
        ETmLam (mkVar "any", TAny) $
        EFromAny (TCon template) (EVar $ mkVar "any")

convertPrim version "EFromAnyChoice"
    ty@(TApp proxy (TCon template) :-> tAny :-> TOptional choice)
    | tAny `elem` [TAny, TUnit] =
    -- TODO: restrict to known template/choice pairs
    whenRuntimeSupports version featureAnyType ty $
        ETmLam (mkVar "_", TApp proxy (TCon template)) $
        ETmLam (mkVar "any", TAny) $
        EFromAny choice (EVar $ mkVar "any")

convertPrim version "EFromAnyContractKey"
    ty@(TApp proxy (TCon template) :-> tAny :-> TOptional key)
    | tAny `elem` [TAny, TUnit] =
    -- TODO: restrict to known template/key pairs
    whenRuntimeSupports version featureAnyType ty $
        ETmLam (mkVar "_", TApp proxy (TCon template)) $
        ETmLam (mkVar "any", TAny) $
        EFromAny key (EVar $ mkVar "any")

convertPrim version "EToAnyTemplate"
    ty@(TCon template :-> tAny)
    | tAny `elem` [TAny, TUnit] =
    -- TODO: restrict to known templates
    whenRuntimeSupports version featureAnyType ty $
        ETmLam (mkVar "template", TCon template) $
        EToAny (TCon template) (EVar $ mkVar "template")

convertPrim version "EToAnyChoice"
    ty@(TApp proxy (TCon template) :-> choice :-> tAny)
    | tAny `elem` [TAny, TUnit] =
    -- TODO: restrict to known template/choice pairs
    whenRuntimeSupports version featureAnyType ty $
        ETmLam (mkVar "_", TApp proxy (TCon template)) $
        ETmLam (mkVar "choice", choice) $
        EToAny choice (EVar $ mkVar "choice")

convertPrim version "EToAnyContractKey"
    ty@(TApp proxy (TCon template) :-> key :-> tAny)
    | tAny `elem` [TAny, TUnit] =
    -- TODO: restrict to known template/key pairs
    whenRuntimeSupports version featureAnyType ty $
        ETmLam (mkVar "_", TApp proxy (TCon template)) $
        ETmLam (mkVar "key", key) $
        EToAny key (EVar $ mkVar "key")

-- Exceptions
convertPrim _ "BEAnyExceptionMessage" (TBuiltin BTAnyException :-> TText) =
    EBuiltin BEAnyExceptionMessage
convertPrim _ "BEGeneralErrorMessage" (TBuiltin BTGeneralError :-> TText) =
    EBuiltin BEGeneralErrorMessage
convertPrim _ "BEArithmeticErrorMessage" (TBuiltin BTArithmeticError :-> TText) =
    EBuiltin BEArithmeticErrorMessage
convertPrim _ "BEContractErrorMessage" (TBuiltin BTContractError :-> TText) =
    EBuiltin BEContractErrorMessage

-- TODO #8020 https://github.com/digital-asset/daml/issues/8020
-- Handle these three in LFConversion.hs and check that ty1 is an exception type.
convertPrim _ "EThrow" (ty1 :-> ty2) =
    ETmLam (mkVar "x", ty1) (EThrow ty2 ty1 (EVar (mkVar "x")))
convertPrim _ "EToAnyException" (ty :-> TBuiltin BTAnyException) =
    ETmLam (mkVar "x", ty) (EToAnyException ty (EVar (mkVar "x")))
convertPrim _ "EFromAnyException" (TBuiltin BTAnyException :-> TOptional ty) =
    ETmLam (mkVar "x", TBuiltin BTAnyException) (EFromAnyException ty (EVar (mkVar "x")))

-- TODO #8020 https://github.com/digital-asset/daml/issues/8020
-- Make sure this indirection is eliminated when doing "try ... catch ..."
-- in the Update monad. This may require a specific LFConversion rule.
convertPrim _ "UTryCatch" ((TUnit :-> TUpdate t1) :-> (TBuiltin BTAnyException :-> TOptional (TUpdate t2)) :-> TUpdate t3)
    | t1 == t2, t2 == t3
        = ETmLam (mkVar "t", TUnit :-> TUpdate t1)
        $ ETmLam (mkVar "c", TBuiltin BTAnyException :-> TOptional (TUpdate t2))
        $ EUpdate
        $ UTryCatch t3
            (EVar (mkVar "t") `ETmApp` EUnit)
            (mkVar "x")
            (EVar (mkVar "c") `ETmApp` EVar (mkVar "x"))

convertPrim (V1 PointDev) (L.stripPrefix "$" -> Just builtin) typ =
    EExperimental (T.pack builtin) typ

-- Unknown primitive.
convertPrim _ x ty = error $ "Unknown primitive " ++ show x ++ " at type " ++ renderPretty ty

-- | Some builtins are only supported in specific versions of DAML-LF.
whenRuntimeSupports :: Version -> Feature -> Type -> Expr -> Expr
whenRuntimeSupports version feature t e
    | version `supports` feature = e
    | otherwise = runtimeUnsupported feature t

runtimeUnsupported :: Feature -> Type -> Expr
runtimeUnsupported (Feature name version _) t =
  ETmApp
  (ETyApp (EBuiltin BEError) t)
  (EBuiltin (BEText (name <> " only supported when compiling to DAML-LF " <> T.pack (renderVersion version) <> " or later")))
