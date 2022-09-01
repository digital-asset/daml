-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-} -- Because the pattern match checker is garbage


-- | The Daml-LF primitives, matched with their type, and using 'primitive' on the libraries side.
module DA.Daml.LFConversion.Primitives(convertPrim) where

import           DA.Daml.LFConversion.ConvertM
import           DA.Daml.LF.Ast
import           DA.Daml.UtilLF
import           DA.Pretty (renderPretty)
import qualified Data.Text as T
import qualified Data.List as L

convertPrim :: Version -> String -> Type -> ConvertM Expr
-- Update
convertPrim _ "UPure" (a1 :-> TUpdate a2) | a1 == a2 =
    pure $ ETmLam (varV1, a1) $ EUpdate $ UPure a1 $ EVar varV1
convertPrim _ "UBind" (t1@(TUpdate a1) :-> t2@(a2 :-> TUpdate b1) :-> TUpdate b2) | a1 == a2, b1 == b2 =
    pure $ ETmLam (varV1, t1) $ ETmLam (varV2, t2) $ EUpdate $ UBind (Binding (varV3, a1) (EVar varV1)) (EVar varV2 `ETmApp` EVar varV3)
convertPrim _ "UAbort" (TText :-> t@(TUpdate a)) =
    pure $ ETmLam (varV1, TText) $ EUpdate (UEmbedExpr a (EBuiltin BEError `ETyApp` t `ETmApp` EVar varV1))
convertPrim _ "UGetTime" (TUpdate TTimestamp) =
    pure $ EUpdate UGetTime

-- Scenario
convertPrim _ "SPure" (a1 :-> TScenario a2) | a1 == a2 =
    pure $ ETmLam (varV1, a1) $ EScenario $ SPure a1 $ EVar varV1
convertPrim _ "SBind" (t1@(TScenario a1) :-> t2@(a2 :-> TScenario b1) :-> TScenario b2) | a1 == a2, b1 == b2 =
    pure $ ETmLam (varV1, t1) $ ETmLam (varV2, t2) $ EScenario $ SBind (Binding (varV3, a1) (EVar varV1)) (EVar varV2 `ETmApp` EVar varV3)
convertPrim _ "SAbort" (TText :-> t@(TScenario a)) =
    pure $ ETmLam (varV1, TText) $ EScenario (SEmbedExpr a (EBuiltin BEError `ETyApp` t `ETmApp` EVar varV1))
convertPrim _ "SCommit" (t1@TParty :-> t2@(TUpdate a1) :-> TScenario a2) | a1 == a2 =
    pure $ ETmLam (varV1, t1) $ ETmLam (varV2, t2) $ EScenario $ SCommit a1 (EVar varV1) (EVar varV2)
convertPrim _ "SMustFailAt" (t1@TParty :-> t2@(TUpdate a1) :-> TScenario TUnit) =
    pure $ ETmLam (varV1, t1) $ ETmLam (varV2, t2) $ EScenario $ SMustFailAt a1 (EVar varV1) (EVar varV2)
convertPrim _ "SPass" (t1@TInt64 :-> TScenario TTimestamp) =
    pure $ ETmLam (varV1, t1) $ EScenario $ SPass $ EVar varV1
convertPrim _ "SGetTime" (TScenario TTimestamp) =
    pure $ EScenario SGetTime
convertPrim _ "SGetParty" (t1@TText :-> TScenario TParty) =
    pure $ ETmLam (varV1, t1) $ EScenario $ SGetParty $ EVar varV1

-- Comparison
convertPrim _ "BEEqual" (a1 :-> a2 :-> TBool) | a1 == a2 =
    pure $ EBuiltin BEEqualGeneric `ETyApp` a1
convertPrim _ "BELess" (a1 :-> a2 :-> TBool) | a1 == a2 =
    pure $ EBuiltin BELessGeneric `ETyApp` a1
convertPrim _ "BELessEq" (a1 :-> a2 :-> TBool) | a1 == a2 =
    pure $ EBuiltin BELessEqGeneric `ETyApp` a1
convertPrim _ "BEGreater" (a1 :-> a2 :-> TBool) | a1 == a2 =
    pure $ EBuiltin BEGreaterGeneric `ETyApp` a1
convertPrim _ "BEGreaterEq" (a1 :-> a2 :-> TBool) | a1 == a2 =
    pure $ EBuiltin BEGreaterEqGeneric `ETyApp` a1
convertPrim _ "BEEqualList" ((a1 :-> a2 :-> TBool) :-> TList a3 :-> TList a4 :-> TBool) | a1 == a2, a2 == a3, a3 == a4 =
    pure $ EBuiltin BEEqualList `ETyApp` a1

-- Integer arithmetic
convertPrim _ "BEAddInt64" (TInt64 :-> TInt64 :-> TInt64) =
    pure $ EBuiltin BEAddInt64
convertPrim _ "BESubInt64" (TInt64 :-> TInt64 :-> TInt64) =
    pure $ EBuiltin BESubInt64
convertPrim _ "BEMulInt64" (TInt64 :-> TInt64 :-> TInt64) =
    pure $ EBuiltin BEMulInt64
convertPrim _ "BEDivInt64" (TInt64 :-> TInt64 :-> TInt64) =
    pure $ EBuiltin BEDivInt64
convertPrim _ "BEModInt64" (TInt64 :-> TInt64 :-> TInt64) =
    pure $ EBuiltin BEModInt64
convertPrim _ "BEExpInt64" (TInt64 :-> TInt64 :-> TInt64) =
    pure $ EBuiltin BEExpInt64

-- Time arithmetic
convertPrim _ "BETimestampToUnixMicroseconds" (TTimestamp :-> TInt64) =
    pure $ EBuiltin BETimestampToUnixMicroseconds
convertPrim _ "BEUnixMicrosecondsToTimestamp" (TInt64 :-> TTimestamp) =
    pure $ EBuiltin BEUnixMicrosecondsToTimestamp
convertPrim _ "BEDateToUnixDays" (TDate :-> TInt64) =
    pure $ EBuiltin BEDateToUnixDays
convertPrim _ "BEUnixDaysToDate" (TInt64 :-> TDate) =
    pure $ EBuiltin BEUnixDaysToDate

-- List operations
convertPrim _ "BEFoldl" ((b1 :-> a1 :-> b2) :-> b3 :-> TList a2 :-> b4) | a1 == a2, b1 == b2, b2 == b3, b3 == b4 =
    pure $ EBuiltin BEFoldl `ETyApp` a1 `ETyApp` b1
convertPrim _ "BEFoldr" ((a1 :-> b1 :-> b2) :-> b3 :-> TList a2 :-> b4) | a1 == a2, b1 == b2, b2 == b3, b3 == b4 =
    pure $ EBuiltin BEFoldr `ETyApp` a1 `ETyApp` b1

-- Error
convertPrim _ "BEError" (TText :-> t2) =
    pure $ ETyApp (EBuiltin BEError) t2

-- Text operations
convertPrim _ "BEToText" (TBuiltin x :-> TText) =
    pure $ EBuiltin $ BEToText x
convertPrim _ "BEExplodeText" (TText :-> TList TText) =
    pure $ EBuiltin BEExplodeText
convertPrim _ "BEImplodeText" (TList TText :-> TText) =
    pure $ EBuiltin BEImplodeText
convertPrim _ "BEAppendText" (TText :-> TText :-> TText) =
    pure $ EBuiltin BEAppendText
convertPrim _ "BETrace" (TText :-> a1 :-> a2) | a1 == a2 =
    pure $ EBuiltin BETrace `ETyApp` a1
convertPrim _ "BESha256Text" (TText :-> TText) =
    pure $ EBuiltin BESha256Text
convertPrim _ "BEPartyToQuotedText" (TParty :-> TText) =
    pure $ EBuiltin BEPartyToQuotedText
convertPrim _ "BETextToParty" (TText :-> TOptional TParty) =
    pure $ EBuiltin BETextToParty
convertPrim _ "BETextToInt64" (TText :-> TOptional TInt64) =
    pure $ EBuiltin BETextToInt64
convertPrim _ "BETextToCodePoints" (TText :-> TList TInt64) =
    pure $ EBuiltin BETextToCodePoints
convertPrim _ "BECodePointsToText" (TList TInt64 :-> TText) =
    pure $ EBuiltin BECodePointsToText

-- Map operations

convertPrim _ "BETextMapEmpty" (TTextMap a) =
    pure $ EBuiltin BETextMapEmpty `ETyApp` a
convertPrim _ "BETextMapInsert"  (TText :-> a1 :-> TTextMap a2 :-> TTextMap a3) | a1 == a2, a2 == a3 =
    pure $ EBuiltin BETextMapInsert `ETyApp` a1
convertPrim _ "BETextMapLookup" (TText :-> TTextMap a1 :-> TOptional a2) | a1 == a2 =
    pure $ EBuiltin BETextMapLookup `ETyApp` a1
convertPrim _ "BETextMapDelete" (TText :-> TTextMap a1 :-> TTextMap a2) | a1 == a2 =
    pure $ EBuiltin BETextMapDelete `ETyApp` a1
convertPrim _ "BETextMapToList" (TTextMap a1 :-> TList (TTextMapEntry a2)) | a1 == a2  =
    pure $ EBuiltin BETextMapToList `ETyApp` a1
convertPrim _ "BETextMapSize" (TTextMap a :-> TInt64) =
    pure $ EBuiltin BETextMapSize `ETyApp` a


convertPrim _ "BEGenMapEmpty" (TGenMap a b) =
    pure $ EBuiltin BEGenMapEmpty `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapInsert"  (a :-> b :-> TGenMap a1 b1 :-> TGenMap a2 b2) | a == a1, a == a2, b == b1, b == b2 =
    pure $ EBuiltin BEGenMapInsert `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapLookup" (a1 :-> TGenMap a b :-> TOptional b1) | a == a1, b == b1 =
    pure $ EBuiltin BEGenMapLookup `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapDelete" (a2 :-> TGenMap a b :-> TGenMap a1 b1) | a == a1, a == a2, b == b1 =
    pure $ EBuiltin BEGenMapDelete `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapKeys" (TGenMap a b :-> TList a1) | a == a1 =
    pure $ EBuiltin BEGenMapKeys `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapValues" (TGenMap a b :-> TList b1) | b == b1 =
    pure $ EBuiltin BEGenMapValues `ETyApp` a `ETyApp` b
convertPrim _ "BEGenMapSize" (TGenMap a b :-> TInt64) =
    pure $ EBuiltin BEGenMapSize `ETyApp` a `ETyApp` b

convertPrim _ "BECoerceContractId" (TContractId a :-> TContractId b) =
    pure $ EBuiltin BECoerceContractId `ETyApp` a `ETyApp` b

-- Decimal->Numeric compatibility. These will only be invoked when
-- Numeric is available as a feature (otherwise it would not appear
-- in the type) but Decimal primitives are still used (from the
-- stdlib). Eventually the Decimal primitives will be phased out.
convertPrim _ "BEAddDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    pure $ ETyApp (EBuiltin BEAddNumeric) TNat10
convertPrim _ "BESubDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    pure $ ETyApp (EBuiltin BESubNumeric) TNat10
convertPrim _ "BEMulDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    pure $ EBuiltin BEMulNumeric `ETyApp` TNat10 `ETyApp` TNat10 `ETyApp` TNat10
convertPrim _ "BEDivDecimal" (TNumeric10 :-> TNumeric10 :-> TNumeric10) =
    pure $ EBuiltin BEDivNumeric `ETyApp` TNat10 `ETyApp` TNat10 `ETyApp` TNat10
convertPrim _ "BERoundDecimal" (TInt64 :-> TNumeric10 :-> TNumeric10) =
    pure $ ETyApp (EBuiltin BERoundNumeric) TNat10
convertPrim _ "BEInt64ToDecimal" (TInt64 :-> TNumeric10) =
    pure $ ETyApp (EBuiltin BEInt64ToNumeric) TNat10
convertPrim _ "BEDecimalToInt64" (TNumeric10 :-> TInt64) =
    pure $ ETyApp (EBuiltin BENumericToInt64) TNat10
convertPrim _ "BEToText" (TNumeric10 :-> TText) =
    pure $ ETyApp (EBuiltin BENumericToText) TNat10
convertPrim _ "BETextToDecimal" (TText :-> TOptional TNumeric10) =
    pure $ ETyApp (EBuiltin BETextToNumeric) TNat10

-- Numeric primitives. These are polymorphic in the scale.
convertPrim _ "BEAddNumeric" (TNumeric n1 :-> TNumeric n2 :-> TNumeric n3) | n1 == n2, n1 == n3 =
    pure $ ETyApp (EBuiltin BEAddNumeric) n1
convertPrim _ "BESubNumeric" (TNumeric n1 :-> TNumeric n2 :-> TNumeric n3) | n1 == n2, n1 == n3 =
    pure $ ETyApp (EBuiltin BESubNumeric) n1
convertPrim _ "BEMulNumeric" (TNumeric n1 :-> TNumeric n2 :-> TNumeric n3) =
    pure $ EBuiltin BEMulNumeric `ETyApp` n1 `ETyApp` n2 `ETyApp` n3
convertPrim _ "BEDivNumeric" (TNumeric n1 :-> TNumeric n2 :-> TNumeric n3) =
    pure $ EBuiltin BEDivNumeric `ETyApp` n1 `ETyApp` n2 `ETyApp` n3
convertPrim _ "BERoundNumeric" (TInt64 :-> TNumeric n1 :-> TNumeric n2) | n1 == n2 =
    pure $ ETyApp (EBuiltin BERoundNumeric) n1
convertPrim _ "BECastNumeric" (TNumeric n1 :-> TNumeric n2) =
    pure $ EBuiltin BECastNumeric `ETyApp` n1 `ETyApp` n2
convertPrim _ "BEShiftNumeric" (TNumeric n1 :-> TNumeric n2) =
    pure $ EBuiltin BEShiftNumeric `ETyApp` n1 `ETyApp` n2
convertPrim _ "BEInt64ToNumeric" (TInt64 :-> TNumeric n) =
    pure $ ETyApp (EBuiltin BEInt64ToNumeric) n
convertPrim _ "BENumericToInt64" (TNumeric n :-> TInt64) =
    pure $ ETyApp (EBuiltin BENumericToInt64) n
convertPrim _ "BENumericToText" (TNumeric n :-> TText) =
    pure $ ETyApp (EBuiltin BENumericToText) n
convertPrim _ "BETextToNumeric" (TText :-> TOptional (TNumeric n)) =
    pure $ ETyApp (EBuiltin BETextToNumeric) n

convertPrim version "BEScaleBigNumeric" ty@(TBigNumeric :-> TInt64) =
    pure $
      whenRuntimeSupports version featureBigNumeric ty $
        EBuiltin BEScaleBigNumeric
convertPrim version "BEPrecisionBigNumeric" ty@(TBigNumeric :-> TInt64) =
    pure $
      whenRuntimeSupports version featureBigNumeric ty $
        EBuiltin BEPrecisionBigNumeric
convertPrim version "BEAddBigNumeric" ty@(TBigNumeric :-> TBigNumeric :-> TBigNumeric) =
    pure $
      whenRuntimeSupports version featureBigNumeric ty $
        EBuiltin BEAddBigNumeric
convertPrim version "BESubBigNumeric" ty@(TBigNumeric :-> TBigNumeric :-> TBigNumeric) =
    pure $
      whenRuntimeSupports version featureBigNumeric ty $
        EBuiltin BESubBigNumeric
convertPrim version "BEMulBigNumeric" ty@(TBigNumeric :-> TBigNumeric :-> TBigNumeric) =
    pure $
      whenRuntimeSupports version featureBigNumeric ty $
        EBuiltin BEMulBigNumeric
convertPrim version "BEDivBigNumeric" ty@(TInt64 :-> TRoundingMode :-> TBigNumeric :-> TBigNumeric :-> TBigNumeric) =
    pure $
      whenRuntimeSupports version featureBigNumeric ty $
        EBuiltin BEDivBigNumeric
convertPrim version "BEShiftRightBigNumeric" ty@(TInt64 :-> TBigNumeric :-> TBigNumeric) =
    pure $
      whenRuntimeSupports version featureBigNumeric ty $
        EBuiltin BEShiftRightBigNumeric
convertPrim version "BENumericToBigNumeric" ty@(TNumeric n :-> TBigNumeric) =
    pure $
      whenRuntimeSupports version featureBigNumeric ty $
        EBuiltin BENumericToBigNumeric `ETyApp` n
convertPrim version "BEBigNumericToNumeric" ty@(TBigNumeric :-> TNumeric n) =
    pure $
      whenRuntimeSupports version featureBigNumeric ty $
        EBuiltin BEBigNumericToNumeric `ETyApp` n

-- Experimental text primitives.
convertPrim _ "BETextToUpper" (TText :-> TText) = pure $ EBuiltin BETextToUpper
convertPrim _ "BETextToLower" (TText :-> TText) = pure $ EBuiltin BETextToLower
convertPrim _ "BETextSlice" (TInt64 :-> TInt64 :-> TText :-> TText) = pure $ EBuiltin BETextSlice
convertPrim _ "BETextSliceIndex" (TText :-> TText :-> TOptional TInt64) = pure $ EBuiltin BETextSliceIndex
convertPrim _ "BETextContainsOnly" (TText :-> TText :-> TBool) = pure $ EBuiltin BETextContainsOnly
convertPrim _ "BETextReplicate" (TInt64 :-> TText :-> TText) = pure $ EBuiltin BETextReplicate
convertPrim _ "BETextSplitOn" (TText :-> TText :-> TList TText) = pure $ EBuiltin BETextSplitOn
convertPrim _ "BETextIntercalate" (TText :-> TList TText :-> TText) = pure $ EBuiltin BETextIntercalate

-- Conversion from ContractId to Text

convertPrim _ "BEContractIdToText" (TContractId t :-> TOptional TText) =
    pure $ ETyApp (EBuiltin BEContractIdToText) t


-- Template Desugaring.

convertPrim _ "UCreate" (TCon template :-> TUpdate (TContractId (TCon template')))
    | template == template' =
    pure $
    ETmLam (mkVar "this", TCon template) $
    EUpdate $ UCreate template (EVar (mkVar "this"))

convertPrim _ "UCreateInterface" (TCon interface :-> TUpdate (TContractId (TCon interface')))
    | interface == interface' =
    pure $
    ETmLam (mkVar "this", TCon interface) $
    EUpdate $ UCreateInterface interface (EVar (mkVar "this"))

convertPrim _ "UFetch" (TContractId (TCon template) :-> TUpdate (TCon template'))
    | template == template' =
    pure $
    ETmLam (mkVar "this", TContractId (TCon template)) $
    EUpdate $ UFetch template (EVar (mkVar "this"))

convertPrim _ "UFetchInterface" (TContractId (TCon iface) :-> TUpdate (TCon iface'))
    | iface == iface' =
    pure $
    ETmLam (mkVar "this", TContractId (TCon iface)) $
    EUpdate $ UFetchInterface iface (EVar (mkVar "this"))

convertPrim _ "UExercise"
    (TContractId (TCon template) :-> TCon choice :-> TUpdate _returnTy) =
    pure $
    ETmLam (mkVar "this", TContractId (TCon template)) $
    ETmLam (mkVar "arg", TCon choice) $
    EUpdate $ UExercise template choiceName (EVar (mkVar "this")) (EVar (mkVar "arg"))
  where
    choiceName = ChoiceName (T.intercalate "." $ unTypeConName $ qualObject choice)

convertPrim _ "UExerciseInterface"
    (   TContractId (TCon iface)
    :-> TCon choice
    :->  TUpdate _returnTy) =
    pure $
    ETmLam (mkVar "this", TContractId (TCon iface)) $
    ETmLam (mkVar "arg", TCon choice) $
    EUpdate $ UExerciseInterface
        { exeInterface  = iface
        , exeChoice     = choiceName
        , exeContractId = EVar (mkVar "this")
        , exeArg        = EVar (mkVar "arg")
        , exeGuard      = Nothing
        }
  where
    choiceName = ChoiceName (T.intercalate "." $ unTypeConName $ qualObject choice)

convertPrim version "UExerciseInterfaceGuarded" _
    | not (version `supports` featureExtendedInterfaces) =
        conversionError "Guards on choice exercises are only available with --target=1.dev"

convertPrim _ "UExerciseInterfaceGuarded"
    (   TContractId (TCon iface)
    :-> TCon choice
    :-> (TCon iface2 :-> TBuiltin BTBool)
    :->  TUpdate _returnTy)
    | iface == iface2 =
    pure $
    ETmLam (mkVar "this", TContractId (TCon iface)) $
    ETmLam (mkVar "arg", TCon choice) $
    ETmLam (mkVar "pred", TCon iface :-> TBuiltin BTBool) $
    EUpdate $ UExerciseInterface
        { exeInterface  = iface
        , exeChoice     = choiceName
        , exeContractId = EVar (mkVar "this")
        , exeArg        = EVar (mkVar "arg")
        , exeGuard      = Just (EVar (mkVar "pred"))
        }
  where
    choiceName = ChoiceName (T.intercalate "." $ unTypeConName $ qualObject choice)

convertPrim _ "UExerciseByKey"
    (tProxy@(TApp _ (TCon template)) :-> key :-> TCon choice :-> TUpdate _returnTy) =
    pure $
    ETmLam (mkVar "_", tProxy) $
    ETmLam (mkVar "key", key) $
    ETmLam (mkVar "arg", TCon choice) $
    EUpdate $ UExerciseByKey template choiceName (EVar (mkVar "key")) (EVar (mkVar "arg"))
  where
    choiceName = ChoiceName (T.intercalate "." $ unTypeConName $ qualObject choice)

convertPrim _ "ULookupByKey" (key :-> TUpdate (TOptional (TContractId (TCon template)))) =
    pure $
      ETmLam (mkVar "key", key) $ EUpdate $
        ULookupByKey $ RetrieveByKey template (EVar $ mkVar "key")

convertPrim _ "UFetchByKey"
    (key :-> TUpdate ty@(TApp (TApp (TCon tuple) ty1@(TContractId (TCon template))) ty2))
    | ty2 == TCon template =
    pure $
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

convertPrim _ "ETemplateTypeRep"
    (tProxy@(TApp _ tCon@(TCon _)) :-> TTypeRep) =
    pure $
    ETmLam (mkVar "_", tProxy) $
    ETypeRep tCon

convertPrim _ "EFromAnyTemplate"
    (TAny :-> TOptional (TCon template)) =
    pure $
    ETmLam (mkVar "any", TAny) $
    EFromAny (TCon template) (EVar $ mkVar "any")

convertPrim _ "EFromAnyTemplateChoice"
    (tProxy :-> TAny :-> TOptional choice) =
    pure $
    ETmLam (mkVar "_", tProxy) $
    ETmLam (mkVar "any", TAny) $
    EFromAny choice (EVar $ mkVar "any")

convertPrim _ "EFromAnyInterfaceChoice"
    (tProxy :-> TAny :-> TOptional choice) =
    pure $
    ETmLam (mkVar "_", tProxy) $
    ETmLam (mkVar "any", TAny) $
    ECase (EFromAny (mkTAnyInterfaceChoice choice) (EVar $ mkVar "any"))
      [  CaseAlternative (CPSome $ mkVar "x") (ESome choice $ projChoice choice (EVar $ mkVar "x"))
      ,  CaseAlternative CPDefault (ENone choice) ]

convertPrim _ "EFromAnyContractKey"
    (tProxy@(TApp _ (TCon _)) :-> TAny :-> TOptional key) =
    pure $
    ETmLam (mkVar "_", tProxy) $
    ETmLam (mkVar "any", TAny) $
    EFromAny key (EVar $ mkVar "any")

convertPrim _ "EToAnyTemplate"
    (TCon template :-> TAny) =
    pure $
    ETmLam (mkVar "template", TCon template) $
    EToAny (TCon template) (EVar $ mkVar "template")

convertPrim _ "EToAnyTemplateChoice"
    (tProxy :-> choice :-> TAny) =
    pure $
    ETmLam (mkVar "_", tProxy) $
    ETmLam (mkVar "choice", choice) $
    EToAny choice (EVar $ mkVar "choice")

convertPrim _ "EToAnyInterfaceChoice"
    (tProxy@(TApp _ (TCon typeId)) :-> choice :-> TAny) =
    pure $
    ETmLam (mkVar "_", tProxy) $
    ETmLam (mkVar "choice", choice) $
    EToAny (mkTAnyInterfaceChoice choice) (mkEAnyInterfaceChoice choice typeId $ EVar $ mkVar "choice")

convertPrim _ "EToAnyContractKey"
    (tProxy@(TApp _ (TCon _)) :-> key :-> TAny) =
    pure $
    ETmLam (mkVar "_", tProxy) $
    ETmLam (mkVar "key", key) $
    EToAny key (EVar $ mkVar "key")

convertPrim _ "EInterfaceTemplateTypeRep" (TCon interface :-> TTypeRep) =
    pure $
    ETmLam (mkVar "this", TCon interface) $
    EInterfaceTemplateTypeRep interface (EVar (mkVar "this"))

convertPrim _ "ESignatoryInterface" (TCon interface :-> TList TParty) =
    pure $
    ETmLam (mkVar "this", TCon interface) $
    ESignatoryInterface interface (EVar (mkVar "this"))

convertPrim _ "EObserverInterface" (TCon interface :-> TList TParty) =
    pure $
    ETmLam (mkVar "this", TCon interface) $
    EObserverInterface interface (EVar (mkVar "this"))

-- Exceptions
convertPrim _ "BEAnyExceptionMessage" (TBuiltin BTAnyException :-> TText) =
    pure $ EBuiltin BEAnyExceptionMessage

convertPrim _ "EThrow" (ty1 :-> ty2) =
    pure $ ETmLam (mkVar "x", ty1) (EThrow ty2 ty1 (EVar (mkVar "x")))
convertPrim _ "EToAnyException" (ty :-> TBuiltin BTAnyException) =
    pure $ ETmLam (mkVar "x", ty) (EToAnyException ty (EVar (mkVar "x")))
convertPrim _ "EFromAnyException" (TBuiltin BTAnyException :-> TOptional ty) =
    pure $ ETmLam (mkVar "x", TBuiltin BTAnyException) (EFromAnyException ty (EVar (mkVar "x")))

convertPrim _ "UTryCatch" ((TUnit :-> TUpdate t1) :-> (TBuiltin BTAnyException :-> TOptional (TUpdate t2)) :-> TUpdate t3)
    | t1 == t2, t2 == t3
        = pure
        $ ETmLam (mkVar "t", TUnit :-> TUpdate t1)
        $ ETmLam (mkVar "c", TBuiltin BTAnyException :-> TOptional (TUpdate t2))
        $ EUpdate
        $ UTryCatch t3
            (EVar (mkVar "t") `ETmApp` EUnit)
            (mkVar "x")
            (EVar (mkVar "c") `ETmApp` EVar (mkVar "x"))

convertPrim _ "EToInterface" (TCon tpid :-> TCon iface) =
    pure $
      ETmLam (mkVar "t", TCon tpid) $
        EToInterface iface tpid (EVar $ mkVar "t")

convertPrim _ "EFromInterface" (TCon iface :-> TOptional (TCon tpid)) =
    pure $
      ETmLam (mkVar "i", TCon iface) $
        EFromInterface iface tpid (EVar $ mkVar "i")

convertPrim _ "EUnsafeFromInterface" (TContractId (TCon iface) :-> TCon iface1 :-> TCon tpid)
    | iface == iface1
        = pure
        $ ETmLam (mkVar "cid", TContractId (TCon iface))
        $ ETmLam (mkVar "i", TCon iface)
        $ EUnsafeFromInterface iface tpid (EVar $ mkVar "cid") (EVar $ mkVar "i")

convertPrim _ "EToRequiredInterface" (TCon subIface :-> TCon superIface) =
    pure $
      ETmLam (mkVar "i", TCon subIface) $
        EToRequiredInterface superIface subIface (EVar $ mkVar "i")

convertPrim _ "EToRequiredInterface" ty@(TCon _ :-> retTy) =
    pure $ runtimeError ty $ "Tried to convert to a required interface '" <> T.pack (renderPretty retTy) <> "', but that type is not an interface."

convertPrim _ "EFromRequiredInterface" (TCon superIface :-> TOptional (TCon subIface)) =
    pure $
      ETmLam (mkVar "i", TCon superIface) $
        EFromRequiredInterface superIface subIface (EVar $ mkVar "i")

convertPrim _ "EFromRequiredInterface" ty@(fromTy :-> TOptional (TCon _)) =
    pure $ runtimeError ty $ "Tried to convert from a required interface '" <> T.pack (renderPretty fromTy) <> "', but that type is not an interface."

convertPrim _ "EUnsafeFromRequiredInterface" (TContractId (TCon superIface) :-> TCon superIface1 :-> TCon subIface)
    | superIface == superIface1
        = pure
        $ ETmLam (mkVar "cid", TContractId (TCon superIface))
        $ ETmLam (mkVar "i", TCon superIface)
        $ EUnsafeFromRequiredInterface superIface subIface (EVar $ mkVar "cid") (EVar $ mkVar "i")

convertPrim _ "EUnsafeFromRequiredInterface" ty@(TContractId fromTy :-> fromTy1 :-> TCon _)
    | fromTy == fromTy1
        = pure $ runtimeError ty $ "Tried to unsafely convert from a required interface '" <> T.pack (renderPretty fromTy) <> "', but that type is not an interface."

convertPrim _ "ETypeRepTyConName" (TTypeRep :-> TOptional TText) = pure $ EBuiltin BETypeRepTyConName

convertPrim _ "EViewInterface" (TCon iface :-> _) =
    pure $
      ETmLam (mkVar "i", TCon iface) $
        EViewInterface iface (EVar $ mkVar "i")

convertPrim (V1 PointDev) (L.stripPrefix "$" -> Just builtin) typ =
    pure $
      EExperimental (T.pack builtin) typ

-- Unknown primitive.
convertPrim _ x ty = conversionError $ "Unknown primitive " ++ show x ++ " at type " ++ renderPretty ty

typeRepField, choiceField :: FieldName
typeRepField = FieldName "choiceInterfaceIdRep"
choiceField = FieldName "choice"

mkTAnyInterfaceChoice :: Type -> Type
mkTAnyInterfaceChoice t = TStruct [(typeRepField, TTypeRep), (choiceField, t)]

mkEAnyInterfaceChoice :: Type -> Qualified TypeConName -> Expr -> Expr
mkEAnyInterfaceChoice _ typeId e = EStructCon [(typeRepField, ETypeRep (TCon typeId)), (choiceField, e)]

projChoice :: Type -> Expr -> Expr
projChoice _ = EStructProj choiceField

-- | Some builtins are only supported in specific versions of Daml-LF.
whenRuntimeSupports :: Version -> Feature -> Type -> Expr -> Expr
whenRuntimeSupports version feature t e
    | version `supports` feature = e
    | otherwise = runtimeError t (featureErrorMessage feature)

runtimeError :: Type -> T.Text -> Expr
runtimeError t msg = ETmApp (ETyApp (EBuiltin BEError) t) (EBuiltin (BEText msg))

featureErrorMessage :: Feature -> T.Text
featureErrorMessage (Feature name version _) =
  name <> " only supported when compiling to Daml-LF " <> T.pack (renderVersion version) <> " or later"
