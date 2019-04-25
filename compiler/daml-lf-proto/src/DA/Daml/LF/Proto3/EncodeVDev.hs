-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Encoding of the LF package into LF dev format.
module DA.Daml.LF.Proto3.EncodeVDev
  ( encodePackage
  , encodeModule
  ) where

import           Control.Lens ((^.), matching)
import           Control.Lens.Ast (rightSpine)

import qualified Data.NameMap as NM
import qualified Data.Text           as T
import qualified Data.Text.Lazy      as TL
import qualified Data.Vector         as V
import qualified Data.Scientific     as Scientific
import           GHC.Stack (HasCallStack)

import           DA.Prelude
import qualified DA.Daml.LF.Decimal  as Decimal
import           DA.Daml.LF.Ast
import           DA.Daml.LF.Mangling
import qualified Da.DamlLfDev as P

import qualified Proto3.Suite as P (Enumerated (..))

-- | Encoding 'from' to type 'to'
class Encode from to | from -> to where
  encode :: from -> to

------------------------------------------------------------------------
-- Simple encodings
------------------------------------------------------------------------

encodeV :: Encode a b => [a] -> V.Vector b
encodeV = V.fromList . map encode

encodeNM :: (NM.Named a, Encode a b) => NM.NameMap a -> V.Vector b
encodeNM = V.fromList . map encode . NM.toList

encodeE :: Encode a b => a -> P.Enumerated b
encodeE = P.Enumerated . Right . encode

encode' :: Encode a b => a -> Maybe b
encode' = Just . encode

instance Encode PackageId TL.Text where
   encode = TL.fromStrict . unTagged

instance Encode PartyLiteral TL.Text where
   encode = TL.fromStrict . unTagged

instance Encode FieldName TL.Text where
   encode = encodeIdentifier . unTagged

instance Encode VariantConName TL.Text where
   encode = encodeIdentifier . unTagged

instance Encode TypeVarName TL.Text where
   encode = encodeIdentifier . unTagged

instance Encode ExprVarName TL.Text where
   encode = encodeIdentifier . unTagged

instance Encode ChoiceName TL.Text where
   encode = encodeIdentifier . unTagged

encodeIdentifier :: T.Text -> TL.Text
encodeIdentifier unmangled = case mangleIdentifier unmangled of
   Left err -> error ("IMPOSSIBLE: could not mangle name " ++ show unmangled ++ ": " ++ err)
   Right x -> TL.fromStrict x

instance Encode T.Text TL.Text where
  encode = TL.fromStrict

instance Encode Decimal.Decimal TL.Text where
  encode d = enc (Decimal.normalize d)
    where
      enc (Decimal.Decimal n k0) =
        TL.pack
          $ Scientific.formatScientific Scientific.Fixed Nothing
          $ Scientific.scientific n (fromIntegral (negate k0))

instance Encode (Qualified TypeConName) P.TypeConName where
  encode (Qualified pref mname con) = P.TypeConName (Just $ P.ModuleRef (encode' pref) (encode' mname)) (encode' con)

instance Encode (Qualified ExprValName) P.ValName where
  encode (Qualified pref mname con) = P.ValName (Just $ P.ModuleRef (encode' pref) (encode' mname)) (encodeDefName (unTagged con))

instance Encode SourceLoc P.Location where
  encode SourceLoc{..} =
    P.Location
      (fmap
        (\(p, m) -> P.ModuleRef (encode' p) (encode' m))
        slocModuleRef)
      (Just (P.Location_Range
        (fromIntegral slocStartLine)
        (fromIntegral slocStartCol)
        (fromIntegral slocEndLine)
        (fromIntegral slocEndCol)))

instance Encode PackageRef P.PackageRef where
  encode = \case
      PRSelf -> P.PackageRef (Just (P.PackageRefSumSelf P.Unit))
      PRImport pkgid -> P.PackageRef (Just (P.PackageRefSumPackageId $ encode pkgid))

encodeDottedName :: [T.Text] -> P.DottedName
encodeDottedName unmangled = case mangled of
  Left (which, err) -> error ("IMPOSSIBLE: could not mangle name " ++ show which ++ ": " ++ err)
  Right xs -> P.DottedName (V.fromList xs)
  where
    mangled = forM unmangled $ \part -> case mangleIdentifier part of
      Left err -> Left (part, err)
      Right x -> Right (TL.fromStrict x)

instance Encode ModuleName P.DottedName where
  encode = encodeDottedName . unTagged

instance Encode TypeConName P.DottedName where
  encode = encodeDottedName . unTagged

instance Encode (FieldName, Type) P.FieldWithType where
  encode (name, typ) = P.FieldWithType (encode name) (encode' typ)

instance Encode (FieldName, Expr) P.FieldWithExpr where
  encode (name, expr) = P.FieldWithExpr (encode name) (encode' expr)

instance Encode (VariantConName, Type) P.FieldWithType where
  encode (name, typ) = P.FieldWithType (encode name) (encode' typ)

instance Encode (TypeVarName, Kind) P.TypeVarWithKind where
  encode (name, kind)  = P.TypeVarWithKind (encode name) (encode' kind)

instance Encode (ExprVarName, Type) P.VarWithType where
  encode (name, typ) = P.VarWithType (encode name) (encode' typ)

------------------------------------------------------------------------
-- Encoding of kinds
------------------------------------------------------------------------

instance Encode Kind P.Kind where
  encode = P.Kind . Just . \case
    KStar -> P.KindSumStar P.Unit
    k@KArrow{} ->
      let (params, result) = k ^. rightSpine _KArrow
      in P.KindSumArrow (P.Kind_Arrow (encodeV params) (encode' result))

------------------------------------------------------------------------
-- Encoding of types
------------------------------------------------------------------------

instance Encode BuiltinType P.PrimType where
  encode = \case
    BTInt64 -> P.PrimTypeINT64
    BTDecimal -> P.PrimTypeDECIMAL
    BTText -> P.PrimTypeTEXT
    BTTimestamp -> P.PrimTypeTIMESTAMP
    BTParty -> P.PrimTypePARTY
    BTEnum et -> case et of
      ETUnit -> P.PrimTypeUNIT
      ETBool -> P.PrimTypeBOOL
    BTList -> P.PrimTypeLIST
    BTUpdate -> P.PrimTypeUPDATE
    BTScenario -> P.PrimTypeSCENARIO
    BTDate -> P.PrimTypeDATE
    BTContractId -> P.PrimTypeCONTRACT_ID
    BTOptional -> P.PrimTypeOPTIONAL
    BTMap -> P.PrimTypeMAP
    BTArrow -> P.PrimTypeARROW

instance Encode Type P.Type where
  encode typ = P.Type . Just $
    case typ ^. _TApps of
      (TVar var, args) ->
        P.TypeSumVar (P.Type_Var (encode var) (encodeV args))
      (TCon con, args) ->
        P.TypeSumCon (P.Type_Con (encode' con) (encodeV args))
      (TBuiltin bltn, args) ->
        P.TypeSumPrim (P.Type_Prim (encodeE bltn) (encodeV args))
      (t@TForall{}, []) ->
          let (binders, body) = t ^. _TForalls
          in P.TypeSumForall (P.Type_Forall (encodeV binders) (encode' body))
      (TTuple flds, []) -> P.TypeSumTuple (P.Type_Tuple (encodeV flds))

      (TApp{}, _) -> error "TApp after unwinding TApp"
      (TTuple{}, _:_) -> error "Application of TTuple"
      -- NOTE(MH): The following case requires impredicative polymorphism,
      -- which we don't support.
      (TForall{}, _:_) -> error "Application of TForall"

------------------------------------------------------------------------
-- Encoding of expressions
------------------------------------------------------------------------

instance Encode EnumCon P.PrimCon where
  encode = \case
    ECUnit -> P.PrimConCON_UNIT
    ECFalse -> P.PrimConCON_FALSE
    ECTrue -> P.PrimConCON_TRUE

instance Encode TypeConApp P.Type_Con where
  encode (TypeConApp tycon args) = P.Type_Con (encode' tycon) (encodeV args)

instance Encode BuiltinExpr P.ExprSum where
  encode = \case
    BEInt64 x -> lit $ P.PrimLitSumInt64 x
    BEDecimal dec -> lit $ P.PrimLitSumDecimal (TL.pack (show dec))
    BEText x -> lit $ P.PrimLitSumText (TL.fromStrict x)
    BETimestamp x -> lit $ P.PrimLitSumTimestamp x
    BEParty x -> lit $ P.PrimLitSumParty (encode x)
    BEDate x -> lit $ P.PrimLitSumDate x

    BEEnumCon con -> P.ExprSumPrimCon (encodeE con)

    BEEqual typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionEQUAL_INT64
      BTDecimal -> builtin P.BuiltinFunctionEQUAL_DECIMAL
      BTText -> builtin P.BuiltinFunctionEQUAL_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionEQUAL_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionEQUAL_DATE
      BTParty -> builtin P.BuiltinFunctionEQUAL_PARTY
      BTEnum ETBool -> builtin P.BuiltinFunctionEQUAL_BOOL
      other -> error $ "BEEqual unexpected type " <> show other

    BELessEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLEQ_INT64
      BTDecimal -> builtin P.BuiltinFunctionLEQ_DECIMAL
      BTText -> builtin P.BuiltinFunctionLEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLEQ_DATE
      BTParty -> builtin P.BuiltinFunctionLEQ_PARTY
      other -> error $ "BELessEq unexpected type " <> show other

    BELess typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLESS_INT64
      BTDecimal -> builtin P.BuiltinFunctionLESS_DECIMAL
      BTText -> builtin P.BuiltinFunctionLESS_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLESS_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLESS_DATE
      BTParty -> builtin P.BuiltinFunctionLESS_PARTY
      other -> error $ "BELess unexpected type " <> show other

    BEGreaterEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGEQ_INT64
      BTDecimal -> builtin P.BuiltinFunctionGEQ_DECIMAL
      BTText -> builtin P.BuiltinFunctionGEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionGEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionGEQ_DATE
      BTParty -> builtin P.BuiltinFunctionGEQ_PARTY
      other -> error $ "BEGreaterEq unexpected type " <> show other

    BEGreater typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGREATER_INT64
      BTDecimal -> builtin P.BuiltinFunctionGREATER_DECIMAL
      BTText -> builtin P.BuiltinFunctionGREATER_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionGREATER_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionGREATER_DATE
      BTParty -> builtin P.BuiltinFunctionGREATER_PARTY
      other -> error $ "BEGreater unexpected type " <> show other

    BEToText typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionTO_TEXT_INT64
      BTDecimal -> builtin P.BuiltinFunctionTO_TEXT_DECIMAL
      BTText -> builtin P.BuiltinFunctionTO_TEXT_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionTO_TEXT_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionTO_TEXT_DATE
      BTParty -> builtin P.BuiltinFunctionTO_TEXT_PARTY
      other -> error $ "BEToText unexpected type " <> show other
    BEPartyFromText -> builtin P.BuiltinFunctionFROM_TEXT_PARTY
    BEPartyToQuotedText -> builtin P.BuiltinFunctionTO_QUOTED_TEXT_PARTY

    BEAddDecimal -> builtin P.BuiltinFunctionADD_DECIMAL
    BESubDecimal -> builtin P.BuiltinFunctionSUB_DECIMAL
    BEMulDecimal -> builtin P.BuiltinFunctionMUL_DECIMAL
    BEDivDecimal -> builtin P.BuiltinFunctionDIV_DECIMAL
    BERoundDecimal -> builtin P.BuiltinFunctionROUND_DECIMAL

    BEAddInt64 -> builtin P.BuiltinFunctionADD_INT64
    BESubInt64 -> builtin P.BuiltinFunctionSUB_INT64
    BEMulInt64 -> builtin P.BuiltinFunctionMUL_INT64
    BEDivInt64 -> builtin P.BuiltinFunctionDIV_INT64
    BEModInt64 -> builtin P.BuiltinFunctionMOD_INT64
    BEExpInt64 -> builtin P.BuiltinFunctionEXP_INT64

    BEInt64ToDecimal -> builtin P.BuiltinFunctionINT64_TO_DECIMAL
    BEDecimalToInt64 -> builtin P.BuiltinFunctionDECIMAL_TO_INT64

    BEFoldl -> builtin P.BuiltinFunctionFOLDL
    BEFoldr -> builtin P.BuiltinFunctionFOLDR
    BEEqualList -> builtin P.BuiltinFunctionEQUAL_LIST
    BEExplodeText -> builtin P.BuiltinFunctionEXPLODE_TEXT
    BEAppendText -> builtin P.BuiltinFunctionAPPEND_TEXT
    BEImplodeText -> builtin P.BuiltinFunctionIMPLODE_TEXT
    BESha256Text -> builtin P.BuiltinFunctionSHA256_TEXT
    BEError -> builtin P.BuiltinFunctionERROR

    BEMapEmpty -> builtin P.BuiltinFunctionMAP_EMPTY
    BEMapInsert -> builtin P.BuiltinFunctionMAP_INSERT
    BEMapLookup -> builtin P.BuiltinFunctionMAP_LOOKUP
    BEMapDelete -> builtin P.BuiltinFunctionMAP_DELETE
    BEMapSize -> builtin P.BuiltinFunctionMAP_SIZE
    BEMapToList -> builtin P.BuiltinFunctionMAP_TO_LIST

    BETimestampToUnixMicroseconds -> builtin P.BuiltinFunctionTIMESTAMP_TO_UNIX_MICROSECONDS
    BEUnixMicrosecondsToTimestamp -> builtin P.BuiltinFunctionUNIX_MICROSECONDS_TO_TIMESTAMP

    BEDateToUnixDays -> builtin P.BuiltinFunctionDATE_TO_UNIX_DAYS
    BEUnixDaysToDate -> builtin P.BuiltinFunctionUNIX_DAYS_TO_DATE

    BETrace -> builtin P.BuiltinFunctionTRACE
    BEEqualContractId -> builtin P.BuiltinFunctionEQUAL_CONTRACT_ID

    where
      builtin = P.ExprSumBuiltin . P.Enumerated . Right
      lit = P.ExprSumPrimLit . P.PrimLit . Just

instance Encode Expr P.Expr where
  encode = encodeExpr

encodeExpr :: Expr -> P.Expr
encodeExpr = \case
  EVar v -> expr $ P.ExprSumVar (encode v)
  EVal v -> expr $ P.ExprSumVal (encode v)
  EBuiltin bi -> expr $ encode bi
  ERecCon{..} -> expr $ P.ExprSumRecCon $ P.Expr_RecCon (encode' recTypeCon) (encodeV recFields)
  ERecProj{..} -> expr $ P.ExprSumRecProj $ P.Expr_RecProj (encode' recTypeCon) (encode recField) (encode' recExpr)
  ERecUpd{..} -> expr $ P.ExprSumRecUpd $ P.Expr_RecUpd (encode' recTypeCon) (encode recField) (encode' recExpr) (encode' recUpdate)
  EVariantCon{..} -> expr $ P.ExprSumVariantCon $ P.Expr_VariantCon (encode' varTypeCon) (encode varVariant) (encode' varArg)
  ETupleCon{..} -> expr $ P.ExprSumTupleCon $ P.Expr_TupleCon (encodeV tupFields)
  ETupleProj{..} -> expr $ P.ExprSumTupleProj $ P.Expr_TupleProj (encode tupField) (encode' tupExpr)
  ETupleUpd{..} -> expr $ P.ExprSumTupleUpd $ P.Expr_TupleUpd (encode tupField) (encode' tupExpr) (encode' tupUpdate)
  e@ETmApp{} ->
      let (fun, args) = e ^. _ETmApps
      in expr $ P.ExprSumApp $ P.Expr_App (encode' fun) (encodeV args)
  e@ETyApp{} ->
      let (fun, args) = e ^. _ETyApps
      in expr $ P.ExprSumTyApp $ P.Expr_TyApp (encode' fun) (encodeV args)
  e@ETmLam{} ->
      let (params, body) = e ^. _ETmLams
      in expr $ P.ExprSumAbs $ P.Expr_Abs (encodeV params) (encode' body)
  e@ETyLam{} ->
      let (params, body) = e ^. _ETyLams
      in expr $ P.ExprSumTyAbs $ P.Expr_TyAbs (encodeV params) (encode' body)
  ECase{..} -> expr $ P.ExprSumCase $ P.Case (encode' casScrutinee) (encodeV casAlternatives)
  e@ELet{} ->
      let (lets, body) = e ^. _ELets
      in expr $ P.ExprSumLet $ P.Block (encodeV lets) (encode' body)
  ENil{..} -> expr $ P.ExprSumNil $ P.Expr_Nil (encode' nilType)
  ECons{..} ->
      let unwind e0 as = case matching _ECons e0 of
            Left e1 -> (e1, as)
            Right (typ, hd, tl)
              | typ /= consType -> error "internal error: unexpected mismatch in cons cell type"
              | otherwise -> unwind tl (hd:as)
          (ctail, cfront) = unwind consTail [consHead]
      in expr $ P.ExprSumCons $ P.Expr_Cons (encode' consType) (encodeV $ reverse cfront) (encode' ctail)
  EUpdate u -> expr $ P.ExprSumUpdate $ encode u
  EScenario s -> expr $ P.ExprSumScenario $ encode s
  ELocation loc e ->
    let (P.Expr _ esum) = encodeExpr e
    in P.Expr (Just (encode loc)) esum
  ENone typ -> expr (P.ExprSumNone (P.Expr_None (encode' typ)))
  ESome typ body -> expr (P.ExprSumSome (P.Expr_Some (encode' typ) (encode' body)))
  where
    expr = P.Expr Nothing . Just

instance Encode Update P.Update where
  encode = P.Update . Just . \case
    UPure{..} -> P.UpdateSumPure $ P.Pure (encode' pureType) (encode' pureExpr)
    e@UBind{} ->
      let (bindings, body) = EUpdate e ^. rightSpine (_EUpdate . _UBind)
      in P.UpdateSumBlock $ P.Block (encodeV bindings) (encode' body)
    UCreate{..} -> P.UpdateSumCreate $ P.Update_Create (encode' creTemplate) (encode' creArg)
    UExercise{..} -> P.UpdateSumExercise $ P.Update_Exercise (encode' exeTemplate) (encode exeChoice) (encode' exeContractId) (encode' exeActors) (encode' exeArg)
    UFetch{..} -> P.UpdateSumFetch $ P.Update_Fetch (encode' fetTemplate) (encode' fetContractId)
    UGetTime -> P.UpdateSumGetTime P.Unit
    UEmbedExpr typ e -> P.UpdateSumEmbedExpr $ P.Update_EmbedExpr (encode' typ) (encode' e)
    UFetchByKey rbk -> P.UpdateSumFetchByKey (encode rbk)
    ULookupByKey rbk -> P.UpdateSumLookupByKey (encode rbk)

instance Encode RetrieveByKey P.Update_RetrieveByKey where
  encode RetrieveByKey{..} = P.Update_RetrieveByKey
    (encode' retrieveByKeyTemplate)
    (encode' retrieveByKeyKey)

instance Encode Scenario P.Scenario where
  encode = P.Scenario . Just . \case
    SPure{..} -> P.ScenarioSumPure $ P.Pure (encode' spureType) (encode' spureExpr)
    e@SBind{} ->
      let (bindings, body) = EScenario e ^. rightSpine (_EScenario . _SBind)
      in P.ScenarioSumBlock $ P.Block (encodeV bindings) (encode' body)
    SCommit{..} ->
      P.ScenarioSumCommit $ P.Scenario_Commit
        (encode' scommitParty)
        (encode' scommitExpr)
        (encode' scommitType)
    SMustFailAt{..} ->
      P.ScenarioSumMustFailAt $ P.Scenario_Commit
        (encode' smustFailAtParty)
        (encode' smustFailAtExpr)
        (encode' smustFailAtType)
    SPass{..} ->
      P.ScenarioSumPass (encode spassDelta)
    SGetTime -> P.ScenarioSumGetTime P.Unit
    SGetParty{..} ->
      P.ScenarioSumGetParty (encode sgetPartyName)
    SEmbedExpr typ e -> P.ScenarioSumEmbedExpr $ P.Scenario_EmbedExpr (encode' typ) (encode' e)

instance Encode Binding P.Binding where
  encode (Binding binder bound) = P.Binding (encode' binder) (encode' bound)

instance Encode CaseAlternative P.CaseAlt where
  encode CaseAlternative{..} =
    let pat = case altPattern of
          CPDefault     -> P.CaseAltSumDefault P.Unit
          CPVariant{..} -> P.CaseAltSumVariant $ P.CaseAlt_Variant (encode' patTypeCon) (encode patVariant) (encode patBinder)
          CPEnumCon con -> P.CaseAltSumPrimCon (encodeE con)
          CPNil         -> P.CaseAltSumNil P.Unit
          CPCons{..}    -> P.CaseAltSumCons $ P.CaseAlt_Cons (encode patHeadBinder) (encode patTailBinder)
          CPNone        -> P.CaseAltSumNone P.Unit
          CPSome{..}    -> P.CaseAltSumSome $ P.CaseAlt_Some (encode patBodyBinder)
    in P.CaseAlt (Just pat) (encode' altExpr)

instance Encode DefDataType P.DefDataType where
  encode DefDataType{..} =
      P.DefDataType (encode' dataTypeCon) (encodeV dataParams)
      (Just $ case dataCons of
        DataRecord fs -> P.DefDataTypeDataConsRecord $ P.DefDataType_Fields (encodeV fs)
        DataVariant fs -> P.DefDataTypeDataConsVariant $ P.DefDataType_Fields (encodeV fs))
      (getIsSerializable dataSerializable)
      (encode <$> dataLocation)

instance Encode DefValue P.DefValue where
  encode DefValue{..} =
    P.DefValue
      (Just (P.DefValue_NameWithType (encodeDefName (unTagged (fst dvalBinder))) (encode' (snd dvalBinder))))
      (encode' dvalBody)
      (getHasNoPartyLiterals dvalNoPartyLiterals)
      (getIsTest dvalIsTest)
      (encode <$> dvalLocation)

instance Encode Template P.DefTemplate where
  encode Template{..} =
    P.DefTemplate
    { P.defTemplateTycon = encode' tplTypeCon
    , P.defTemplateParam = encode tplParam
    , P.defTemplatePrecond = encode' tplPrecondition
    , P.defTemplateSignatories = encode' tplSignatories
    , P.defTemplateObservers = encode' tplObservers
    , P.defTemplateAgreement = encode' tplAgreement
    , P.defTemplateChoices = encodeNM tplChoices
    , P.defTemplateLocation = encode <$> tplLocation
    , P.defTemplateKey = fmap (encodeTemplateKey tplParam) tplKey
    }

encodeTemplateKey :: ExprVarName -> TemplateKey -> P.DefTemplate_DefKey
encodeTemplateKey templateVar TemplateKey{..} = P.DefTemplate_DefKey
  { P.defTemplate_DefKeyType = encode' tplKeyType
  , P.defTemplate_DefKeyKey = case encodeKeyExpr templateVar tplKeyBody of
      Left err -> error err
      Right x -> Just (P.KeyExpr (encode' x))
  , P.defTemplate_DefKeyMaintainers = encode' tplKeyMaintainers
  }

data KeyExpr =
    KeyExprProjections ![(TypeConApp, FieldName)]
  | KeyExprRecord !TypeConApp ![(FieldName, KeyExpr)]

instance Encode KeyExpr P.KeyExprSum where
  encode = \case
    KeyExprProjections projs -> P.KeyExprSumProjections $ P.KeyExpr_Projections $ V.fromList $ do
      (tyCon, fld) <- projs
      return (P.KeyExpr_Projection (encode' tyCon) (encode fld))
    KeyExprRecord tyCon flds -> P.KeyExprSumRecord $ P.KeyExpr_Record (encode' tyCon) $ V.fromList $ do
      (fldName, ke) <- flds
      return (P.KeyExpr_RecordField (encode fldName) (Just (P.KeyExpr (encode' ke))))

encodeKeyExpr :: ExprVarName -> Expr -> Either String KeyExpr
encodeKeyExpr tplParameter = \case
  EVar var -> if var == tplParameter
    then Right (KeyExprProjections [])
    else Left ("Expecting variable " ++ show tplParameter ++ " in key expression, got " ++ show var)
  ERecProj tyCon fld e -> do
    keyExpr <- encodeKeyExpr tplParameter e
    case keyExpr of
      KeyExprProjections projs -> return (KeyExprProjections (projs ++ [(tyCon, fld)]))
      KeyExprRecord{} -> Left "Trying to project out of a record in key expression"
  ERecCon tyCon flds -> do
    keyFlds <- mapM (\(lbl, e) -> (lbl, ) <$> encodeKeyExpr tplParameter e) flds
    return (KeyExprRecord tyCon keyFlds)
  e -> Left ("Bad key expression " ++ show e)

instance Encode TemplateChoice P.TemplateChoice where
  encode TemplateChoice{..} =
    P.TemplateChoice
    { P.templateChoiceName = encode chcName
    , P.templateChoiceConsuming = chcConsuming
    , P.templateChoiceControllers = encode' chcControllers
    , P.templateChoiceSelfBinder = encode chcSelfBinder
    , P.templateChoiceArgBinder = encode' chcArgBinder
    , P.templateChoiceRetType = encode' chcReturnType
    , P.templateChoiceUpdate = encode' chcUpdate
    , P.templateChoiceLocation = encode <$> chcLocation
    }

instance Encode FeatureFlags P.FeatureFlags where
  encode FeatureFlags{..} =  P.FeatureFlags
    { P.featureFlagsForbidPartyLiterals = forbidPartyLiterals
    }


instance Encode Module P.Module where
  encode Module{..} =
    P.Module (encode' moduleName)
      (encode' moduleFeatureFlags)
      (encodeNM moduleDataTypes)
      (encodeNM moduleValues)
      (encodeNM moduleTemplates)

encodeModule :: Module -> P.Module
encodeModule = encode

-- | NOTE(MH): Assumes the 'MajorVersion' of the 'Package' is 'VDev'.
encodePackage :: Package -> P.Package
encodePackage (Package _version mods) = P.Package (encodeNM mods)

-- | For now, value names are always encoded using a single segment.
--
-- This is to keep backwards compat with older .dalf files, but also
-- because currently GenDALF generates weird names like `.` that we'd
-- have to handle separatedly. So for now, considering that we do not
-- use values in codegen, just mangle the entire thing.
encodeDefName :: (HasCallStack, Encode T.Text b) => T.Text -> V.Vector b
encodeDefName txt = case mangleIdentifier txt of
  Left err -> error ("IMPOSSIBLE: could not mangle def name " ++ show txt ++ ": " ++ err)
  Right mangled -> encodeV [mangled]
