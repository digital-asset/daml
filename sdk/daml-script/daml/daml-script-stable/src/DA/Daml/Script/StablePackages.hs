-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ImportQualifiedPost #-}

module DA.Daml.Script.StablePackages
    ( main
    ) where

import Data.Map.Strict qualified as MS
import Data.Text qualified as T
import System.Environment (getArgs)

import DA.Daml.Script.StablePackageBuilders
import DA.Daml.LF.Ast
import DA.Daml.StablePackages (stablePackageByModuleName)
import DA.Daml.UtilLF

main :: IO ()
main = do
  [action, path] <- getArgs
  case action of
    "generate" -> makeStableDars path allStablePackagesList
    "list" -> makeStablePackageList path allStablePackagesList
    _ -> error $ "Unknown action: " <> action

allStablePackagesList :: [Package]
allStablePackagesList =
  [ free
  , ledgerValue
  , question
  , scriptF
  , script
  , anyContractId
  , anyContractKey
  , command
  , commandWithMeta
  , commandResult
  , commands
  , disclosure
  , secp256k1KeyPair
  , partyDetails
  , partyIdHint
  , participantName
  , packageId
  , submitOptions
  , taggedRecord
  , anySubmitError
  , anyUpgradeErrorType
  , anyCryptoErrorType
  , anyExternalCallErrorType
  , anyDevErrorType
  , commandName
  , errorClassName
  , errorMessage
  , failedCmd
  , created
  , treeEventAndExercised
  , transactionTree
  , createdIndexPayload
  , treeIndexAndExercisedIndexPayload
  , userId
  , user
  , userRight
  , invalidUserId
  , userAlreadyExists
  , userNotFound
  ]

freeModule, lowLevelModule, utilModule, commandsModule, cryptoTextModule
  , partyManagementModule, submitModule, submitErrorModule, testingModule
  , transactionTreeModule, userManagementModule :: T.Text
freeModule = "Daml.Script.Internal.Free.Stable"
lowLevelModule = "Daml.Script.Internal.LowLevel.Stable"
utilModule = "Daml.Script.Internal.Questions.Util.Stable"
commandsModule = "Daml.Script.Internal.Questions.Commands.Stable"
cryptoTextModule = "Daml.Script.Internal.Questions.Crypto.Text.Stable"
partyManagementModule = "Daml.Script.Internal.Questions.PartyManagement.Stable"
submitModule = "Daml.Script.Internal.Questions.Submit.Stable"
submitErrorModule = "Daml.Script.Internal.Questions.Submit.Error.Stable"
testingModule = "Daml.Script.Internal.Questions.Testing.Stable"
transactionTreeModule = "Daml.Script.Internal.Questions.TransactionTree.Stable"
userManagementModule = "Daml.Script.Internal.Questions.UserManagement.Stable"

-- Modules of the packages holding (mutually) recursive types, named after the first type they
-- define. Bound because both the package definition and its selfTy references need them.
freeTypeModule, treeEventTypeModule, treeIndexTypeModule :: T.Text
freeTypeModule = freeModule <> ".Free"
treeEventTypeModule = transactionTreeModule <> ".TreeEvent"
treeIndexTypeModule = transactionTreeModule <> ".TreeIndex"

tVar :: T.Text -> Type
tVar = TVar . mkTypeVar

-- Look up one of damlc's stable packages by the module it defines
compilerStablePackage :: [T.Text] -> (PackageId, Package)
compilerStablePackage modNameParts =
  case MS.lookup (V2, mkModName modNameParts) stablePackageByModuleName of
    Just pkg -> pkg
    Nothing -> error $ "No stable package for module " <> show modNameParts

-- Reference a type from one of damlc's stable packages
compilerStableTy :: [T.Text] -> T.Text -> [Type] -> Type
compilerStableTy modNameParts tyName args =
    TConApp (Qualified (ImportedPackageId pkgId) (mkModName modNameParts) (mkTypeCon [tyName])) args
  where
    (pkgId, _) = compilerStablePackage modNameParts

daInternalAnyModule, daStackTypesModule, daTypesModule :: [T.Text]
daInternalAnyModule = ["DA", "Internal", "Any"]
daStackTypesModule = ["DA", "Stack", "Types"]
daTypesModule = ["DA", "Types"]

daInternalAnyPkg, daStackTypesPkg, daTypesPkg :: Package
daInternalAnyPkg = snd $ compilerStablePackage daInternalAnyModule
daStackTypesPkg = snd $ compilerStablePackage daStackTypesModule
daTypesPkg = snd $ compilerStablePackage daTypesModule

tTemplateTypeRep, tAnyTemplate, tAnyChoice, tSrcLoc :: Type
tTemplateTypeRep = compilerStableTy daInternalAnyModule "TemplateTypeRep" []
tAnyTemplate = compilerStableTy daInternalAnyModule "AnyTemplate" []
tAnyChoice = compilerStableTy daInternalAnyModule "AnyChoice" []
tSrcLoc = compilerStableTy daStackTypesModule "SrcLoc" []

tTuple2 :: Type -> Type -> Type
tTuple2 a b = compilerStableTy daTypesModule "Tuple2" [a, b]

tLedgerValue :: Type
tLedgerValue = depTy ledgerValue "LedgerValue" []

free :: Package
free = makePackage defaultPackageDef
  { packageDefModuleName = freeTypeModule
  , packageDefTypes = pure VariantDef
      { name = "Free"
      , typeParams = [("f", KStar `KArrow` KStar), ("a", KStar)]
      , constructors =
          [ ("Pure", tVar "a")
          , ("Free", TApp (tVar "f") (selfTy freeTypeModule "Free" [tVar "f", tVar "a"]))
          ]
      }
  }

ledgerValue :: Package
ledgerValue = makePackage defaultPackageDef
  { packageDefModuleName = lowLevelModule <> ".LedgerValue"
  , packageDefTypes = pure RecordDef
      { name = "LedgerValue"
      , typeParams = []
      , fields = []
      }
  }

question :: Package
question = makePackage defaultPackageDef
  { packageDefModuleName = lowLevelModule <> ".Question"
  , packageDefTypes = pure RecordDef
      { name = "Question"
      , typeParams = [("req", KStar), ("res", KStar), ("a", KStar)]
      , fields =
          [ ("commandName", TText)
          , ("commandVersion", TInt64)
          , ("payload", tVar "req")
          , ("locations", TList (tTuple2 TText tSrcLoc))
          , ("continue", tVar "res" :-> tVar "a")
          ]
      }
  , packageDefDependencies = [daTypesPkg, daStackTypesPkg]
  }

scriptF :: Package
scriptF = makePackage defaultPackageDef
  { packageDefModuleName = lowLevelModule <> ".ScriptF"
  , packageDefTypes = pure NewTypeDef
      { name = "ScriptF"
      , typeParams = [("a", KStar)]
      , mUnwrap = Nothing
      , typ = depTy question "Question" [tLedgerValue, tLedgerValue, tVar "a"]
      }
  , packageDefDependencies = [question, ledgerValue]
  }

script :: Package
script = makePackage defaultPackageDef
  { packageDefModuleName = lowLevelModule <> ".Script"
  , packageDefTypes = pure RecordDef
      { name = "Script"
      , typeParams = [("a", KStar)]
      , fields =
          [ ("runScript", TUnit :-> depTy free "Free" [depTy scriptF "ScriptF" [], tTuple2 (tVar "a") TUnit])
          , ("dummy", TUnit)
          ]
      }
  , packageDefDependencies = [free, scriptF, daTypesPkg]
  }

anyContractId :: Package
anyContractId = makePackage defaultPackageDef
  { packageDefModuleName = utilModule <> ".AnyContractId"
  , packageDefTypes = pure RecordDef
      { name = "AnyContractId"
      , typeParams = []
      , fields =
          [ ("templateId", tTemplateTypeRep)
          , ("contractId", TContractId TUnit)
          ]
      }
  , packageDefDependencies = [daInternalAnyPkg]
  }

anyContractKey :: Package
anyContractKey = makePackage defaultPackageDef
  { packageDefModuleName = commandsModule <> ".AnyContractKey"
  , packageDefTypes = pure RecordDef
      { name = "AnyContractKey"
      , typeParams = []
      , fields =
          [ ("getAnyContractKey", tLedgerValue)
          , ("getAnyContractKeyTemplateTypeRep", tTemplateTypeRep)
          ]
      }
  , packageDefDependencies = [ledgerValue, daInternalAnyPkg]
  }

command :: Package
command = makePackage defaultPackageDef
  { packageDefModuleName = commandsModule <> ".Command"
  , packageDefTypes = pure VariantRecordDef
      { name = "Command"
      , typeParams = []
      , constructorsWithFields =
          [ ("Create", [("argC", tAnyTemplate)])
          , ("Exercise",
              [ ("tplId", tTemplateTypeRep)
              , ("cId", TContractId TUnit)
              , ("argE", tAnyChoice)
              ])
          , ("ExerciseByKey",
              [ ("tplId", tTemplateTypeRep)
              , ("keyE", depTy anyContractKey "AnyContractKey" [])
              , ("argE", tAnyChoice)
              ])
          , ("CreateAndExercise",
              [ ("tplArgCE", tAnyTemplate)
              , ("choiceArgCE", tAnyChoice)
              ])
          ]
      }
  , packageDefDependencies = [anyContractKey, daInternalAnyPkg]
  }

commandWithMeta :: Package
commandWithMeta = makePackage defaultPackageDef
  { packageDefModuleName = commandsModule <> ".CommandWithMeta"
  , packageDefTypes = pure RecordDef
      { name = "CommandWithMeta"
      , typeParams = []
      , fields =
          [ ("command", depTy command "Command" [])
          , ("additionalData", TGenMap TText tLedgerValue)
          ]
      }
  , packageDefDependencies = [command, ledgerValue]
  }

commandResult :: Package
commandResult = makePackage defaultPackageDef
  { packageDefModuleName = commandsModule <> ".CommandResult"
  , packageDefTypes = pure VariantDef
      { name = "CommandResult"
      , typeParams = []
      , constructors =
          [ ("CreateResult", TContractId TUnit)
          , ("ExerciseResult", tLedgerValue)
          ]
      }
  , packageDefDependencies = [ledgerValue]
  }

commands :: Package
commands = makePackage defaultPackageDef
  { packageDefModuleName = commandsModule <> ".Commands"
  , packageDefTypes = pure RecordDef
      { name = "Commands"
      , typeParams = [("a", KStar)]
      , fields =
          [ ("commands", TList (depTy commandWithMeta "CommandWithMeta" []))
          , ("continue", TList (depTy commandResult "CommandResult" []) :-> tVar "a")
          ]
      }
  , packageDefDependencies = [commandWithMeta, commandResult]
  }

disclosure :: Package
disclosure = makePackage defaultPackageDef
  { packageDefModuleName = commandsModule <> ".Disclosure"
  , packageDefTypes = pure RecordDef
      { name = "Disclosure"
      , typeParams = []
      , fields =
          [ ("templateId", tTemplateTypeRep)
          , ("contractId", TContractId TUnit)
          , ("blob", TText)
          ]
      }
  , packageDefDependencies = [daInternalAnyPkg]
  }

secp256k1KeyPair :: Package
secp256k1KeyPair = makePackage defaultPackageDef
  { packageDefModuleName = cryptoTextModule <> ".Secp256k1KeyPair"
  , packageDefTypes = pure RecordDef
      { name = "Secp256k1KeyPair"
      , typeParams = []
      , fields =
          [ ("privateKey", TText)
          , ("publicKey", TText)
          ]
      }
  }

partyDetails :: Package
partyDetails = makePackage defaultPackageDef
  { packageDefModuleName = partyManagementModule <> ".PartyDetails"
  , packageDefTypes = pure RecordDef
      { name = "PartyDetails"
      , typeParams = []
      , fields =
          [ ("party", TParty)
          , ("isLocal", TBool)
          ]
      }
  }

partyIdHint :: Package
partyIdHint = makePackage defaultPackageDef
  { packageDefModuleName = partyManagementModule <> ".PartyIdHint"
  , packageDefTypes = pure NewTypeDef
      { name = "PartyIdHint"
      , typeParams = []
      , mUnwrap = Just "partyIdHint"
      , typ = TText
      }
  }

participantName :: Package
participantName = makePackage defaultPackageDef
  { packageDefModuleName = partyManagementModule <> ".ParticipantName"
  , packageDefTypes = pure NewTypeDef
      { name = "ParticipantName"
      , typeParams = []
      , mUnwrap = Just "participantName"
      , typ = TText
      }
  }

packageId :: Package
packageId = makePackage defaultPackageDef
  { packageDefModuleName = submitModule <> ".PackageId"
  , packageDefTypes = pure NewTypeDef
      { name = "PackageId"
      , typeParams = []
      , mUnwrap = Nothing
      , typ = TText
      }
  }

-- A single constructor with an unlabelled field becomes a variant, not a record
submitOptions :: Package
submitOptions = makePackage defaultPackageDef
  { packageDefModuleName = submitModule <> ".SubmitOptions"
  , packageDefTypes = pure VariantDef
      { name = "SubmitOptions"
      , typeParams = []
      , constructors = [("SubmitOptions", TGenMap TText tLedgerValue)]
      }
  , packageDefDependencies = [ledgerValue]
  }

taggedRecord :: Package
taggedRecord = makePackage defaultPackageDef
  { packageDefModuleName = submitErrorModule <> ".TaggedRecord"
  , packageDefTypes = pure RecordDef
      { name = "TaggedRecord"
      , typeParams = []
      , fields =
          [ ("tgTag", TText)
          , ("tgData", TGenMap TText tLedgerValue)
          ]
      }
  , packageDefDependencies = [ledgerValue]
  }

taggedRecordNewtype :: T.Text -> Package
taggedRecordNewtype tyName = makePackage defaultPackageDef
  { packageDefModuleName = submitErrorModule <> "." <> tyName
  , packageDefTypes = pure NewTypeDef
      { name = tyName
      , typeParams = []
      , mUnwrap = Nothing
      , typ = depTy taggedRecord "TaggedRecord" []
      }
  , packageDefDependencies = [taggedRecord]
  }

anySubmitError, anyUpgradeErrorType, anyCryptoErrorType, anyExternalCallErrorType, anyDevErrorType :: Package
anySubmitError = taggedRecordNewtype "AnySubmitError"
anyUpgradeErrorType = taggedRecordNewtype "AnyUpgradeErrorType"
anyCryptoErrorType = taggedRecordNewtype "AnyCryptoErrorType"
anyExternalCallErrorType = taggedRecordNewtype "AnyExternalCallErrorType"
anyDevErrorType = taggedRecordNewtype "AnyDevErrorType"

commandName :: Package
commandName = makePackage defaultPackageDef
  { packageDefModuleName = testingModule <> ".CommandName"
  , packageDefTypes = pure NewTypeDef
      { name = "CommandName"
      , typeParams = []
      , mUnwrap = Just "getCommandName"
      , typ = TText
      }
  }

errorClassName :: Package
errorClassName = makePackage defaultPackageDef
  { packageDefModuleName = testingModule <> ".ErrorClassName"
  , packageDefTypes = pure NewTypeDef
      { name = "ErrorClassName"
      , typeParams = []
      , mUnwrap = Just "getErrorClassName"
      , typ = TText
      }
  }

errorMessage :: Package
errorMessage = makePackage defaultPackageDef
  { packageDefModuleName = testingModule <> ".ErrorMessage"
  , packageDefTypes = pure NewTypeDef
      { name = "ErrorMessage"
      , typeParams = []
      , mUnwrap = Just "getErrorMessage"
      , typ = TText
      }
  }

failedCmd :: Package
failedCmd = makePackage defaultPackageDef
  { packageDefModuleName = testingModule <> ".FailedCmd"
  , packageDefTypes = pure RecordDef
      { name = "FailedCmd"
      , typeParams = []
      , fields =
          [ ("commandName", depTy commandName "CommandName" [])
          , ("errorClassName", depTy errorClassName "ErrorClassName" [])
          , ("errorMessage", depTy errorMessage "ErrorMessage" [])
          ]
      }
  , packageDefDependencies = [commandName, errorClassName, errorMessage]
  }

created :: Package
created = makePackage defaultPackageDef
  { packageDefModuleName = transactionTreeModule <> ".Created"
  , packageDefTypes = pure RecordDef
      { name = "Created"
      , typeParams = []
      , fields =
          [ ("contractId", depTy anyContractId "AnyContractId" [])
          , ("argument", tAnyTemplate)
          ]
      }
  , packageDefDependencies = [anyContractId, daInternalAnyPkg]
  }

-- TreeEvent and Exercised are mutually recursive, so they must share a package
treeEventAndExercised :: Package
treeEventAndExercised = makePackage defaultPackageDef
  { packageDefModuleName = treeEventTypeModule
  , packageDefTypes =
      [ VariantDef
          { name = "TreeEvent"
          , typeParams = []
          , constructors =
              [ ("CreatedEvent", depTy created "Created" [])
              , ("ExercisedEvent", selfTy treeEventTypeModule "Exercised" [])
              ]
          }
      , RecordDef
          { name = "Exercised"
          , typeParams = []
          , fields =
              [ ("contractId", depTy anyContractId "AnyContractId" [])
              , ("choice", TText)
              , ("argument", tAnyChoice)
              , ("childEvents", TList (selfTy treeEventTypeModule "TreeEvent" []))
              ]
          }
      ]
  , packageDefDependencies = [created, anyContractId, daInternalAnyPkg]
  }

transactionTree :: Package
transactionTree = makePackage defaultPackageDef
  { packageDefModuleName = transactionTreeModule <> ".TransactionTree"
  , packageDefTypes = pure RecordDef
      { name = "TransactionTree"
      , typeParams = []
      , fields = [("rootEvents", TList (depTy treeEventAndExercised "TreeEvent" []))]
      }
  , packageDefDependencies = [treeEventAndExercised]
  }

createdIndexPayload :: Package
createdIndexPayload = makePackage defaultPackageDef
  { packageDefModuleName = transactionTreeModule <> ".CreatedIndexPayload"
  , packageDefTypes = pure RecordDef
      { name = "CreatedIndexPayload"
      , typeParams = [("t", KStar)]
      , fields =
          [ ("templateId", tTemplateTypeRep)
          , ("offset", TInt64)
          ]
      }
  , packageDefDependencies = [daInternalAnyPkg]
  }

-- TreeIndex and ExercisedIndexPayload are mutually recursive, so they must share a package
treeIndexAndExercisedIndexPayload :: Package
treeIndexAndExercisedIndexPayload = makePackage defaultPackageDef
  { packageDefModuleName = treeIndexTypeModule
  , packageDefTypes =
      [ VariantDef
          { name = "TreeIndex"
          , typeParams = [("t", KStar)]
          , constructors =
              [ ("CreatedIndex", depTy createdIndexPayload "CreatedIndexPayload" [tVar "t"])
              , ("ExercisedIndex", selfTy treeIndexTypeModule "ExercisedIndexPayload" [tVar "t"])
              ]
          }
      , RecordDef
          { name = "ExercisedIndexPayload"
          , typeParams = [("t", KStar)]
          , fields =
              [ ("templateId", tTemplateTypeRep)
              , ("choice", TText)
              , ("offset", TInt64)
              , ("child", selfTy treeIndexTypeModule "TreeIndex" [tVar "t"])
              ]
          }
      ]
  , packageDefDependencies = [createdIndexPayload, daInternalAnyPkg]
  }

userId :: Package
userId = makePackage defaultPackageDef
  { packageDefModuleName = userManagementModule <> ".UserId"
  , packageDefTypes = pure NewTypeDef
      { name = "UserId"
      , typeParams = []
      , mUnwrap = Nothing
      , typ = TText
      }
  }

user :: Package
user = makePackage defaultPackageDef
  { packageDefModuleName = userManagementModule <> ".User"
  , packageDefTypes = pure RecordDef
      { name = "User"
      , typeParams = []
      , fields =
          [ ("userId", depTy userId "UserId" [])
          , ("primaryParty", TOptional TParty)
          ]
      }
  , packageDefDependencies = [userId]
  }

userRight :: Package
userRight = makePackage defaultPackageDef
  { packageDefModuleName = userManagementModule <> ".UserRight"
  , packageDefTypes = pure VariantDef
      { name = "UserRight"
      , typeParams = []
      , constructors =
          [ ("ParticipantAdmin", TUnit)
          , ("CanActAs", TParty)
          , ("CanReadAs", TParty)
          , ("CanReadAsAnyParty", TUnit)
          , ("CanExecuteAs", TParty)
          , ("CanExecuteAsAnyParty", TUnit)
          , ("CanActAsAnyParty", TUnit)
          ]
      }
  }

invalidUserId :: Package
invalidUserId = makePackage defaultPackageDef
  { packageDefModuleName = userManagementModule <> ".InvalidUserId"
  , packageDefTypes = pure RecordDef
      { name = "InvalidUserId"
      , typeParams = []
      , fields = [("m", TText)]
      }
  }

userAlreadyExists :: Package
userAlreadyExists = makePackage defaultPackageDef
  { packageDefModuleName = userManagementModule <> ".UserAlreadyExists"
  , packageDefTypes = pure RecordDef
      { name = "UserAlreadyExists"
      , typeParams = []
      , fields = [("userId", depTy userId "UserId" [])]
      }
  , packageDefDependencies = [userId]
  }

userNotFound :: Package
userNotFound = makePackage defaultPackageDef
  { packageDefModuleName = userManagementModule <> ".UserNotFound"
  , packageDefTypes = pure RecordDef
      { name = "UserNotFound"
      , typeParams = []
      , fields = [("userId", depTy userId "UserId" [])]
      }
  , packageDefDependencies = [userId]
  }
