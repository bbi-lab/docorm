import _ from 'lodash'

import {Dao} from './dao.js'
import {InternalError} from './errors.js'
import {importDirectory} from './import-dir.js'
import {QueryClause, QueryOrder} from './queries.js'
import {ConcreteEntitySchema, EntitySchema, getSchema, makeSchemaConcrete} from './schemas.js'

export type HttpMethod = 'get' | 'post' | 'put' | 'delete'

export type Id = string | number

export type EntityTypeName = string
export type Entity = {
  _id: Id
  [key: string]: any
}

export interface Collection {
  name: string,
  subpath: string,
  entityType: EntityTypeName,
  persistence: 'id-list' | 'subdocument'
}

export interface User {
  _id: string,
  initials: string
}

export interface SaveDaoCallbackOptions {
  draftBatchId?: string
}

export interface BeforeSaveRestCallbackOptions {
  user?: User,
  draftBatchId?: string,
  parentCollections: Collection[],
  parentDaos: Dao[],
  parentIds: string[],
  dao: Dao
}

export interface AfterSaveRestCallbackOptions {
  user?: User,
  draftBatchId?: string,
  parentCollections: Collection[],
  parentDaos: Dao[],
  parentIds: string[]
}

export interface BeforeListItemsRestCallbackOptions {
  user?: User,
  draftBatchId?: string,
  parentCollections: Collection[],
  parentDaos: Dao[],
  parentIds: string[],
  query?: QueryClause,
  order?: QueryOrder,
  offset?: number,
  limit?: number
}

export interface BeforeValidateRestCallbackOptions {
  user?: User,
  draftBatchId?: string,
  parentCollections: Collection[],
  parentDaos: Dao[],
  parentIds: string[],
  dao: Dao
}

export interface DbCallbacks {
  beforeInsert?: ((item: Entity, options: SaveDaoCallbackOptions) => void)[]
  afterInsert?: ((item: Entity, options: SaveDaoCallbackOptions) => void)[]
  beforeUpdate?: ((oldItem: Entity, newItem: Entity, options: SaveDaoCallbackOptions) => void)[]
  beforeUpdateWithoutOriginal?: ((newItem: Entity, options: SaveDaoCallbackOptions) => void)[]
  afterUpdateWithoutOriginal?: ((newItem: Entity, options: SaveDaoCallbackOptions) => void)[]
  afterUpdate?: ((oldItem: Entity, newItem: Entity, options: SaveDaoCallbackOptions) => void)[]
  beforeDelete?: ((id: Id) => void)[]
  afterDelete?: ((id: Id) => void)[]
  beforeEmbedRelatedItemCopy?: ((item: Entity, relatedItemPath: string, relatedItemEntityType: EntityType, relatedItem: Entity) => Entity)[]
}

export interface RestCallbacks {
  beforeCreate?: ((item: Entity, options: BeforeSaveRestCallbackOptions) => void)[]
  afterCreate?: ((item: Entity, options: AfterSaveRestCallbackOptions) => void)[]
  beforeListItems?: ((options: BeforeListItemsRestCallbackOptions) => void)[]
  beforeUpdate?: ((item: Entity, options: BeforeSaveRestCallbackOptions) => Entity)[]
  afterUpdate?: ((item: Entity, options: AfterSaveRestCallbackOptions) => void)[]
  beforeValidateCreate?: ((item: Entity, options: BeforeValidateRestCallbackOptions) => void)[]
  beforeValidateUpdate?: ((item: Entity, options: BeforeValidateRestCallbackOptions) => void)[]
}

export interface EntityTypeDefinition {
  parent?: EntityTypeName
  name: EntityTypeName
  abstract?: boolean
  restCollection: string
  commonTitle: string
  title: string
  allowsDrafts?: boolean
  schema: {
    name: string
    currentVersion: string
  }
  table?: string
  import?: {
    propertyMappings?: []
  }
  dbCallbacks?: DbCallbacks
  history?: {
    trackChange?: boolean | ((oldItem: Entity, newItem: Entity) => boolean | Promise<boolean>)
  }
  derivedProperties?: {
    [propertyName: string]: (item: Entity) => any
  },
  restCallbacks?: RestCallbacks
  extraCollectionActions: {
    [actionName: string]: {
      method: HttpMethod
      // TODO Specify types based on Express.js documentation.
      handler: (req: any, res: any, next: any, dao: Dao) => void | Promise<void>
    }
  }
  extraInstanceActions: {
    [actionName: string]: {
      method: HttpMethod
      // TODO Specify types based on Express.js documentation.
      handler: (req: any, res: any, next: any, item: Entity, dao: Dao) => void | Promise<void>
    }
  }
}

export interface EntityType extends Omit<EntityTypeDefinition, 'parent' | 'abstract' | 'schema'> {
  parent?: EntityType
  abstract: boolean
  schema: ConcreteEntitySchema
}

// TODO Extract this from the actual Typescript interface somehow.
const ENTITY_TYPE_KEYS = [
  'parent',
  'name',
  'abstract',
  'restCollection',
  'commonTitle',
  'title',
  'allowsDrafts',
  'schema',
  'table',
  'import',
  'dbCallbacks',
  'history',
  'derivedProperties',
  'restCallbacks',
  'extraCollectionActions',
  'extraInstanceActions'
]

const entityTypes: {[name: string]: EntityType} = {}

export async function registerEntityTypes(dirPath: string) {
  await importDirectory(dirPath)
}

export function getEntityType(name: string, {required = true} = {}): EntityType {
  if (required && !entityTypes[name]) {
    throw new InternalError(`Entity type "${name}' is unknown.`)
  }
  return entityTypes[name]
}

export async function getEntityTypes() {
  return entityTypes
}

/*
let loadedAllEntityTypes = false
export async function getEntityTypes() {
  if (!loadedAllEntityTypes) {
    for (const entityTypeName of _.keys(unimportedEntityTypePaths)) {
      await getEntityType(entityTypeName)
    }
    loadedAllEntityTypes = true
  }
  return entityTypes
}

export async function getEntityType(entityTypeName) {
  let entityType = entityTypes[entityTypeName]
  if (!entityType && unimportedEntityTypePaths[entityTypeName]) {
    // console.log(`Importing file ${unimportedEntityTypePaths[entityTypeName]}`)

    // Using file:// makes this work with Windows paths that begin with drive letters.
    const {default: newlyLoadedEntityType} = await import(`file://${unimportedEntityTypePaths[entityTypeName]}`)
    delete unimportedEntityTypePaths[entityTypeName]
    entityTypes[entityTypeName] = newlyLoadedEntityType
    entityType = newlyLoadedEntityType
  }
  return entityType
}
*/

function mergeCallbacks<T = DbCallbacks | RestCallbacks>(...callbackSources: T[]): T {
  const callbacks: {[callbackName: string]: any[]} = {}
  for (const source of callbackSources) {
    // TODO Eliminate "as object."
    _.forEach(source as object, (callbacksOfType, key) => {
      if (!callbacks[key]) {
        callbacks[key] = []
      }
      callbacks[key].push(...callbacksOfType as any[])
    })
  }
  // TODO This is a crude way to return a result of the same type as the parameters. We really want to accept either
  // DbCallbacks or RestCallbacks and return the same type.
  return callbacks as unknown as T
}

function makeParentProxy(parentName: string): EntityType {
  if (entityTypes[parentName]) {
    return entityTypes[parentName]
  } else {
    return makeObjectProxy(() => getEntityType(parentName))
  }
}

interface ObjectProxyTarget<T> {
  value?: T
  loaded: boolean
}

function makeObjectProxy<T extends object>(load: () => T | undefined): T {
  const target: ObjectProxyTarget<T> = {value: undefined, loaded: false}
  return new Proxy(target, {
    has: (target, property) => {
      return [...ENTITY_TYPE_KEYS, '_isProxy', '_loaded'].includes(property.toString())
    },
    get: (target, property) => {
      if (property == 'then') {
        return undefined
      }
      if (property == '_isProxy') {
        return true
      }
      if (property == '_loaded') {
        return target.loaded
      }
      if (!target.loaded) {
        target.value = load()
        target.loaded = true
      }
      if (target.value) {
        return target.value[property as keyof T]
      }
      return undefined
    }
  }) as T
}

function makeMergedCallbacksProxy<T = DbCallbacks | RestCallbacks>(callbacks: T, parentEntityType: EntityType | undefined, callbacksProperty: string): T {
  const target: ObjectProxyTarget<T> = {value: undefined, loaded: false}
  return new Proxy(target, {
    has: (target, property) => {
      return [...ENTITY_TYPE_KEYS, '_isProxy', '_loaded'].includes(property.toString())
    },
    get: (target, property, receiver) => {
      if (property == 'then') {
        return undefined
      }
      if (property == '_isProxy') {
        return true
      }
      if (property == '_loaded') {
        return target.loaded
      }
      if (!target.loaded) {
        target.value = mergeCallbacks(
          (parentEntityType as any)?.[callbacksProperty] || {} as T,
          callbacks || {} as T
        )  
        target.loaded = true
      }
      if (target.value) {
        return target.value[property as keyof T]
      }
      return undefined
    }
  }) as T
}

export function makeUnproxiedEntityType(definition: EntityTypeDefinition): EntityType {
  const parentEntityType: EntityType | undefined = definition.parent ? getEntityType(definition.parent) : undefined
  const entityType: EntityType = _.merge({}, parentEntityType || {}, definition, {
    parent: parentEntityType,
    abstract: definition.abstract || false,
    dbCallbacks: mergeCallbacks(
      parentEntityType?.dbCallbacks || {},
      definition.dbCallbacks || {}
    ),
    restCallbacks: mergeCallbacks(
      parentEntityType?.restCallbacks || {},
      definition.restCallbacks || {}
    )
  })
  /*
  entityType.parent = parentEntityType
  entityType.dbCallbacks = makeMergedCallbacksProxy(definition.dbCallbacks, parentEntityType, 'dbCallbacks')
  entityType.restCallbacks = makeMergedCallbacksProxy(definition.restCallbacks, parentEntityType, 'restCallbacks')
  */
  const schema = getSchema([definition.schema.name, definition.schema.currentVersion], 'model')
  if (!schema) {
    throw new InternalError(`Entity type "${entityType.name} has no schema.`)
  }
  // TODO Add support for namespaced entity schemas.
  entityType.schema = makeSchemaConcrete(schema, 'model')
  entityTypes[entityType.name] = entityType
  return entityType
}

export function makeEntityType(definition: EntityTypeDefinition): EntityType {
  if (definition.parent) {
    const parentEntityType = getEntityType(definition.parent, {required: false})
    if (!parentEntityType) {
      return makeObjectProxy(() => makeUnproxiedEntityType(definition))
    } else {
      return makeUnproxiedEntityType(definition)
    }
  } else {
    return makeUnproxiedEntityType(definition)
  }
}

export async function calculateDerivedProperties(entityType: EntityType, item: Entity) {
  if (entityType.derivedProperties) {
    for (const derivedPropertyPath of _.keys(entityType.derivedProperties)) {
      _.set(item, derivedPropertyPath, await entityType.derivedProperties[derivedPropertyPath](item))
    }
  }
}
