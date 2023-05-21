import _ from 'lodash'

import {Dao} from './dao.js'
import {InternalError} from './errors.js'
import {importDirectory} from './import-dir.js'
import {ConcreteEntitySchema, EntitySchema, getSchema, makeSchemaConcrete} from './schemas.js'

type HttpMethod = 'get' | 'post' | 'put' | 'delete'

type EntityTypeName = string
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

interface User {
  _id: string,
  initials: string
}

interface SaveDaoCallbackOptions {
  draftBatchId?: string
}

interface BeforeSaveRestCallbackOptions {
  user?: User,
  draftBatchId?: string,
  parentCollections: Collection[],
  parentDaos: Dao[],
  parentIds: string[],
  dao: Dao
}

interface AfterSaveRestCallbackOptions {
  user?: User,
  draftBatchId?: string,
  parentCollections: Collection[],
  parentDaos: Dao[],
  parentIds: string[]
}

interface BeforeListItemsRestCallbackOptions {
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

interface BeforeValidateRestCallbackOptions {
  user?: User,
  draftBatchId?: string,
  parentCollections: Collection[],
  parentDaos: Dao[],
  parentIds: string[],
  dao: Dao
}

interface DbCallbacks {
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

interface RestCallbacks {
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

const entityTypes: {[name: string]: EntityType} = {}

export async function registerEntityTypes(dirPath: string) {
  await importDirectory(dirPath)
}

export function getEntityType(name: string): EntityType {
  if (!entityTypes[name]) {
    throw new InternalError(`Entity type "${name}' is unknown.`)
  }
  return entityTypes[name]
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

function mergeCallbacks<T extends DbCallbacks | RestCallbacks>(...callbackSources: T[]): T {
  const callbacks: {[callbackName: string]: any[]} = {}
  for (const source of callbackSources) {
    _.forEach(source, (callbacksOfType, key) => {
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
}

function makeObjectProxy<T extends object>(load: () => T | undefined): T {
  const target: ObjectProxyTarget<T> = {value: undefined}
  return new Proxy(target, {
    get: (target, prop) => {
      if (!target.value) {
        target.value = load()
      }
      if (target.value) {
        return target.value[prop as keyof T]
      }
      return undefined
    }
  }) as T
}

export async function makeEntityType(definition: EntityTypeDefinition): Promise<EntityType> {
  const parentEntityType: EntityType | undefined = definition.parent ? makeParentProxy(definition.parent) : undefined
  // import(`../model/entity-types/${options.parent}`) :
  const x = parentEntityType?.dbCallbacks || {}
  const entityType: EntityType = _.merge({}, parentEntityType || {}, definition, {
    parent: parentEntityType,
    abstract: definition.abstract || false,
    dbCallbacks: mergeCallbacks(
      parentEntityType?.dbCallbacks || {},
      //(parentEntityType || {}).dbCallbacks || {},
      definition.dbCallbacks || {}
    ),
    restCallbacks: mergeCallbacks(
      parentEntityType?.restCallbacks || {},
      definition.restCallbacks || {}
    )
  })
  const schema = getSchema(`${definition.schema.name}.${definition.schema.currentVersion}`, 'model')
  if (!schema) {
    throw new InternalError(`Entity type "${entityType.name} has no schema.`)
  }
  const concreteSchema = makeSchemaConcrete(schema, 'model')
  // TODO Add support for namespaced entity schemas.
  entityType.schema = makeSchemaConcrete(schema, 'model')
  entityTypes[entityType.name] = entityType
  return entityType
}
