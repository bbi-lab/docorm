import _ from 'lodash'
import {
  arrayToDottedPath,
  type PropertyPathArray,
  type PropertyPathStr,
  type Schema
} from 'schema-fun'

import {Dao} from './dao.js'
import {docorm} from './index.js'
import {InternalError} from './errors.js'
import {importDirectory} from './import-dir.js'
import {QueryClause, QueryOrder} from './queries.js'
//import {ConcreteEntitySchema, EntitySchema, SchemaType, getSchema, makeSchemaConcrete} from './schemas.js'

type SyncOrAsync<T extends (...args: any) => any> =
    ((...args: Parameters<T>) => ReturnType<T>) | ((...args: Parameters<T>) => Promise<ReturnType<T>>)
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
  // TODO Replace inverse-ref and ref with 'relationship,' and use the schema to determine how the relationship is stored.
  persistence: 'inverse-ref' | 'ref' | 'subdocument',
  // TODO Use the foreign key path from the schema.
  foreignKeyPath?: string
}

export interface User {
  _id: string,
  initials: string
}

export interface DeleteDaoCallbackOptions {
  dao: Dao
}

export interface SaveDaoCallbackOptions {
  dao: Dao,
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
  beforeInsert?: (SyncOrAsync<(item: Entity, options: SaveDaoCallbackOptions) => void>)[]
  afterInsert?: (SyncOrAsync<(item: Entity, options: SaveDaoCallbackOptions) => void>)[]
  beforeUpdate?: (SyncOrAsync<(oldItem: Entity, newItem: Entity, options: SaveDaoCallbackOptions) => void>)[]
  beforeUpdateWithoutOriginal?: (SyncOrAsync<(newItem: Entity, options: SaveDaoCallbackOptions) => void>)[]
  afterUpdateWithoutOriginal?: (SyncOrAsync<(newItem: Entity, options: SaveDaoCallbackOptions) => void>)[]
  afterUpdate?: (SyncOrAsync<(oldItem: Entity, newItem: Entity, options: SaveDaoCallbackOptions) => void>)[]
  beforeDelete?: (SyncOrAsync<(id: Id, options: DeleteDaoCallbackOptions) => void>)[]
  afterDelete?: (SyncOrAsync<(id: Id, options: DeleteDaoCallbackOptions) => void>)[]
  beforeEmbedRelatedItemCopy?: (SyncOrAsync<(item: Entity, relatedItemPath: string, relatedItemEntityType: EntityType, relatedItem: Entity) => Entity>)[]
}

export interface RestCallbacks {
  beforeCreate?: (SyncOrAsync<(item: Entity, options: BeforeSaveRestCallbackOptions) => void>)[]
  afterCreate?: (SyncOrAsync<(item: Entity, options: AfterSaveRestCallbackOptions) => void>)[]
  beforeListItems?: (SyncOrAsync<(options: BeforeListItemsRestCallbackOptions) => void>)[]
  beforeUpdate?: (SyncOrAsync<(item: Entity, options: BeforeSaveRestCallbackOptions) => Entity>)[]
  afterUpdate?: (SyncOrAsync<(item: Entity, options: AfterSaveRestCallbackOptions) => void>)[]
  beforeValidateCreate?: (SyncOrAsync<(item: Entity, options: BeforeValidateRestCallbackOptions) => void>)[]
  beforeValidateUpdate?: (SyncOrAsync<(item: Entity, options: BeforeValidateRestCallbackOptions) => void>)[]
}

export interface EntityTypeDefinition {
  parent?: EntityTypeName
  name: EntityTypeName
  abstract?: boolean
  restCollection: string
  commonTitle: string
  title: string
  allowsDrafts?: boolean
  schemaId: string,
  table?: string // TODO Move into mapping
  mapping?: {
    idColumn?: string
    jsonColumn?: string
    readonly?: boolean
  }
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

export interface PropertyMapping {
  column: string
  propertyPath: PropertyPathStr
}

export interface EntityTypeMapping {
  table: string
  idColumn: string
  jsonColumn?: string
  propertyMappings: PropertyMapping[]
  readonly: boolean
}

export interface EntityType extends Omit<EntityTypeDefinition, 'abstract' | 'parent'> {
  parent?: EntityType
  abstract: boolean
  schema: Schema
  // concreteSchema: ConcreteEntitySchema
  mapping?: EntityTypeMapping
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
  'schemaName',
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
    throw new InternalError(`Entity type "${name}" is unknown.`)
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
    },
    ownKeys: (target) => target.value ? Object.keys(target.value) : [],
    getOwnPropertyDescriptor: (target, property) => {
      if (property == '_isProxy') {
        return {enumerable: false, value: true}
      }
      if (property == '_loaded') {
        return {enumerable: false, value: target.loaded}
      }
      /*
      if (!target.loaded) {
        target.value = load()
        target.loaded = true
      }
      */
      if (target.value) {
        return Object.getOwnPropertyDescriptor(target.value, property)
      }
      return undefined
    }
  }) as T
}

/*
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
*/

export interface BuildColumnMapState {
  knownSubschemas: {
    [schemaId: string]: Schema
  }
}

const INITIAL_BUILD_COLUMN_MAP_STATE = {
  knownSubschemas: {}
}

function buildPropertyMappings(
    schema: Schema,
    // schemaType: SchemaType = 'model',
    // namespace: string | undefined = undefined,
    currentPath: PropertyPathArray = [],
    state: BuildColumnMapState = INITIAL_BUILD_COLUMN_MAP_STATE
): PropertyMapping[] {
  let mappings: PropertyMapping[] = []
  const {knownSubschemas} = state

  const column: string | undefined = (schema as any)?.mapping?.column

  if (schema.allOf || schema.oneOf) {
    const schemaOptions = (schema.allOf || schema.oneOf) as Schema[]
    const schemaOptionMaps = schemaOptions.map((schemaOption: Schema) =>
      buildPropertyMappings(schemaOption, currentPath, state)
      // buildPropertyMappings(schemaOption, schemaType, namespace, currentPath, state)
    )
    mappings = _.uniqBy(schemaOptionMaps.flat(), 'propertyPath')
  } else if (schema.$ref) {
    if (!['ref', 'inverse-ref'].includes((schema as any).storage)) {
      const schemaRef = schema.$ref //_.trimStart(schema.$ref, '/').split('/')
      const cachedSchemaKey = (schema as any).$ref
      let subschema = {}
      if (state.knownSubschemas[cachedSchemaKey] != null) {
        subschema = knownSubschemas[cachedSchemaKey]
      } else {
        // TODO Handle error if not found
        const subschema = docorm.config.schemaRegistry?.getSchema(schemaRef)
        if (!subschema) {
          throw new InternalError(`Schema refers to unknown subschema with $ref "${schemaRef}".`)
        }
        knownSubschemas[cachedSchemaKey] = subschema
      }
      mappings = buildPropertyMappings(subschema, currentPath)
    } else if ((schema as any).storage == 'ref') {
      if (column) {
        mappings.push({propertyPath: arrayToDottedPath([...currentPath, '$ref']), column})
      }
    }
  } else {
    switch ((schema as any).type) {
      case 'object':
        {
          const properties: {[propertyName: string]: Schema} | undefined = schema.properties
          if (properties) {
            for (const propertyName in properties) {
              const partialMappings = buildPropertyMappings(properties[propertyName], [...currentPath, propertyName])
              mappings.push(...partialMappings)
            }
          }
        }
        break
      case 'array':
        // Stop here. We cannot map array elements or their properties to columns.
        break
      default:
        if (column) {
          mappings.push({propertyPath: arrayToDottedPath(currentPath), column})
        }
    }
  }
  return mappings
}

export function makeUnproxiedEntityType(definition: EntityTypeDefinition): EntityType {
  const parentEntityType: EntityType | undefined = definition.parent ? getEntityType(definition.parent) : undefined
  const schema = docorm.config.schemaRegistry?.getSchema(definition.schemaId)
  if (!schema) {
    throw new InternalError(`Entity type "${definition.name}" has no schema.`)
  }

  // TODO Move definition.table to definition.mapping.table.
  const table = definition.table || parentEntityType?.mapping?.table
  const entityType: EntityType = _.merge({}, parentEntityType || {}, definition, {
    parent: parentEntityType,
    abstract: definition.abstract || false,
    schema,
    // concreteSchema: makeSchemaConcrete(schema, 'model'),
    mapping: undefined,
    dbCallbacks: mergeCallbacks(
      parentEntityType?.dbCallbacks || {},
      definition.dbCallbacks || {}
    ),
    restCallbacks: mergeCallbacks(
      parentEntityType?.restCallbacks || {},
      definition.restCallbacks || {}
    )
  })
  // Don't merge parent mappings into this entity type's mapping.
  entityType.mapping = table ? {
    table,
    idColumn: definition.mapping?.idColumn || 'id',
    jsonColumn: definition.mapping ? definition.mapping.jsonColumn : 'data',
    propertyMappings: buildPropertyMappings(schema),
    readonly: !!definition.mapping?.readonly
  } : undefined
  return entityType
}

export function makeEntityType(definition: EntityTypeDefinition): EntityType {
  let entityType: EntityType | undefined = undefined
  if (definition.parent) {
    const parentEntityType = getEntityType(definition.parent, {required: false})
    if (!parentEntityType) {
      entityType = makeObjectProxy(() => makeUnproxiedEntityType(definition))
    } else {
      entityType = makeUnproxiedEntityType(definition)
    }
  } else {
    entityType = makeUnproxiedEntityType(definition)
  }
  entityTypes[definition.name] = entityType
  return entityType
}

export async function calculateDerivedProperties(entityType: EntityType, item: Entity) {
  if (entityType.derivedProperties) {
    for (const derivedPropertyPath of _.keys(entityType.derivedProperties)) {
      _.set(item, derivedPropertyPath, await entityType.derivedProperties[derivedPropertyPath](item))
    }
  }
}
