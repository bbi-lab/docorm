/**
 * Data Access Objects for PostgreSQL
 *
 * @module lib/db/postgresql/dao
 */

import jsonPointer from 'json-pointer'
import {JSONPath as jsonPath} from 'jsonpath-plus'
import _ from 'lodash'
import {
  type JsonPathStr,
  jsonPathToPropertyPath,
  type JsonPointerStr,
  mapPaths,
  pathDepth,
  type PathTransformer,
  type PropertyPathStr,
  type Relationship,
  type Schema,
  shortenPath,
  tailPath
} from 'schema-fun'
import {Readable, Transform} from 'stream'
import {v4 as uuidv4} from 'uuid'

import {docorm} from './index.js'
import {Collection, Entity, EntityType, getEntityType, Id} from './entity-types.js'
import {PersistenceError} from './errors.js'
import {Client} from './postgresql/db.js'
import {
  applyQuery,
  QueryClause,
  queryClauseIsAnd,
  QueryOrder
} from './queries.js'
import makeRawDao, {FetchResults, fetchResultsIsArray, fetchResultsIsStream} from './postgresql/raw-dao.js'
/*
import {
  ConcreteEntitySchema,
  findPropertyInSchema,
  findRelationships,
  listTransientPropertiesOfSchema,
  Relationship
} from './schemas.js'
*/

export type Dao = any

/**
 * Return a function that transforms property paths.
 *
 * The function returns a value of type PathTransformer containing the transformed path together with additional
 * options.
 *
 * - If isDraft is true, the function will prepend `draft.` to all property paths other than `_id`.
 * - If the property type is boolean (as determined by examining the schema to which the property path refers), an
 *   option is added with key `sqlType` and value `boolean`.
 *
 * @param schema The schema in which to look for the property path, to determine if the property is boolean.
 * @param isDraft A flag indicating whether the path is to be used in the context of a draft (true) or a regular
 *   document (false).
 * @returns A value containing the transformed path together with additional options (currently, just `sqlType:
 *   'boolean'`).
 */
const makePathTransformer = (schema: Schema, isDraft = false) =>
  (path: PropertyPathStr) => {
    const result: ReturnType<PathTransformer> = {
      path: (path == '_id' || !isDraft) ? path : `draft.${path}`
    }
    const propertySchema = docorm.config.schemaRegistry?.findPropertyInSchema(schema, path)
    if (propertySchema) {
      const propertySchemaType = (propertySchema as any).type
      switch (propertySchemaType) {
        case 'boolean':
          result.additionalOptions = {sqlType: 'boolean'}
          break
          // TODO Handle other JSON -> SQL type conversions? Only numbers may be needed. (What about dates?)
          /*
          case 'number':
          result.additionalOptions = {sqlType: 'real'}
          break
          */
        default:
          break
      }
    }
    return result
  }

interface DaoOptionsInput {
  /**
   * An array of ancestor collections, with the immediate parent collection last. When managing items that do not
   * belong to a parent collection, this is empty.
   */
  parentCollections?: Collection[],
  /** An array of DAOs for the ancestor objects. This should have the same length as parentCollections. */
  parentDaos?: Dao[],
  /** The draft batch ID, if any. */
  draftBatchId?: string
}

interface DaoOptions {
  /**
   * An array of ancestor collections, with the immediate parent collection last. When managing items that do not
   * belong to a parent collection, this is empty.
   */
  parentCollections: Collection[],
  /** An array of DAOs for the ancestor objects. This should have the same length as parentCollections. */
  parentDaos: Dao[],
  /** The draft batch ID, if any. */
  draftBatchId?: string
}

const DAO_DEFAULT_OPTIONS: DaoOptions = {
  parentCollections: [],
  parentDaos: []
}

interface CountOptions {
  client?: Client
}

interface FetchOptionsInput {
  client?: Client,
  order?: QueryOrder,
  offset?: number,
  limit?: number,
  propertyBlacklist?: string[],
  stream?: boolean
}

interface FetchOptions extends FetchOptionsInput {
  stream: boolean
}

const FETCH_DEFAULT_OPTIONS: FetchOptions = {
  stream: false
}

interface FetchRelationshipsOptionsInput {
  client?: Client
  relationships?: Relationship[]
  entityTypes?: {[entityTypeName: string]: EntityType}
  daos?: {[entityTypeName: string]: Dao}
  knownItems?: {[entityTypeName: string]: {[id: Id]: Entity}}
}

interface FetchRelationshipsOptions extends FetchRelationshipsOptionsInput {
  client?: Client
  entityTypes: {[entityTypeName: string]: EntityType}
  daos: {[entityTypeName: string]: Dao}
  knownItems: {[entityTypeName: string]: {[id: Id]: Entity}}
  pathPrefix?: {[pathPrefix: string]: string}
}

const DEFAULT_FETCH_RELATIONSHIPS_OPTIONS: FetchRelationshipsOptions = {
  entityTypes: {},
  daos: {},
  knownItems: {}
}

/**
 * Create a Data Access Object (DAO).
 *
 * The new DAO's behavior is determined by several parameters:
 * - The entity type defines the storage table, schema, and validation behaviors.
 * - parentCollections is an array of ancestor collections. TODO Document the collection data type. If present, then
 *   this DAO will not read and write items using a database table but will fetch them from a parent item's collection
 *   and, on insert or update, will save the parent item.
 * - If draftBatchId is non-null, then items are written to the drafts table instead of the appropriate item storage
 *   table.
 *
 * @param entityType - The entity type of the items that this DAO will manage.
 * @param options - Options for this DAO.
 * @return {Object} A new DAO.
 */
const makeDao = async function(entityType: EntityType, options: DaoOptionsInput = DAO_DEFAULT_OPTIONS): Promise<Dao> {
  const {parentCollections, parentDaos, draftBatchId} = _.merge({}, DAO_DEFAULT_OPTIONS, options) as DaoOptions

  //const concreteSchema = entityType.concreteSchema
  const schema = entityType.schema

  const transientPropertyPaths = docorm.config.schemaRegistry?.findTransientPropertiesInSchema(schema) || []
  const dbCallbacks = entityType.dbCallbacks || {}

  const draftEntityType = await getEntityType('draft')
  const rawDao = makeRawDao(draftBatchId ? draftEntityType : entityType)

  const mayTrackChanges = entityType.name != 'item-version'
  const itemVersionEntityType = mayTrackChanges ? await getEntityType('item-version') : null
  const itemVersionsDao = itemVersionEntityType ? await makeDao(itemVersionEntityType) : null

  async function recordItemVersion(itemVersion: Entity, client = null) {
    if (itemVersionsDao && itemVersion._id) {
      await itemVersionsDao.insert({item: itemVersion}, [], {client})
    }
  }

  function wrapDraft(item: Entity): Entity {
    return {
      _id: item._id,
      draftBatchId,
      _type: draftEntityType.name,
      draft: _.assign(_.omit(item, '_id'), {_type: entityType.name})
    }
  }

  function unwrapDraft(draft: Entity): Entity {
    return _.merge({}, draft.draft, {_id: draft._id, _type: draft._draftType})
  }

  return {
    entityType: entityType,
    //concreteSchema: concreteSchema,
    draftBatchId,

    /**
     * Prepare an item for saving by removing any transient properties.
     *
     * Transient properties are catalogued at DAO creation by calling
     * {@link module:lib/schemas.listTransientPropertiesOfSchema listTransientPropertiesOfSchema}.
     *
     * @param {Object} item - The item to sanitize.
     * @return {Object} A deep clone of the item, with transient properties recursively removed; or the item itself, if
     *   there are no transient properties.
     */
    sanitizeItem: function(item: Entity) {
      let sanitizedItem = item
      if (transientPropertyPaths.length > 0) {
        sanitizedItem = _.cloneDeep(item)
        for (const path of transientPropertyPaths) {
          _.unset(sanitizedItem, path)
        }
      }
      return sanitizedItem
    },

    count: async function(
        query?: QueryClause,
        parentIds: string[] = [],
        options: CountOptions = {}
    ) {
      const {client} = options
      if (parentCollections.length > 0 && parentDaos.length > 0 && parentIds.length > 0) {
        const items = this.fetch(query, parentIds, {client})
        return items.length
      } else {
        if (query != undefined && query !== false) {
          query = mapPaths(query, makePathTransformer(schema, !!draftBatchId))
        }
        if (query != undefined && query !== false) {
          if (draftBatchId) {
            // query = query || {}
            // query = mapPaths(query, (path) => (path == '_id' ? path : `draft.${path}`))
            // query = _.mapKeys(query, (value, path) => (path == '_id') ? path : `draft.${path}`)
            const draftClauses = [
              {l: {path: 'draft._type'}, r: {constant: entityType.name}},
              {l: {path: 'draftBatchId'}, r: {constant: draftBatchId}}
            ]
            if (queryClauseIsAnd(query)) {
              query = {
                and: [...draftClauses, ...query.and]
              }
            } else {
              query = {and: [...draftClauses, ...(query ? [query] : [])]}
            }
          }
        }

        return await rawDao.count(query, {client})
      }
    },

    fetch: async function(
        query?: QueryClause,
        parentIds: string[] = [],
        options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS
    ) {
      let {client, order, offset, limit, propertyBlacklist, stream} = _.merge({}, FETCH_DEFAULT_OPTIONS, options) as
          FetchOptions

      const collection = _.last(parentCollections)
      if (collection && parentDaos.length > 0 && parentIds.length > 0) {
        let results: Entity[] = []
        switch (collection.persistence) {
          case 'inverse-ref': {
            // TODO Use the schema's foreign key path instead of having one in the REST collection config.
            if (!collection.foreignKeyPath) {
              throw new PersistenceError('Collection lacks a foreign key path')
            }
            // TODO Optimize by fetching only the parent's _id.
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              results = [] // TODO Or error?
            } else {
              const collectionMembers = await rawDao.fetch({
                l: {path: `${collection.foreignKeyPath}.$ref`}, r: {constant: parent._id}
              }) as Entity[]
              results = collectionMembers.filter((x) => query ? applyQuery(x, query) : true)
              // TODO Apply order
              // TODO Apply limit
            }
          }
            break
          case 'ref': {
            // TODO Optimize by fetching only the path we need from the parent. Do the same in other fetch methods.
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              results = [] // TODO Or error?
            } else {
              const collectionMemberIds = _.get(parent, collection.subpath) || []
              const collectionMembers = await rawDao.fetchById(collectionMemberIds, {client, propertyBlacklist})
              results = collectionMembers.filter((x) => query ? applyQuery(x, query) : true)
              // TODO Apply order
              // TODO Apply limit
            }
          }
            break
          case 'subdocument': {
            const parent = await _.last(parentDaos)
                .fetchOneById(_.last(parentIds), parentIds.slice(0, -1), {client, propertyBlacklist})
            if (!parent) {
              results = [] // TODO Or error?
            } else {
              // TODO Implement collection filtering by query
              results = (_.get(parent, collection.subpath) || [])
                  .filter((x: any) => query ? applyQuery(x, query) : true)
              // TODO Apply order
              // TODO Apply limit
            }
          }
            break
        }
        return stream ? Readable.from(results) : results
      } else {
        if (query !== undefined && query !== false) {
          query = mapPaths(query, makePathTransformer(schema, !!draftBatchId))
        }
        if (query !== undefined && query !== false) {
          if (draftBatchId) {
            // query = query || {}
            // query = mapPaths(query, (path) => (path == '_id' ? path : `draft.${path}`))
            // query = _.mapKeys(query, (value, path) => (path == '_id') ? path : `draft.${path}`)
            const draftClauses = [
              {l: {path: 'draft._type'}, r: {constant: entityType.name}},
              {l: {path: 'draftBatchId'}, r: {constant: draftBatchId}}
            ]
            if (queryClauseIsAnd(query)) {
              query = {
                and: [...draftClauses, ...query.and]
              }
            } else {
              query = {and: [...draftClauses, ...(query ? [query] : [])]}
            }
          }
        }

        if (order != null) {
          order = mapPaths(order, makePathTransformer(schema, !!draftBatchId))
          if (draftBatchId) {
            // order = mapPaths(order, (path) => (path == '_id' ? path : `draft.${path}`))
            /* order = _.map(order, orderElement => {
              let path = _.isArray(orderElement) ? orderElement[0] : orderElement
              if (path != '_id') {
                path = `draft.${path}`
              }
              return _.isArray(orderElement) ? [path, ...orderElement.slice(1)] : path
            })*/
          }
        }

        const itemsOrStreamQuery = await rawDao.fetch(query, {client, order, offset, limit, propertyBlacklist, stream})
        if (draftBatchId) {
          if (fetchResultsIsStream(itemsOrStreamQuery)) {
            const unwrapDrafts = new Transform({
              objectMode: true,
              transform: (item, _, callback) => callback(null, unwrapDraft(item))
            })
            return {run: itemsOrStreamQuery.run, stream: itemsOrStreamQuery.stream.pipe(unwrapDrafts)}
            // return itemsOrStreamQuery.pipe(unwrapDrafts)
          } else {
            return itemsOrStreamQuery.map((item) => unwrapDraft(item))
          }
        } else {
          return itemsOrStreamQuery
        }
      }
    },

    // TODO rawDao.fetchWithSql currently does not support the 'stream' option.
    fetchWithSql: async function(
        whereClauseSql: string | null = null,
        whereClauseParameters = [],
        options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS
    ) {
      let {client, order, offset, limit, propertyBlacklist, stream} = _.merge({}, FETCH_DEFAULT_OPTIONS, options) as
          FetchOptions
      return await rawDao.fetchWithSql(
        whereClauseSql,
        whereClauseParameters,
        {client, order, offset, limit, propertyBlacklist, stream}
      )
    },

    fetchAll: async function(
        parentIds: string[] = [],
        options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS
    ) {
      let {client, order, offset, limit, propertyBlacklist, stream} = _.merge({}, FETCH_DEFAULT_OPTIONS, options) as
          FetchOptions
      const collection = _.last(parentCollections)
      if (collection && parentDaos.length > 0 && parentIds.length > 0) {
        switch (collection.persistence) {
          case 'inverse-ref': {
            // TODO Use the schema's foreign key path instead of having one in the REST collection config.
            if (!collection.foreignKeyPath) {
              throw new PersistenceError('Collection lacks a foreign key path')
            }
            // TODO Optimize by fetching only the parent's _id.
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return [] // TODO Or error?
            } else {
              const collectionMembers = await rawDao.fetch({
                l: {path: `${collection.foreignKeyPath}.$ref`}, r: {constant: parent._id}
              }, {client, order, offset, limit, propertyBlacklist}) as Entity[]
              return collectionMembers
              // TODO Apply order
              // TODO Apply limit
            }
          }
          case 'ref': {
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return [] // TODO Or error?
            } else {
              const collectionMemberIds = _.get(parent, collection.subpath) || []
              return await rawDao.fetchById(collectionMemberIds, {
                client,
                limit,
                propertyBlacklist,
                order,
                offset
              })
            }
          }
          case 'subdocument': {
            const parent = await _.last(parentDaos)
                .fetchOneById(_.last(parentIds), parentIds.slice(0, -1), {client, propertyBlacklist})
            if (!parent) {
              return [] // TODO Or error?
            } else {
              const collectionMembers = _.get(parent, collection.name) || []
              // TODO Apply order
              if (offset) {
                return limit ? collectionMembers.slice(offset, offset + limit) : collectionMembers.slice(offset)
              } else {
                return limit ? collectionMembers.slice(0, limit) : collectionMembers
              }
            }
          }
        }
      } else {
        let query: QueryClause | undefined
        if (draftBatchId) {
          query = {
            and: [
              {l: {path: 'draft._type'}, r: {constant: entityType.name}},
              {l: {path: 'draftBatchId'}, r: {constant: draftBatchId}}
            ]
          }
        }

        if (order != null) {
          order = mapPaths(order, makePathTransformer(schema, !!draftBatchId))
          if (draftBatchId) {
            // order = mapPaths(order, (path) => (path == '_id' ? path : `draft.${path}`))
            /* order = _.map(order, orderElement => {
              let path = _.isArray(orderElement) ? orderElement[0] : orderElement
              if (path != '_id') {
                path = `draft.${path}`
              }
              return _.isArray(orderElement) ? [path, ...orderElement.slice(1)] : path
            })*/
          }
        }

        const itemsOrStreamQuery = query ?
            await rawDao.fetch(query, {client, order, offset, limit, propertyBlacklist, stream})
            : await rawDao.fetchAll({client, order, offset, limit, propertyBlacklist, stream})
        if (draftBatchId) {
          if (fetchResultsIsStream(itemsOrStreamQuery)) {
            const unwrapDrafts = new Transform({
              objectMode: true,
              transform: (item, _, callback) => callback(null, unwrapDraft(item))
            })
            return {run: itemsOrStreamQuery.run, stream: itemsOrStreamQuery.stream.pipe(unwrapDrafts)}
            // return itemsOrStreamQuery.pipe(unwrapDrafts)
          } else if (fetchResultsIsArray(itemsOrStreamQuery)) {
            return itemsOrStreamQuery.map((item: Entity) => unwrapDraft(item))
          }
        } else {
          return itemsOrStreamQuery
        }
        /*
        const fetchResult = query ?
            await rawDao.fetch(query, {client, order, offset, limit})
            : await rawDao.fetchAll({client, order, offset, limit})
        return draftBatchId ? fetchResult.map((item) => unwrapDraft(item)) : fetchResult
        */
      }
    },

    fetchByIds: async function(
        ids: Id[],
        parentIds: Id[] = [],
        {client = null, returnMatchingList = true, propertyBlacklist = []} = {}
    ) {
      let items: Entity[] = []
      ids = _.uniq(ids)
      const collection = _.last(parentCollections)
      if (collection && parentDaos.length > 0 && parentIds.length > 0) {
        switch (collection.persistence) {
          case 'inverse-ref': {
            // TODO Use the schema's foreign key path instead of having one in the REST collection config.
            if (!collection.foreignKeyPath) {
              throw new PersistenceError('Collection lacks a foreign key path')
            }
            // TODO Optimize by fetching only the parent's _id.
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              items = [] // TODO Or error?
            } else {
              const query : QueryClause = {
                and: [
                  {l: {path: `${collection.foreignKeyPath}.$ref`}, r: {constant: parent._id}},
                  {l: {path: '_id'}, r: {constant: ids}, operator: 'in'}
                ]
              }
              items = await rawDao.fetch(query, {client, propertyBlacklist}) as Entity[]
            }
          }
            break
          case 'ref': {
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return [] // TODO Or error?
            } else {
              const collectionMemberIds = (_.get(parent, collection.subpath) || []).filter((x: any) => ids.includes(x))
              return await rawDao.fetchById(collectionMemberIds, {client, propertyBlacklist})
            }
          }
            break
          case 'subdocument': {
            // TODO Revisit
            const parent = await _.last(parentDaos)
                .fetchOneById(_.last(parentIds), parentIds.slice(0, -1), {client, propertyBlacklist})
            if (!parent) {
              items = [] // TODO Or error?
            } else {
              items = (_.get(parent, collection.name) || []).filter((x: any) => x?._id && ids.includes(x._id))
            }
          }
        }
      } else {
        // TODO Support stream-based fetching.
        const fetchResult = (ids.length > 0) ?
            await rawDao.fetch({l: {path: '_id'}, r: {constant: ids}, operator: 'in'}, {client, propertyBlacklist}) as FetchResults : []
        items = draftBatchId ? fetchResult.map((item: Entity) => unwrapDraft(item)) : fetchResult
      }

      if (returnMatchingList) {
        return ids.map((id) => items.find((item) => item._id == id))
      } else {
        return items
      }
    },

    // TODO Support fetching one subcollection member by ID
    fetchOneById: async function(id: Id, parentIds = [], {client = null, propertyBlacklist = []} = {}) {
      const collection = _.last(parentCollections)
      if (collection && parentDaos.length > 0 && parentIds.length > 0) {
        switch (collection.persistence) {
          case 'inverse-ref':
            // TODO Ensure that the item belongs to the parent's collection.
            const fetchResult = await rawDao.fetchOneById(id, {client, propertyBlacklist})
            return (draftBatchId && fetchResult) ? unwrapDraft(fetchResult) : fetchResult
          case 'ref':
            // TODO
            return null
          case 'subdocument': {
            // TODO Revisit
            const parent = await _.last(parentDaos)
                .fetchOneById(_.last(parentIds), parentIds.slice(0, -1), {client, propertyBlacklist})
            if (!parent) {
              return [] // TODO Or error?
            } else {
              return (_.get(parent, collection.name) || []).find((x: any) => x?._id == id)
            }
          }
        }
      } else {
        const fetchResult = await rawDao.fetchOneById(id, {client, propertyBlacklist})
        // TODO for drafts:          if (fetchResult && ((_.get(fetchResult,
        // 'draft._type') != entityType.name) || (fetchResult.draftBatchId != draftBatchId))) {
        // fetchResult = null
        //  }

        return (draftBatchId && fetchResult) ? unwrapDraft(fetchResult) : fetchResult
      }
    },

    fetchRelationships: async function(
      items: Entity[],
      pathsToExpand?: string[],
      {client = undefined, entityTypeAtPathPrefix = undefined, maxDepth = undefined, pathPrefix = undefined}:
      {client?: Client, entityTypeAtPathPrefix?: EntityType, maxDepth?: number, pathPrefix?: string} = {}
    ) {
      if (pathsToExpand && pathsToExpand.length == 0) {
        return
      }

      const currentEntityType = entityTypeAtPathPrefix !== undefined ? entityTypeAtPathPrefix : entityType

      // Initialize a map of known items, if not already initialized. This will be used to avoid fetching the same item
      // more than once.
      const knownItems: {[entityTypeName: string]: {[id: Id]: Entity}} = {
        [currentEntityType.name]: {}
      }
      for (const item of items) {
        if (item._id) {
          knownItems[currentEntityType.name][item._id] = item
        }
      }

      const entityTypes: {[entityTypeName: string]: EntityType} = {}
      const daos: {[entityTypeName: string]: Dao} = {}

      // Get all related item definitions in the current item's entity type.
      const maxRelationshipDepth = pathsToExpand ? Math.max(0, ...pathsToExpand.map((p) => pathDepth(p.replace(/^\$./, '')))) : maxDepth
      const relationships = docorm.config.schemaRegistry?.findRelationshipsInSchema(
        currentEntityType.schema,
        ['ref', 'inverse-ref'],
        undefined,
        maxRelationshipDepth
      )

      /*
      const groupedNestedPaths: {[pathPrefix: string]: string[]} = {}
      if (pathsToExpand) {
          // Split paths to expand into nested and non-nested.
          // Only expand non-nested paths on this iteration, call recursively for nested paths
          const nestedPaths = pathsToExpand.filter((item) => pathsToExpand?.includes(item.substr(0, item.lastIndexOf("."))))
          pathsToExpand = pathsToExpand.filter((item) => !nestedPaths.includes(item))

          for (const pathToExpand of pathsToExpand) {
            if (_.some(nestedPaths, (nestedPath: string) => nestedPath.startsWith(pathToExpand))) {
              groupedNestedPaths[pathToExpand] = nestedPaths.filter((nestedPath) => nestedPath.startsWith(pathToExpand))
            }
          }
      }
      */

      let numReferencesFetched = 0

      // For forward references, make the paths relative to the list of items (instead of relative to one item),
      // then apply them to the items to get a list of JSON pointers representing nodes to expand in the data graph.
      const forwardReferencePointersToExpand = pathsToExpand ?
          _.uniq(pathsToExpand.map((path) =>
            jsonPath({
              path: path.replace(/^\$/, '$[*]'),
              json: items,
              resultType: 'pointer'
            }) as string[]).flat()
          )
          : null
      numReferencesFetched += await this._fetchForwardReferences(items, relationships, forwardReferencePointersToExpand, {
        client,
        entityTypes,
        daos,
        knownItems,
        pathPrefix
      })

      numReferencesFetched +=  await this._fetchInverseReferences(items, relationships, pathsToExpand, {
        client,
        entityTypes,
        daos,
        knownItems
      })

      if (numReferencesFetched > 0) {
        await this.fetchRelationships(items, pathsToExpand, {client, entityTypeAtPathPrefix, pathPrefix});
      }

      /*
      // recursive call to expand nested paths
      for (const [key, value] of Object.entries(groupedNestedPaths)) {
        const relationship = relationships.find((r) => r.path == key)
        const relationshipEntityType = relationship ? await getEntityType(relationship.entityTypeName) : null
        if (relationshipEntityType) {
          await this.fetchRelationships(items, value, {client, entityTypeAtPathPrefix: relationshipEntityType, pathPrefix: key})
        }
      }
      */
    },

    _fetchForwardReferences: async function(
        items: Entity[],
        relationships: Relationship[],
        pointersToExpand?: JsonPointerStr[],
        options: FetchRelationshipsOptions = DEFAULT_FETCH_RELATIONSHIPS_OPTIONS
    ) {
      let {
        client,
        entityTypes,
        daos,
        knownItems,
        pathPrefix
      } = _.merge(options, DEFAULT_FETCH_RELATIONSHIPS_OPTIONS)

      // Get all related item references in the current item.
      // Expand any references that can be satisfied with items already loaded, and collect a list of the rest.
      const itemReferencesByEntityTypeName: {[entityTypeName: string]: {pointer: string, id: Id}[]} = {}
      let numReferencesFetched = 0
      for (const relationship of relationships.filter((r) => r.storage == 'ref')) {
        if (relationship.entityTypeName) {
          // relationship.path is a PropertyPathStr. Turn it into a JsonPathStr. This involves prepending "$.", but we
          // want it to refer to the items array rather than a single item, so we prepend "$[*]." instead.
          const relationshipPathPrefix = pathPrefix ? pathPrefix.toString().replace(/^\$/, '$[*]') : '$[*]'
          let pathInItemsArray = `${relationshipPathPrefix}.${relationship.path}`
          if (relationship.toMany) {
            pathInItemsArray = `${pathInItemsArray}[*]`
          }
          let referencePointers: string[] = jsonPath({path: pathInItemsArray, json: items, resultType: 'pointer'}) as string[]
          if (pointersToExpand) {
            referencePointers = _.intersection(referencePointers, pointersToExpand)
          }
          for (const referencePointer of referencePointers) {
            const reference = jsonPointer.get(items, referencePointer)
            // TODO Handle collections with forward references, where reference has the form {type: 'array', items: {storage: 'ref'}}.
            if (reference && reference.$ref) {
              const knownItem = _.get(knownItems, [relationship.entityTypeName, reference.$ref])
              if (knownItem) {
                // TODO Ensure that _.set works with all simple JSONPaths returned by JSONPath({resultType: 'path'}).
                // Alternatively, use JSON pointers instead.
                _.set(items, referencePointer, knownItem)
                numReferencesFetched += 1
              } else {
                itemReferencesByEntityTypeName[relationship.entityTypeName] =
                    itemReferencesByEntityTypeName[relationship.entityTypeName] || []
                itemReferencesByEntityTypeName[relationship.entityTypeName].push({
                  pointer: referencePointer,
                  id: reference.$ref
                })
              }
            }
          }
        }
      }

      for (const referencedItemEntityTypeName of _.keys(itemReferencesByEntityTypeName)) {
        const itemReferences = itemReferencesByEntityTypeName[referencedItemEntityTypeName]
        let referencedItemEntityType = entityTypes[referencedItemEntityTypeName]
        let referencedItemDao = daos[referencedItemEntityTypeName]
        if (!referencedItemEntityType) {
          referencedItemEntityType = await getEntityType(referencedItemEntityTypeName)
          entityTypes[referencedItemEntityTypeName] = referencedItemEntityType
        }
        if (!referencedItemEntityType) {
          throw new PersistenceError(
            'Unknown entity type when attempting to fetch referenced items.',
            {entityTypeName: referencedItemEntityTypeName}
          )
        }
        if (!referencedItemDao) {
          referencedItemDao = await makeDao(referencedItemEntityType, {draftBatchId})
          daos[referencedItemEntityTypeName] = referencedItemDao
        }
        const referencedItems = _.keyBy(
          await referencedItemDao.fetchByIds(itemReferences.map((ref) => ref.id), {client}),
          '_id'
        )
        knownItems[referencedItemEntityTypeName] = knownItems[referencedItemEntityTypeName] || {}
        _.assign(knownItems[referencedItemEntityTypeName], referencedItems)
        for (const reference of itemReferences) {
          const referencedItem = referencedItems[reference.id]
          if (referencedItem) {
            jsonPointer.set(items, reference.pointer, referencedItem)
            numReferencesFetched += 1
          } else {
            // TODO Consider throwing an error here instead.
            console.log('Warning: Referenced item was missing.')
            console.log(reference)
          }
        }
      }

      return numReferencesFetched
    },

    _fetchInverseReferences: async function(
        items: Entity[],
        relationships: Relationship[],
        pathsToExpand?: JsonPathStr[],
        options: FetchRelationshipsOptions = DEFAULT_FETCH_RELATIONSHIPS_OPTIONS
    ) {
      let {
        client,
        entityTypes,
        daos
      } = _.merge(options, DEFAULT_FETCH_RELATIONSHIPS_OPTIONS)

      let numReferencesFetched = 0

      // Get all related item references in the current item.
      // Expand any references that can be satisfied with items already loaded, and collect a list of the rest.
      const inverseReferencesByEntityTypeNameAndForeignKeyPath: {
        [entityTypeName: string]: {
          [foreignKeyPath: string]: {
            item: Entity,
            path: JsonPathStr,
            propertyPath: PropertyPathStr,
            toMany: boolean,
            parentId: Id
          }[]
        }
      } = {}
      for (const relationship of relationships.filter((r) => r.storage == 'inverse-ref')) {
        if (relationship.entityTypeName) {
          if (pathsToExpand && !pathsToExpand.includes(relationship.path)) {
            continue
          }
          if (!relationship.foreignKeyPath) {
            // TODO Provide more details.
            throw new PersistenceError('Missing foreign key path in relationship with storage inverse-ref')
          }
          inverseReferencesByEntityTypeNameAndForeignKeyPath[relationship.entityTypeName] = inverseReferencesByEntityTypeNameAndForeignKeyPath[relationship.entityTypeName] || {}
          inverseReferencesByEntityTypeNameAndForeignKeyPath[relationship.entityTypeName][relationship.foreignKeyPath] =
              inverseReferencesByEntityTypeNameAndForeignKeyPath[relationship.entityTypeName][relationship.foreignKeyPath] || []

          const propertyPathFromRoot: PropertyPathStr = jsonPathToPropertyPath(relationship.path)
          const parentPath: PropertyPathStr = shortenPath(propertyPathFromRoot, relationship.depthFromParent)
          const propertyPath: PropertyPathStr = tailPath(propertyPathFromRoot, relationship.depthFromParent)// CHANGE
          const parentPathInItemsArray: PropertyPathStr = ['$[*]', parentPath]
              .filter(Boolean).filter((x) => x.length > 0).join('.')
          const parentPointers = jsonPath({path: parentPathInItemsArray, json: items, resultType: 'pointer'}) as JsonPointerStr[]

          const parentItems: Entity[] = []
          for (const parentPointer of parentPointers) {
              const parentItem = jsonPointer.get(items, parentPointer)
              if (parentItem) {
                  parentItems.push(parentItem)
              }
          }

          const parentItemsWithUnfilledRelationships = parentItems.filter(
            (parentItem) => _.get(parentItem, propertyPath) === undefined
          )
          if (parentItems.length > 0) {
            inverseReferencesByEntityTypeNameAndForeignKeyPath[relationship.entityTypeName][relationship.foreignKeyPath].push(
              ...parentItemsWithUnfilledRelationships.map((parentItem) => ({
                item: parentItem,
                path: relationship.path,
                propertyPath: propertyPath,
                toMany: relationship.toMany,
                // CHANGE
                parentId: parentItem._id
              }))
            )
          }
        }
      }

      for (const referencedItemEntityTypeName of _.keys(inverseReferencesByEntityTypeNameAndForeignKeyPath)) {
        for (const foreignKeyPath of _.keys(inverseReferencesByEntityTypeNameAndForeignKeyPath[referencedItemEntityTypeName])) {
          const inverseReferences = inverseReferencesByEntityTypeNameAndForeignKeyPath[referencedItemEntityTypeName][foreignKeyPath]
          const parentIds = _.uniq(inverseReferences.map((r) => r.parentId))

          // Fetch all related items with this entity type and foreign key.
          if (parentIds.length > 0) {
  //        const itemReferences = itemReferencesByEntityTypeName[referencedItemEntityTypeName]
            let referencedItemEntityType = entityTypes[referencedItemEntityTypeName]
            let referencedItemDao = daos[referencedItemEntityTypeName]
            if (!referencedItemEntityType) {
              referencedItemEntityType = await getEntityType(referencedItemEntityTypeName)
              entityTypes[referencedItemEntityTypeName] = referencedItemEntityType
            }
            if (!referencedItemEntityType) {
              throw new PersistenceError(
                'Unknown entity type when attempting to fetch referenced items.',
                {entityTypeName: referencedItemEntityTypeName}
              )
            }
            if (!referencedItemDao) {
              referencedItemDao = await makeDao(referencedItemEntityType, {draftBatchId})
              daos[referencedItemEntityTypeName] = referencedItemDao
            }
            const relatedItemsByParentId = _.groupBy(
              await referencedItemDao.fetch({l: {path: `${foreignKeyPath}.$ref`}, r: {constant: parentIds}, operator: 'in'}, [], {client}),
              (referencedItem) => _.get(referencedItem, `${foreignKeyPath}.$ref`)
            )

            // Assign the related items to their relationships.
            for (const inverseReference of inverseReferences) {
              const relatedItems = relatedItemsByParentId[inverseReference.parentId] || [];
              if (inverseReference.toMany) {
                // TODO Order the related items if an order is configured.
                _.set(inverseReference.item, inverseReference.propertyPath, relatedItems)
                numReferencesFetched += 1
              } else if (relatedItems.length > 1) {
                // TODO Provide more detail.
                throw new PersistenceError(`Found more than one related item for a to-one relationship.`)
              } else if (relatedItems.length == 1) {
                _.set(inverseReference.item, inverseReference.propertyPath, relatedItems[0])
                numReferencesFetched += 1
              } else if (relatedItems.length == 0) {
                // Set the reference to null, since undefined means it has not been fetched.
                _.set(inverseReference.item, inverseReference.propertyPath, null)
              }
            }
          }
        }
      }

      return numReferencesFetched
    },

    // TODO For collections, what about adding an existing item to the collection? Here we only support creating a new
    // item in the collection.
    insert: async function(item: Entity, parentIds = [], {client = null} = {}) {
      item = this.sanitizeItem(item)
      if (!item._id) {
        item._id = uuidv4()
      }
      const collection = _.last(parentCollections)
      if (collection && parentDaos.length > 0 && parentIds.length > 0) {
        switch (collection.persistence) {
          case 'inverse-ref': {
            // TODO Use the schema's foreign key path instead of having one in the REST collection config.
            if (!collection.foreignKeyPath) {
              throw new PersistenceError('Collection lacks a foreign key path')
            }
            // TODO Optimize by fetching only the parent's _id.
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return null // TODO Error
            } else {
              // TODO Support an auto-incremented order property.
              _.set(item, collection.foreignKeyPath, {$ref: parent._id})

              for (const callback of dbCallbacks.beforeInsert || []) {
                await callback(item, {dao: this, draftBatchId})
              }

              const wrappedItem = draftBatchId ? wrapDraft(item) : item
              const insertResult = await rawDao.insert(wrappedItem, {client})
              item = draftBatchId ? unwrapDraft(insertResult) : insertResult
            }
          }
            break
          case 'ref': {
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return null // TODO Error
            } else {
              for (const callback of dbCallbacks.beforeInsert || []) {
                await callback(item, {dao: this, draftBatchId})
              }

              const wrappedItem = draftBatchId ? wrapDraft(item) : item
              const insertResult = await rawDao.insert(wrappedItem, {client})
              item = draftBatchId ? unwrapDraft(insertResult) : insertResult

              if (item && item._id) {
                _.set(
                  parent,
                  collection.subpath,
                  [...(_.get(parent, collection.subpath) || []), item._id]
                )
                await _.last(parentDaos).update(parent, parentIds.slice(0, -1))
              }
            }
            break
          }
          case 'subdocument': {
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return null // TODO Error
            } else {
              const existingItem = (_.get(parent, collection.subpath) || []).find((x: any) =>
                x?._id == item._id
              )
              if (existingItem) {
                return null // TODO Error
              } else {
                for (const callback of dbCallbacks.beforeInsert || []) {
                  await callback(item, {dao: this, draftBatchId})
                }

                _.set(
                  parent,
                  collection.subpath,
                  [...(_.get(parent, collection.subpath) || []), item]
                )
                await _.last(parentDaos).update(parent, parentIds.slice(0, -1))
              }
            }
            break
          }
        }
      } else {
        for (const callback of dbCallbacks.beforeInsert || []) {
          await callback(item, {dao: this, draftBatchId})
        }

        const wrappedItem = draftBatchId ? wrapDraft(item) : item
        const insertResult = await rawDao.insert(wrappedItem, {client})
        item = draftBatchId ? unwrapDraft(insertResult) : insertResult
      }
      for (const callback of dbCallbacks.afterInsert || []) {
        await callback(item, {dao: this, draftBatchId})
      }
      return item
    },

    // No support for parent IDs
    insertMultipleItems: async function(items: Entity[]) {
      if (items.length > 0) {
        if (draftBatchId) {
          items = items.map((item) => wrapDraft(item))
        }
        await rawDao.insertMultipleItems(items)
      }
    },

    save: async function(item: Entity, parentIds: Id[] = [], {client = null} = {}) {
      if (item._id) {
        return await this.update(item, parentIds, {client})
      } else {
        return await this.insert(item, parentIds, {client})
      }
    },

    update: async function(item: Entity, parentIds: Id[] = [], {client = null} = {}) {
      item = this.sanitizeItem(item)
      let originalItem = null
      if ([...dbCallbacks.beforeUpdate || [], ...dbCallbacks.afterUpdate || []].length > 0) {
        originalItem = await this.fetchOneById(item._id)
      }
      for (const callback of dbCallbacks.beforeUpdate || []) {
        await callback(originalItem, item, {dao: this, draftBatchId})
      }
      for (const callback of dbCallbacks.beforeUpdateWithoutOriginal || []) {
        await callback(item, {dao: this, draftBatchId})
      }
      const collection = _.last(parentCollections)
      if (collection && parentDaos.length > 0 && parentIds.length > 0) {
        switch (collection.persistence) {
          case 'inverse-ref': {
            // TODO Use the schema's foreign key path instead of having one in the REST collection config.
            if (!collection.foreignKeyPath) {
              throw new PersistenceError('Collection lacks a foreign key path')
            }
            // TODO Optimize by fetching only the parent's _id.
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return null // TODO Error
            } else {
              // TODO First check that the item belongs to the collection.
              // TODO Support an auto-incremented order property.
              _.set(item, collection.foreignKeyPath, {$ref: parent._id})
              const wrappedItem = draftBatchId ? wrapDraft(item) : item
              const updateResult = await rawDao.update(wrappedItem, {client})
              item = draftBatchId ? unwrapDraft(updateResult) : updateResult
            }
          }
            break
          case 'ref': {
            // TODO First check that the item belongs to the collection.
            const wrappedItem = draftBatchId ? wrapDraft(item) : item
            const updateResult = await rawDao.update(wrappedItem, {client})
            item = draftBatchId ? unwrapDraft(updateResult) : updateResult
            break
          }
          case 'subdocument': {
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return null // TODO Error
            } else {
              const existingItemIndex = (_.get(parent, collection.name) || []).findIndex((x: any) => x?._id == item._id)
              if (existingItemIndex < 0) {
                return null // TODO Error
              } else {
                const collectionItems = _.get(parent, collection.name)
                collectionItems.splice(existingItemIndex, 1, item)
                _.set(parent, collection.name, collectionItems)
                await _.last(parentDaos).update(parent, parentIds.slice(0, -1))
              }
            }
            break
          }
        }
      } else {
        const wrappedItem = draftBatchId ? wrapDraft(item) : item

        if (!draftBatchId && mayTrackChanges) {
          let trackChange = entityType.history?.trackChange
          if (_.isFunction(trackChange)) {
            if (originalItem == null) {
              originalItem = await this.fetchOneById(item._id)
            }
            trackChange = await trackChange(originalItem, item)
          }
          if (trackChange) {
            if (originalItem == null) {
              originalItem = await this.fetchOneById(item._id)
              if (_.isEqual(originalItem, item)) {
                trackChange = false
              }
            }
            if (trackChange) {
              await recordItemVersion(originalItem, client)
            }
          }
        }

        const updateResult = await rawDao.update(wrappedItem, {client})
        item = draftBatchId ? unwrapDraft(updateResult) : updateResult
      }
      for (const callback of dbCallbacks.afterUpdateWithoutOriginal || []) {
        await callback(item, {dao: this, draftBatchId})
      }
      for (const callback of dbCallbacks.afterUpdate || []) {
        await callback(originalItem, item, {dao: this, draftBatchId})
      }
      return item
    },

    // No support for parent IDs
    updateMultipleItems: async function(items: Entity[]) {
      await rawDao.updateMultipleItems(items)
    },

    // For now we just support property-equality queries with one or more properties.
    delete: async function(query?: QueryClause, parentIds: Id[] = [], {client = null} = {}) {
      const collection = _.last(parentCollections)
      if (collection && parentDaos.length > 0 && parentIds.length > 0) {
        switch (collection.persistence) {
          case 'inverse-ref':
            // TODO
            break
          case 'ref':
            // TODO
            break
          case 'subdocument': {
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1), {client})
            if (!parent) {
              return [] // TODO Or error?
            } else {
              // TODO Implement collection filtering by query
              return (_.get(parent, collection.name) || []).find(() => false)
            }
          }
        }
      } else {
        let idsToDelete: Id[] = []
        if (
          (dbCallbacks.beforeDelete || []).length > 0
          || (dbCallbacks.afterDelete || []).length > 0
        ) {
          const itemsToDelete = await rawDao.fetch(query, {client}) as FetchResults
          idsToDelete = itemsToDelete.map((item) => item._id)
        }
        for (const id of idsToDelete) {
          for (const callback of dbCallbacks.beforeDelete || []) {
            await callback(id, {dao: this})
          }
        }

        await rawDao.delete(query, {client})

        for (const id of idsToDelete) {
          for (const callback of dbCallbacks.afterDelete || []) {
            await callback(id, {dao: this})
          }
        }

        /*
        if (rows.length == 1) {
          return {_id: rows[0].id, ...rows[0].data}
        } else {
          return null
        }
        */
      }
    },

    deleteOneById: async function(id: Id, parentIds = [], {client = null} = {}) {
      const collection = _.last(parentCollections)
      if (collection && parentDaos.length > 0 && parentIds.length > 0) {
        switch (collection.persistence) {
          case 'inverse-ref': {
            // TODO Use the schema's foreign key path instead of having one in the REST collection config.
            if (!collection.foreignKeyPath) {
              throw new PersistenceError('Collection lacks a foreign key path')
            }
            // TODO Optimize by fetching only the parent's _id.
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return null // TODO Error
            } else {
              // TODO First check that the item belongs to the collection.
              // TODO Support an auto-incremented order property, which may be decremented for following siblings.
              for (const callback of dbCallbacks.beforeDelete || []) {
                await callback(id, {dao: this})
              }
              await rawDao.deleteOneById(id, {client})
              for (const callback of dbCallbacks.afterDelete || []) {
                await callback(id, {dao: this})
              }
            }
          }
            break
          case 'ref': {
            // TODO First check that the item belongs to the collection.
            for (const callback of dbCallbacks.beforeDelete || []) {
              await callback(id, {dao: this})
            }
            await rawDao.deleteOneById(id, {client})
            for (const callback of dbCallbacks.afterDelete || []) {
              await callback(id, {dao: this})
            }
            // TODO Delete the reference from the parent.
            break
          }
          case 'subdocument': {
            const parent = await _.last(parentDaos).fetchOneById(_.last(parentIds), parentIds.slice(0, -1))
            if (!parent) {
              return null // TODO Error
            } else {
              const existingItemIndex = (_.get(parent, collection.name) || []).findIndex((x: any) => x?._id == id)
              if (existingItemIndex < 0) {
                return null // TODO Error
              } else {
                for (const callback of dbCallbacks.beforeDelete || []) {
                  await callback(id, {dao: this})
                }
                const collectionItems = _.get(parent, collection.name)
                collectionItems.splice(existingItemIndex, 1)
                _.set(parent, collection.name, collectionItems)
                await _.last(parentDaos).update(parent, parentIds.slice(0, -1))
                for (const callback of dbCallbacks.afterDelete || []) {
                  await callback(id, {dao: this})
                }
              }
            }
            break
          }
        }
      } else {
        for (const callback of dbCallbacks.beforeDelete || []) {
          await callback(id, {dao: this})
        }
        await rawDao.deleteOneById(id, {client})
        for (const callback of dbCallbacks.afterDelete || []) {
          await callback(id, {dao: this})
        }
      }
    }
  }
}

export default makeDao
