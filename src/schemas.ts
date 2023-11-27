import _ from 'lodash'

import {InternalError} from './errors.js'
import {importDirectory} from './import-dir.js'

export type JSONSchema = object
export type EntitySchema = JSONSchema
export type ConcreteEntitySchema = EntitySchema

export type SchemaPath = string | string[]
export type SchemaType = 'model' | 'view-model'
export interface SchemaDirectory {
  namespace?: string,
  schemaType: SchemaType,
  path: string,
  schemas: object
}
const schemaDirectories: SchemaDirectory[] = []

/**
 * 
 * @param path - The directory path containing schemas.
 * @param schemaType - The schema type
 * @param namespace 
 */
export async function registerSchemaDirectory(path: string, schemaType: SchemaType, namespace: string | undefined = undefined) {
  const schemas = await importDirectory(path, {recurse: true, extensions: ['json']})
  schemaDirectories.push({namespace, schemaType, path, schemas})
}

export function getSchema(path: SchemaPath, schemaType: SchemaType, namespace: string | undefined = undefined): EntitySchema | null {
  const pathArr = _.isArray(path) ? path : path.split('.')
  for (const dir of schemaDirectories.filter((d) => d.schemaType == schemaType && d.namespace == namespace)) {
    const schema = _.get(dir.schemas, pathArr, null)
    if (schema) {
      return schema
    }
  }
  return null
}

export interface MakeSchemaConcreteState {
  knownConcreteSubschemas: {
    [path: string]: EntitySchema
  },
  currentSchemaNewRef?: string
}

const INITIAL_MAKE_SCHEMA_CONCRETE_STATE = {
  knownConcreteSubschemas: {}
}

// TODO When expanding $refs, allow circularity. Concrete schemas can be circular; we must handle serialization and
// deserialization appropriately.
export function makeSchemaConcrete(
    schema: EntitySchema,
    schemaType: SchemaType = 'model',
    namespace: string | undefined = undefined,
    state: MakeSchemaConcreteState = INITIAL_MAKE_SCHEMA_CONCRETE_STATE
) {
  const {knownConcreteSubschemas, currentSchemaNewRef} = state
  let concreteSchema: EntitySchema = {}

  if (currentSchemaNewRef) {
    state.knownConcreteSubschemas[currentSchemaNewRef] = concreteSchema
  }
  if ((schema as any).allOf) {
    const concreteSubschemas: EntitySchema[] = (schema as any).allOf.map((subschema: EntitySchema) =>
      makeSchemaConcrete(subschema, schemaType, namespace, {knownConcreteSubschemas}))
    const invalidSubschemaIndex = concreteSubschemas.findIndex((s) => (s as any).type != 'object')
    if (invalidSubschemaIndex >= 0) {
      // Handle error
    }
    const requiredProperties = _.flatten(concreteSubschemas.map((s) => (s as any).required || []))
    const properties = Object.assign({}, ...concreteSubschemas.map((s) => (s as any).properties || {}))
    // TODO Validate the properties once merged?
    Object.assign(concreteSchema, {
      type: 'object',
      required: _.isEmpty(requiredProperties) ? undefined : requiredProperties,
      properties: _.isEmpty(properties) ? undefined : properties
    })
  } else if ((schema as any).oneOf) {
    // TODO To properly handle anyOf and oneOf, we need to let them remain in the concrete schema. Since our concrete
    // schema implementation doesn't support this yet, we currently treat them just like allOf, but we ignore
    // required properties.
    const concreteSubschemas: EntitySchema[] = (schema as any).oneOf.map((subschema: EntitySchema) =>
      makeSchemaConcrete(subschema, schemaType, namespace, {knownConcreteSubschemas}))
    const invalidSubschemaIndex = concreteSubschemas.findIndex((s) => (s as any).type != 'object')
    if (invalidSubschemaIndex >= 0) {
      // Handle error
    }
    const requiredProperties: string[] = [] // _.flatten(concreteSubschemas.map((s) => s.required || []))
    const properties = Object.assign({}, ...concreteSubschemas.map((s) => (s as any).properties || {}))

    // If the schema has oneOf at the top level, check each of the options. If any option describes a relationship
    // with an entity type or storage method (like 'ref' or 'inverse-ref'), copy those properties to the top level.
    // This ensures that we can expand references even when they occur as oneOf options.
    // TODO This approach is a kluge. We should ultimately distinguish between fully concrete schemas (which don't
    // have refs or storage) and unexpanded concrete schemas (which do).
    const refProperties = _.merge({}, ...(schema as any).oneOf.map((subschema: EntitySchema) =>
      _.pick(subschema, ['storage', 'foreignKey', 'entityType'])))

    // TODO Validate the properties once merged?
    Object.assign(concreteSchema, {
      type: 'object',
      ...refProperties,
      required: _.isEmpty(requiredProperties) ? undefined : requiredProperties,
      properties: _.isEmpty(properties) ? undefined : properties
    })
  } else if ((schema as any).$ref) {
    const path = _.trimStart((schema as any).$ref, '/').split('/')
    if ((state.knownConcreteSubschemas[(schema as any).$ref] != null) && ((schema as any).$ref != currentSchemaNewRef)) {
      concreteSchema = knownConcreteSubschemas[(schema as any).$ref]
    } else {
      // TODO Handle error if not found
      const subschema = getSchema(path, schemaType, namespace)
      if (!subschema) {
        throw new InternalError(`Schema refers to unknown subschema with $ref "${path}".`)
      }
      concreteSchema = makeSchemaConcrete(subschema, schemaType, namespace, {
        knownConcreteSubschemas,
        currentSchemaNewRef: (schema as any).$ref
      })
    }
    if ((schema as any).entityType || (schema as any).foreignKey || (schema as any).storage) {
      concreteSchema = _.cloneDeep(concreteSchema)
      // TODO Could cause problems if we had the same $ref with different entityTypes or storage.
      Object.assign(concreteSchema, _.pick(schema, ['entityType', 'foreignKey', 'storage']));
    }
  } else {
    switch ((schema as any).type) {
      case 'object':
        {
          Object.assign(concreteSchema, _.clone(schema))
          if ((concreteSchema as any).properties) {
            (concreteSchema as any).properties = _.mapValues(
              (concreteSchema as any).properties,
              (p) => makeSchemaConcrete(p, schemaType, namespace, {knownConcreteSubschemas})
            )
          }
        }
        break
      case 'array':
        {
          Object.assign(concreteSchema, _.clone(schema))
          if ((concreteSchema as any).items) {
            (concreteSchema as any).items = makeSchemaConcrete((concreteSchema as any).items, schemaType, namespace, {knownConcreteSubschemas})
          }
        }
        break
      default:
        Object.assign(concreteSchema, schema)
    }
  }
  return concreteSchema
}

export type RelationshipStorage = 'copy' | 'ref' | 'inverse-ref'

export interface Relationship {
  path: string
  toMany: boolean
  storage: RelationshipStorage
  entityTypeName: string
  schema: ConcreteEntitySchema
  foreignKeyPath?: string
}

export function findPropertyInSchema(schema: ConcreteEntitySchema, path: string | string[]): ConcreteEntitySchema | null {
  const schemaType = (schema as any).type
  if (!_.isArray(path)) {
    path = path.split('.')
  }
  if (path.length == 0) {
    // TODO Warn about an invalid path
    return null
  }
  switch (schemaType) {
    case 'object':
    {
      const subschema = _.get(schema, ['properties', path[0]], null)
      if ((path.length == 1) || (subschema == null)) {
        return subschema
      } else {
        return findPropertyInSchema(subschema, _.slice(path, 1))
      }
    }
    case 'array':
    {
      const subschema = _.get(schema, ['items'], null)
      if (subschema == null) {
        // TODO Warn about missing items in schema
        return null
      } else {
        return findPropertyInSchema(subschema, _.slice(path, 1))
      }
    }
    default:
      // TODO Warn that we're trying to find a property in a non-object schema.
      return null
  }
}

/*
* Find all related items reference by ID.
* currentPath is a JSONPath
*/
export function findRelationships(
    schema: ConcreteEntitySchema,
    allowedStorage?: RelationshipStorage[], 
    currentPath = '$',
    nodesTraversedInPath: ConcreteEntitySchema[] = []
) {
  // Do not traverse circular references.
  if (nodesTraversedInPath.includes(schema)) {
    return []
  }

  let relationships: Relationship[] = []
  const schemaType = (schema as any).type
  const oneOf = (schema as any).oneOf
  if (oneOf && _.isArray(oneOf)) {
    // This case does not actually arise right now, since we don't allow concrete schemas to use oneOf.
    for (const subschema of oneOf) {
      relationships = relationships.concat(
        findRelationships(subschema, allowedStorage, `${currentPath}`, [...nodesTraversedInPath, schema])
      )
    }
  } else {
    switch (schemaType) {
      case 'object':
        {
          const entityTypeName = (schema as any).entityType as string | undefined
          const storage = (schema as any).storage as RelationshipStorage | undefined
          if (entityTypeName && storage && (!allowedStorage || allowedStorage.includes(storage))) {
            const relationship: Relationship = {
              path: currentPath,
              toMany: false,
              storage: storage || 'copy',
              entityTypeName,
              schema
            }
            if (storage == 'inverse-ref') {
              const foreignKeyPath = (schema as any).foreignKey as string | undefined
              if (!foreignKeyPath) {
                // TODO Include the current location in the logged error.
                throw new InternalError(`Missing foreign key path in relationship with storage type inverse-ref`)
              }
              relationship.foreignKeyPath = foreignKeyPath
            }
            relationships.push(relationship)
          } else {
            const propertySchemas = _.get(schema, ['properties'], [])
            for (const property of _.keys(propertySchemas)) {
              const subschema = propertySchemas[property]
              relationships = relationships.concat(
                findRelationships(subschema, allowedStorage, `${currentPath}.${property}`, [...nodesTraversedInPath, schema])
              )
            }
          }
        }
        break
      case 'array':
        {
          const itemsSchema = (schema as any)?.items as ConcreteEntitySchema | undefined
          if (itemsSchema) {
            const entityTypeName = (itemsSchema as any).entityType as string | undefined
            const storage = (itemsSchema as any).storage as RelationshipStorage | undefined
            if (entityTypeName && storage && (!allowedStorage || allowedStorage.includes(storage))) {
              const relationship: Relationship = {
                path: currentPath,
                toMany: true,
                storage: storage || 'copy',
                entityTypeName,
                schema: itemsSchema
              }
              if (storage == 'inverse-ref') {
                const foreignKeyPath = (itemsSchema as any).foreignKey as string | undefined
                if (!foreignKeyPath) {
                  // TODO Include the current location in the logged error.
                  throw new InternalError(`Missing foreign key path in relationship with storage type inverse-ref`)
                }
                relationship.foreignKeyPath = foreignKeyPath
              }
              relationships.push(relationship)
            } else {
              relationships = relationships.concat(
                findRelationships(itemsSchema, allowedStorage, `${currentPath}[*]`, [...nodesTraversedInPath, schema])
              )
            }
          }
        }
        break
      default:
        break
    }
  }
  return relationships
}

/**
* Find all related item definitions along one path in a concrete schema.
*/
// TODO Needs adjustment to handle related items within arrays.
// TODO Do we really need this? If so, could we filter the results of findRelatedItemsInSchema?
export function findRelationshipsAlongPath(schema: ConcreteEntitySchema, path: string | string[], allowedStorage?: RelationshipStorage[], currentPath: string[] = []) {
  let relationships: Relationship[] = []
  const schemaType = (schema as any).type
  if (!_.isArray(path)) {
    path = path.split('.')
  }
  if (path.length == 0) {
    // TODO Warn about an invalid path
    return relationships
  }
  switch (schemaType) {
    case 'object':
      {
        const entityTypeName = (schema as any).entityType as string | undefined
        const storage = (schema as any).storage as RelationshipStorage | undefined
        if (entityTypeName && storage && (!allowedStorage || allowedStorage.includes(storage))) {
          relationships.push({
            path: currentPath.join('.'),
            toMany: false,
            storage: storage || 'copy',
            entityTypeName,
            schema
          })
        }

        // Whether or not the object is a relationship, continue traversing the path.
        const propertySchema = _.get(schema, ['properties', path[0]], null)
        if (!propertySchema) {
          // TODO Warn about property in the path that is not in the schema.
        } else {
          relationships = relationships.concat(
            findRelationshipsAlongPath(propertySchema, _.slice(path, 1), allowedStorage, [...currentPath, path[0]])
          )
        }
      }
      break
    case 'array':
      {
        const itemsSchema = (schema as any)?.items as ConcreteEntitySchema | undefined
        if (itemsSchema) {
          const entityTypeName = (itemsSchema as any).entityType as string | undefined
          const storage = (itemsSchema as any).storage as RelationshipStorage | undefined
          if (entityTypeName && storage && (!allowedStorage || allowedStorage.includes(storage))) {
            relationships.push({
              path: currentPath.join('.'),
              toMany: true,
              storage: storage || 'copy',
              entityTypeName,
              schema: itemsSchema
            })
          }

          // Whether or not the array is a relationship, continue traversing the path.
          const subschema = itemsSchema
          relationships = relationships.concat(
            findRelationshipsAlongPath(itemsSchema, _.slice(path, 1), allowedStorage, [...currentPath, path[0]])
          )
        }

        // TODO Isn't this redundant?
        const itemSchema = _.get(schema, ['items'], null)
        if (!itemSchema) {
          // TODO Warn about array entry in the path that has no schema.
        } else {
          relationships = relationships.concat(
            findRelationshipsAlongPath(itemSchema, _.slice(path, 1), allowedStorage, [...currentPath, path[0]])
          )
        }
      }
      break
    default:
      break
  }
  return relationships
}

/**
 * List all the transient properties of a concrete schema. Do not traverse relationships stored by reference.
 *
 * Transient properties are identified by the custom JSON schema attribute "custom".
 *
 * Because the schema must be concrete, it does not contain incorporate the contents of any other schemas; but it may
 * define entity-relationship properties that refer to other schemas. TODO Is this true? Clarify this point.
 *
 * @param {*} schema A concrete JSON schema (one that does not contain any references to other schemas).
 * @param {propertyPathElement[]} [currentPath=[]] - A path to a subschema to catalogue, used when this function calls
 *   itself recursively. The default value is an empty path, indicating that the whole schema shoulc be catalogued from
 *   its root down.
 * @return {string[]} A list of transient property paths in dot notation.
 */
export function listTransientPropertiesOfSchema(schema: ConcreteEntitySchema, currentPath: string[] = []) {
  let transientFieldPaths: string[] = []
  const schemaType = (schema as any).type
  const transient = (schema as any).transient
  const properties = (schema as any).properties as {[propertyName: string]: ConcreteEntitySchema}
  if (transient && currentPath.length > 0) {
    // The root of a schema cannot be transient.
    transientFieldPaths = [currentPath.join('.')]
  } else if (schemaType == 'object') {
    transientFieldPaths = _.flatten(
      _.map(properties, (subschema, name) =>
        ((subschema as any).storage == 'ref') ?
            [] : listTransientPropertiesOfSchema(subschema, currentPath.concat([name]))
      )
    )
  }

  return transientFieldPaths
}
