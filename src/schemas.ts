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
    // TODO Validate the properties once merged?
    Object.assign(concreteSchema, {
      type: 'object',
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
      const concreteSubschema = makeSchemaConcrete(subschema, schemaType, namespace, {
        knownConcreteSubschemas,
        currentSchemaNewRef: (schema as any).$ref
      })
      if ((schema as any).entityType && (schema as any).storage) {
        // TODO Could cause problems if we had the same $ref with different entityTypes or storage.
        Object.assign(subschema, _.pick(schema, ['entityType', 'storage']))
      }
      Object.assign(concreteSchema, subschema)
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
            (concreteSchema as any).items = makeSchemaConcrete((concreteSchema as any).items,schemaType, namespace, {knownConcreteSubschemas})
          }
        }
        break
      default:
        Object.assign(concreteSchema, schema)
    }
  }
  return concreteSchema
}

export type RelatedItemStorage = 'copy' | 'ref'

export interface RelatedItem {
  path: string
  schema: ConcreteEntitySchema
  entityTypeName: string
  storage: RelatedItemStorage
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
export function findRelatedItemsInSchema(schema: ConcreteEntitySchema, allowedStorage?: RelatedItemStorage[], currentPath = '$') {
  let relatedItems: RelatedItem[] = []
  const schemaType = (schema as any).type
  const entityTypeName = (schema as any).entityType as string | undefined
  const storage = (schema as any).storage as RelatedItemStorage | undefined
  const items = (schema as any).items as ConcreteEntitySchema[]
  switch (schemaType) {
    case 'object':
      {
        if (entityTypeName && (!allowedStorage || (storage && allowedStorage.includes(storage)))) {
          relatedItems.push({
            path: currentPath, schema, entityTypeName, storage: storage || 'copy'
          })
        } else {
          const propertySchemas = _.get(schema, ['properties'], [])
          for (const property of _.keys(propertySchemas)) {
            const subschema = propertySchemas[property]
            relatedItems = relatedItems.concat(
              findRelatedItemsInSchema(subschema, allowedStorage, `${currentPath}.${property}`)
            )
          }
        }
      }
      break
    case 'array':
      {
        if (items) {
          relatedItems = relatedItems.concat(
            findRelatedItemsInSchema(items, allowedStorage, `${currentPath}[*]`)
          )
        }
      }
      break
    default:
      break
  }
  return relatedItems
}

/**
* Find all related item definitions along one path in a concrete schema.
*/
// TODO Needs adjustment to handle related items within arrays.
// TODO Do we really need this? If so, could we filter the results of findRelatedItemsInSchema?
export function findRelatedItemsInSchemaAlongPath(schema: ConcreteEntitySchema, path: string | string[], allowedStorage?: RelatedItemStorage[], currentPath: string[] = []) {
  let relatedItems: RelatedItem[] = []
  const schemaType = (schema as any).type
  const entityTypeName = (schema as any).entityType as string | undefined
  const storage = (schema as any).storage as RelatedItemStorage | undefined
  const items = (schema as any).items as ConcreteEntitySchema[]
  if (!_.isArray(path)) {
    path = path.split('.')
  }
  if (path.length == 0) {
    // TODO Warn about an invalid path
    return relatedItems
  }
  switch (schemaType) {
    case 'object':
      {
        if (entityTypeName && (!allowedStorage || (storage && allowedStorage.includes(storage)))) {
          relatedItems.push({
            path: currentPath.join('.'), schema, entityTypeName, storage: storage || 'copy'
          })
        }
        const subschema = _.get(schema, ['properties', path[0]], null)
        if (!subschema) {
          // TODO Warn
        } else {
          relatedItems = relatedItems.concat(
            findRelatedItemsInSchemaAlongPath(subschema, _.slice(path, 1), allowedStorage, [...currentPath, path[0]])
          )
        }
      }
      break
    case 'array':
      {
        const subschema = _.get(schema, ['items'], null)
        if (!subschema) {
          // TODO Warn about missing items in schema
        } else {
          relatedItems = relatedItems.concat(
            findRelatedItemsInSchemaAlongPath(subschema, _.slice(path, 1), allowedStorage, [...currentPath, path[0]])
          )
        }
      }
      break
    default:
      break
  }
  return relatedItems
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
