import _ from 'lodash'

import config, {DocOrmConfig, DocOrmConfigInput, setConfig} from './config.js'
import makeDao, {Dao} from './dao.js'
import {setLogger} from './logger.js'
import * as db from './postgresql/db.js'
import {
  calculateDerivedProperties,
  getEntityType,
  getEntityTypes,
  makeEntityType,
  registerEntityTypes
} from './entity-types.js'
import {InternalError, PersistenceError} from './errors.js'
import {
  findPropertyInSchema,
  findRelatedItemsInSchema,
  findRelatedItemsInSchemaAlongPath,
  getSchema,
  listTransientPropertiesOfSchema,
  makeSchemaConcrete,
  registerSchemaDirectory
} from './schemas.js'

export function initDocOrm(config: DocOrmConfigInput) {
  setConfig(config)
  if (config.logger) {
    setLogger(config.logger)
  }
  db.initDb()
}

export function makeDocOrmMiddleware() {
  const localConfig = config
  // Build a lambda closure that captures the current configuration.
  // We use 'any' here so that we don't need Express.js as a dependency.
  return (req: any, res: any, next: any) => {
    initDocOrm(localConfig)
    next()
  }
}

export {
  config,

  findPropertyInSchema,
  findRelatedItemsInSchema,
  findRelatedItemsInSchemaAlongPath,
  getSchema,
  listTransientPropertiesOfSchema,
  makeSchemaConcrete,
  registerSchemaDirectory,

  calculateDerivedProperties,
  getEntityType,
  getEntityTypes,
  makeEntityType,
  registerEntityTypes,

  db,

  makeDao,

  InternalError,
  PersistenceError
}
