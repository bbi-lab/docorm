import _ from 'lodash'

import config, {DocOrmConfig, DocOrmConfigInput, setConfig} from './config.js'
import makeDao, {Dao} from './dao.js'
import {setLogger} from './logger.js'
import * as db from './postgresql/db.js'
import {InternalError, PersistenceError} from './errors.js'
import {
  ConcreteEntitySchema,
  EntitySchema,
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

export {
  DocOrmConfig,
  DocOrmConfigInput,
  config,

  ConcreteEntitySchema,
  EntitySchema,
  findPropertyInSchema,
  findRelatedItemsInSchema,
  findRelatedItemsInSchemaAlongPath,
  getSchema,
  listTransientPropertiesOfSchema,
  makeSchemaConcrete,
  registerSchemaDirectory,

  db,

  Dao,
  makeDao,

  InternalError,
  PersistenceError
}
