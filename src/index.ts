import _ from 'lodash'

// const {db, makeDao} = await import(`./${config.db.dbType}/index.js`)
import makeDao from './dao.js'
import * as db from './postgresql/db.js'

import config, {DocOrmConfigInput, setConfig} from './config.js'
import {setLogger} from './logger.js'
import {initDb} from './postgresql/db.js'

export function initDocOrm(config: DocOrmConfigInput) {
  setConfig(config)
  if (config.logger) {
    setLogger(config.logger)
  }
  initDb()
}

export {db, makeDao}
