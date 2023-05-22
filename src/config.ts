import _ from 'lodash'
import winston from 'winston'

export interface DocOrmConfigInput {
  logger?: winston.Logger,
  clsNamespaceName?: string,
  operationIdKey?: string,
  postgresql?: {
    host?: string,
    port?: number,
    username?: string,
    password?: string,
    database?: string,
    allowUnknownSslCertificate?: boolean,
    ssl?: boolean
  }
}

export interface DocOrmConfig extends DocOrmConfigInput {
  logger?: winston.Logger,
  clsNamespaceName?: string,
  operationIdKey?: string,
  postgresql: {
    host: string,
    port: number,
    username: string,
    password: string,
    database?: string,
    allowUnknownSslCertificate: boolean,
    ssl: boolean
  }
}

const DEFAULT_DOC_ORM_CONFIG = {
  postgresql: {
    host: 'localhost',
    port: 5432,
    username: 'postgres',
    password: 'postgres',
    allowUnknownSslCertificate: false,
    ssl: true
  }
}

let config: DocOrmConfig = {...DEFAULT_DOC_ORM_CONFIG}

export function setConfig(newConfig: DocOrmConfigInput) {
  config = _.merge({}, DEFAULT_DOC_ORM_CONFIG, newConfig)
}

export default config
