import _ from 'lodash'
import {SchemaRegistry} from 'schema-fun'
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
  },
  schemaRegistry?: SchemaRegistry
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
  },
  schemaRegistry?: SchemaRegistry
}

export const DEFAULT_DOC_ORM_CONFIG = {
  postgresql: {
    host: 'localhost',
    port: 5432,
    username: 'postgres',
    password: 'postgres',
    allowUnknownSslCertificate: false,
    ssl: true
  }
}
