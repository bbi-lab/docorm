import _ from 'lodash'
import winston from 'winston'

import {docorm} from './index.js'

const levels = {
  emergency: 0,
  alert: 1,
  critical: 2,
  error: 3,
  warn: 4,
  notice: 5,
  info: 6,
  http: 7,
  db: 8,
  debug: 9,
  verbose: 10
}

const isDevelopment = false

const level = () => {
  return isDevelopment ? 'debug' : 'db'
}

const colors = {
  emergency: 'red',
  alert: 'red',
  critical: 'red',
  error: 'red',
  warn: 'yellow',
  notice: 'yellow',
  info: 'green',
  http: 'magenta',
  db: 'magenta',
  debug: 'white',
  verbose: 'white'
}

winston.addColors(colors)

const formatMetadataAsText = (metadata: object) => {
  return _.map(Object.keys(metadata || {}), (key) => {
    const value = metadata[key as keyof typeof metadata] as any
    if (value != null) {
      const formattedValue = _.isObject(value) || _.isArray(value) ? JSON.stringify(value) : value.toString()
      return `    ${key}: ${formattedValue}`
    }
  }).join('\n')
}

const textFormat = winston.format.combine(
  winston.format.errors({stack: true}),
  winston.format.colorize({all: true}),
  winston.format.metadata(),
  winston.format.timestamp({format: 'YYYY-MM-DD HH:mm:ss:ms'}),
  winston.format.printf((info) => {
    const message = info.message
    return [
      `${info.timestamp} ${info.level}: ${message}`,
      ...(_.keys(info.metadata || {}).length > 0 ? [formatMetadataAsText(info.metadata)] : [])
    ].join('\n')
  })
)

const jsonFormat = winston.format.combine(
  winston.format.metadata(),
  winston.format.errors({stack: true}),
  winston.format.timestamp({format: 'YYYY-MM-DD HH:mm:ss:ms'}),
  winston.format.json()
)

const transports = [
  new winston.transports.Console({
    format: isDevelopment ? textFormat : jsonFormat,
    handleExceptions: true
  })
  /* new winston.transports.File({
    filename: 'logs/error.log',
    level: 'error',
    format: jsonFormat
  })*/
]

let _logger: winston.Logger | null = null

export function setLogger(logger: winston.Logger | null) {
  _logger = logger
}

export default function logger() {
  if (!_logger && docorm.config.logger) {
    _logger = docorm.config.logger
  }
  if (!_logger) {
    _logger = winston.createLogger({
      level: level(),
      levels,
      // format,
      transports,
      exitOnError: false
    })
  }
  return _logger
}
