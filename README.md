# DocORM

Data persistence manager for document-oriented data

## Installation

```
npm install docorm@git+ssh://github.com/bbi-lab/docorm
```

The package will soon be available through npmjs.com.

## Setup

```js
import cls from 'cls-hooked'
import {db, initDocOrm, registerEntityTypes, registerSchemaDirectory} from 'docorm'
import path, {dirname} from 'path'
import {fileURLToPath} from 'url'
import {v4 as uuidv4} from 'uuid'

import config from '../config.js'
import logger from './logger.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

export async function initDataPersistence() {
 initDocOrm({
    logger // Optional Winston logger
    clsNamespaceName: 'arbitrary-namespace-name',
    operationIdKey: 'operation-id',
    postgresql: {
      host: 'localhost',
      port: 5432,
      username: 'postgres',
      password: 'postgres',
      database: 'my_database',
      allowUnknownSslCertificate: true,
      ssl: true
    }
  })
  await registerSchemaDirectory(path.join(__dirname, 'models-directory'), 'model')
  await registerEntityTypes(path.join(__dirname, 'schemas-directory'))
}
```

## Configuration

## Features

- Data models based on [JSON Schema](https://json-schema.org)
- A JSON-based query language suitable for exposure in APIs
- Relationship fetching
- Two query execution modes: immediate execution and streaming query execution using database cursors

# Database support

Currently, only PostgreSQL 13 and higher are supported.

## Schemas

Data model schemas adhere to [JSON Schema](https://json-schema.org), with some limitations and some extensions.

### Supported and unsupported JSON Schema keywords

#### Applicator keywords

Supported:
- `allOf`

Unsupported:
- `oneOf` and `anyOf`
- `if`, `then`, `else`, and `not`
- `properties`, `patternProperties`, and `additionalProperties`
- `dependentSchemas`
- `propertyNames`
- `prefixItems`
- `contains`

#### Validation keywords

Any validation keywords may be used, but currently only the following keywords are used in DocORM's built-in validation. Your own code may add support for other validation keywords.

- `type`
- `enum`
- 


## Data models

## Queries

## Data storage

## Connection management

## Running queries

## Use of JSON paths

[JSONPath](https://goessner.net/articles/JsonPath/), [JSON pointers](https://datatracker.ietf.org/doc/html/rfc6901), and simple (dot-separated or array) paths

Use of [JSONPath-Plus](https://github.com/JSONPath-Plus/JSONPath)

## Current & future directions

- More ORM-like interface for interacting with relationships between documents
- Ability to map JSON properties to relational database columns
