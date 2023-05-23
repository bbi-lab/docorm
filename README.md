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

## Features

## Configuration

## Current & future directions

- More ORM-like interface for interacting with relationships between documents
