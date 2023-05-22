/**
 * PostgreSQL database connection management and query utilities
 *
 * @module lib/db/postgresql/db
 */

import {Mutex} from 'async-mutex'
import cls from 'cls-hooked'
import pg from 'pg'
import QueryStream from 'pg-query-stream'
import pgpFactory from 'pg-promise'

import config from '../config.js'
import logger from '../logger.js'
import {PersistenceError} from '../errors.js'

// The following environment variables define the database connection:
// - PGHOST default: 'localhost'
// - PGPORT default: 5432
// - PGUSER default: process.env.user
// - PGDATABASE default: process.env.user
// - PGPASSWORD default: null

export type Client = pg.PoolClient & {lastQuery?: any[], numQueriesInTransaction?: number}

/** node-postgres connection pool. */
let pool: pg.Pool | null = null

/** pg-promise database factory. */
let pgp: ReturnType<typeof pgpFactory> | null

/** pg-promise database. */
let pgpDb: ReturnType<Exclude<typeof pgp, null>> | null = null

/** Mutex used to synchronize requests for new clients from a pool. */
const getClientMutex = new Mutex()

export function initDb() {
  // node-postgres setup

  pool = new pg.Pool({
    ssl: config.postgresql.ssl ?
        {
          rejectUnauthorized: !config.postgresql.allowUnknownSslCertificate
        }
        : undefined,
    max: 100
  })

  pool.on('error', (err) => {
    console.error('The database connection pool reported an error:', err)
  })

  // pg-promise setup

  // We have introduced pg-promise to handle bulk inserts better than raw node-postgres. This is used
  // in one case (saving REDCap events). We may soon stop using node-postgres directly altogether,
  // replacing getClient below with getTx (where transactions are needed) and getTask (where they are
  // not).

  const pgp = pgpFactory({capSQL: true})
  const pgpDb = pgp({
    host: config.postgresql.host,
    port: config.postgresql.port,
    database: config.postgresql.database,
    user: config.postgresql.username,
    password: config.postgresql.password,
    ssl: config.postgresql.ssl ?
        {rejectUnauthorized: !config.postgresql.allowUnknownSslCertificate}
        : undefined,
    max: 20
  })
}

function getClsNamespace() {
  const clsNamespace = config.clsNamespaceName ? cls.getNamespace(config.clsNamespaceName) : null
  const operationId = (config.operationIdKey ? clsNamespace?.get(config.operationIdKey) : undefined) as string | undefined
  return {clsNamespace, operationId}
}

/**
 * Obtain a database client for the current CLS context.
 *
 * The database client is a node-postgres client Client object managed by the node-postgres connection pool.
 *
 * Each Continuation Local Storage (CLS) context uses at most one database client at a time. A CLS context typicall
 * represents an API request in progress or a worker task. In typical usage, once requested, the client will remain
 * checked out by that context until the API request or worker task finishes. However, it is possible to release the
 * client and request a new one; this pattern may be appropriate for long-running worker tasks that do not need to use
 * a single transaction and only access the database intermittently.
 *
 * The CLS context is identified by the namespace "lims.db.transaction".
 *
 * This function either returns the client associated with the current CLS context or obtains a new client and
 * associates it with the context.
 *
 * This function is asynchronous, and it may need to wait (a) until a new connection is allocated and added to the pool,
 * if all connections are in use and the maximum pool size has not been reached, or (b) until a connection is released
 * back to the pool, in case the pool is full and all connections are in use.
 *
 * @param options
 * @param options.transactional - A flag indicating whether to start a transaction when obtaining a new client. This has
 *   no effect if a client is already associated with the CLS context.
 * @param options.useClientFromCLS - A flag indicating whether to use an existing client obtained from Continuation
 *   Local Storage. If false, a new client will be requested from the pool.
 * @return - A database client from the pool.
 */
export async function getClient({transactional = true, useClientFromCLS = true} = {}) {
  const {clsNamespace, operationId} = getClsNamespace()

  let client: Client | null = null
  if (useClientFromCLS && clsNamespace) {
    logger().info('TRYING TO GET CLIENT FROM CLS NAMESPACE')
    client = clsNamespace.get('client')
  }
  if (!client) {
    // TODO The mutex could be specific to this CLS context.
    await getClientMutex.runExclusive(async () => {
      // Check again for a client, in case another function call in our CLS context has obtained one.
      if (useClientFromCLS && clsNamespace) {
        logger().info('TRYING AGAIN TO GET CLIENT FROM CLS NAMESPACE')
        client = clsNamespace.get('client')
      }
      if (!client) {
        logger().info('GETTING CLIENT FROM POOL, operation ID=' + operationId)
        if (!pool) {
          throw new PersistenceError('No pool when attempting to get a new database client.')
        }
        client = await pool.connect()
        customizeClient(client)
        if (transactional) {
          await client.query('BEGIN')
          logger().info(`Began transaction`, {operationId})
        } else {
          logger().info(
            `Obtained database client without starting a database transaction`,
            {operationId}
          )
        }
        client.numQueriesInTransaction = 0
        if (useClientFromCLS && clsNamespace) {
          clsNamespace.set('client', client)
        }
      }
    })
  }
  if (!client) {
    throw 'Could not obtain a database client'
  }
  return client
}

/*
export function setupPgPromiseClient(transactional = true) {
  const clsNamespace = cls.getNamespace('lims.db.transaction')
  return new Promise((resolve, reject) => {
    if (transactional) {
      pgpDb.tx(async (t) => {
        clsNamespace.set('client', t)
        resolve(t)
      })
    } else {
      pgpDb.task(async (t) => {
        clsNamespace.set('client', t)
        resolve(t)
      })
    }
  })
}

async function getPgPromiseClient() {
  const clsNamespace = cls.getNamespace('lims.db.transaction')
  return clsNamespace.get('client')
}
*/

export async function commit(client?: Client) {
  const useClientFromCls = client == null
  const {clsNamespace, operationId} = getClsNamespace()

  if (useClientFromCls && clsNamespace) {
    client = clsNamespace.get('client')
  }
  if (!client) {
    throw new PersistenceError('No database client when attempting to commit changes.', {operationId})
  }
  if (client.numQueriesInTransaction && client.numQueriesInTransaction > 0) {
    await client.query('COMMIT')
    logger().info('Committed changes', {operationId})
    client.numQueriesInTransaction = 0
  }
}

export async function commitAndBeginTransaction(client: Client | null = null) {
  const useClientFromCls = client == null
  const {clsNamespace, operationId} = getClsNamespace()

 if (useClientFromCls && clsNamespace) {
    client = clsNamespace.get('client')
  }
  if (!client) {
    throw new PersistenceError(
      'No database client when attempting to commit changes and begin a new transaction.',
      {operationId}
    )
  }
  if (client && client.numQueriesInTransaction && client.numQueriesInTransaction > 0) {
    await client.query('COMMIT')
    logger().info('Committed changes', {operationId})
    await client.query('BEGIN')
    logger().info('Began transaction', {operationId})
    client.numQueriesInTransaction = 0
  }
}

export async function rollback(client: Client | null = null) {
  const useClientFromCls = client == null
  const {clsNamespace, operationId} = getClsNamespace()

  if (useClientFromCls && clsNamespace) {
    client = clsNamespace.get('client')
  }
  if (!client) {
    throw new PersistenceError('No database client when attempting to roll changes back.', {operationId})
  }
  if (client && client.numQueriesInTransaction && client.numQueriesInTransaction > 0) {
    await client.query('ROLLBACK')
    logger().info(`Rolled back transaction`, {operationId})
    client.numQueriesInTransaction = 0
  }
}

export function releaseClient(client: Client | null = null) {
  const useClientFromCls = client == null
  const {clsNamespace, operationId} = getClsNamespace()

  try {
    if (useClientFromCls && clsNamespace) {
      client = clsNamespace.get('client')
      clsNamespace.set('client', null)
    }
    if (client) {
      client.numQueriesInTransaction = 0
      client.release()
      logger().info(`Released database client`, {operationId})
    }
  } catch (err) {
    console.log(err)
    logger().log(
      'critical',
      'A database client could not be released.'
          + ' If this happens repeatedly, it may become impossible to get database clients',
      err
    )
  }
}

/**
 * Insert multiple rows, with no transaction.
 *
 * Unlike the query() function, this uses the pg-promise package instead of node-postgres. pg-promise offers more
 * efficient handling of bulk inserts than node-postgres, on which it is based.
 *
 * If we migrate fully to pg-promise, we can add support for transactions, which are not currently needed in the one
 * context where we perform bulk inserts.
 *
 * @param table - The name of the table.
 * @param columns - An array of names of columns to be populated.
 * @param rows - An array of row objects to insert. Each row object should have properties whose names match the column
 *   names.
 */
export async function insertMultipleRows(table: string, columns: string[], rows: object[]) {
  const {clsNamespace, operationId} = getClsNamespace()

  if (!pgp || !pgpDb) {
    throw new PersistenceError('pgp-promise has not been initialized before attempt to insert multiple rows.')
  }
  const columnSet = new pgp.helpers.ColumnSet(columns, {table})
  const query = pgp.helpers.insert(rows, columnSet)
  await pgpDb.none(query)
  logger().log('db', `Inserted ${rows.length} rows into ${table}`, {operationId})
}

export async function updateMultipleRows(table: string, idColumn: string, columnsToUpdate: string[], rows: object[]) {
  const {clsNamespace, operationId} = getClsNamespace()

  if (!pgp || !pgpDb) {
    throw new PersistenceError('pgp-promise has not been initialized before attempt to insert multiple rows.')
  }
  const columnSet = new pgp.helpers.ColumnSet(
    // [`?${idColumn}`, ...columnsToUpdate],
    [{name: idColumn, cnd: true, cast: 'uuid'}, ...columnsToUpdate.map((col) => ({name: col, cast: 'jsonb'}))],
    {table}
  )
  const query = pgp.helpers.update(rows, columnSet) + ` WHERE v.${idColumn} = t.${idColumn}`
  await pgpDb.none(query)
  logger().log('db', `Updated ${rows.length} rows in ${table}`, {operationId})
}

/**
 * Perform a database query.
 *
 * The query is run by the current node-postgres client, obtained by calling getClient(). TODO UPDATE for client param.
 *
 * @param sqlQuery - The query text, which may include numbered parameters of the form $1, $2, etc.
 * @param params - An array of parameter values. The first array element (at index 0) fills parameter $1, and so forth.
 * @param client - A database client to use. If null, then a client will be obtained by calling {@link getClient}.
 * @return - The query result as returned by node-postgres which may include a rows property.
 */
export async function query(sqlQuery: string, params: any[] = [], client: Client | null = null) {
  const useClientFromCls = client == null
  const {operationId} = getClsNamespace()

  if (useClientFromCls) {
    client = await getClient()
  }
  if (!client) {
    throw new PersistenceError('No database client when attempting to execute query.', {operationId})
  }
  const start = Date.now()
  try {
    // const res = await pool.query(text + ' ', params)
    const res = await client.query(sqlQuery + ' ', params)
    const duration = Date.now() - start
    logger().log('db', 'Executed query', {sqlQuery, params, duration, rows: res.rowCount, operationId})
    return res
  } catch (err) {
    throw new PersistenceError('Error while executing database query', {sqlQuery, params}, err)
  }
}

/**
 * Perform a database query and return results in a stream, using a database cursor.
 *
 * The query is run by the current node-postgres client, obtained by calling getClient().
 *
 * Streaming is accomplished using the pg-query-stream library. By using a database cursor, we are minimize the number
 * of records in memory.
 *
 * Notice that if other queries may be run to process this query's results while its cursor is still open, this
 * stream-based query should be run using a different database cursor. Otherwise deadlock may occur.
 *
 * @param text - The query text, which may include numbered parameters of the form $1, $2, etc.
 * @param params - An array of parameter values. The first array element (at index 0) fills parameter $1, and so forth.
 * @param client - A database client to use. If null, then a client will be obtained by calling {@link getClient}.
 * @return A stream of query result rows.
 */
export async function queryStream(sqlQuery: string, params: any[] = [], client: Client | null = null) {
  const useClientFromCls = client == null
  const {operationId} = getClsNamespace()

  if (useClientFromCls) {
    client = await getClient()
  }
  if (!client) {
    throw new PersistenceError('No database client when attempting to execute query with cursor.', {operationId})
  }
  // const start = Date.now()
  try {
    const query = new QueryStream(sqlQuery + ' ', params)
    // const resultsStream = await client.query(query)
    /* resultsStream.on('end', () => {
      const duration = Date.now() - start
      logger().log('db',
        'Executed query with cursor',
        {text, params, duration, transactionId: clsNamespace.get('tid')}
      )
      if (done) {
        done()
      }
    })*/
    logger().log('db', 'Executing query with cursor', {sqlQuery, params, operationId})
    const c = client // Without this, TypeScript doesn't recognize that client is non-null in the lambda expression.
    return {run: async () => await c.query(query), stream: query}
    // return resultsStream
  } catch (err) {
    throw new PersistenceError('Error while executing database query', {sqlQuery, params}, err)
  }
}

export async function customizeClient(client: Client) {
  const query = client.query
  const release = client.release
  client.numQueriesInTransaction = 0

  // Set a timeout of 5 seconds, after which we will log this client's last query.
  const timeout = setTimeout(() => {
    console.error(
      'A database client has been checked out for more than 5 seconds.',
      {query: client.lastQuery}
    )
    console.error(`The last executed query on this client was: ${client.lastQuery}`)
  }, 5000)

  // Monkey-patch the query method to count queries in a transaction and keep track of the last query executed.
  client.query = (async (...args: Parameters<typeof query>) => {
    client.lastQuery = args
    client.numQueriesInTransaction = (client.numQueriesInTransaction || 0) + 1
    return await query.apply(client, args)
  }) as typeof query

  client.release = () => {
    // Clear the timeout.
    clearTimeout(timeout)

    // Remove the monkey-patching.
    client.query = query
    client.release = release

    client.numQueriesInTransaction = 0
    return release.apply(client)
  }
  return client
}

export async function closePool() {
  if (pool) {
    await pool.end()
  }
}
