import _ from 'lodash'
import QueryStream from 'pg-query-stream'
import {Transform} from 'stream'
import {v4 as uuidv4} from 'uuid'

import * as db from './db.js'
import {Entity, EntityType, Id} from '../entity-types.js'
import {PersistenceError} from '../errors.js'
import {
  PropertyPath,
  QueryClause,
  queryClauseIsAnd,
  queryClauseIsFullTextSearch,
  queryClauseIsNot,
  queryClauseIsOr,
  QueryExpression,
  queryExpressionIsCoalesce,
  queryExpressionIsConstant,
  queryExpressionIsConstantList,
  queryExpressionIsFullText,
  queryExpressionIsFunction,
  queryExpressionIsOperator,
  queryExpressionIsPath,
  queryExpressionIsRange,
  QueryOrder,
  SqlClause,
  SqlExpression
} from '../queries.js'

const ALLOWED_OPERATORS = ['AND', 'OR', 'NOT']
const SQL_TYPES = ['boolean']

export type FetchResults = Entity[]
export interface FetchResultsStream {
  run: () => Promise<QueryStream>
  stream: Transform
}

export function fetchResultsIsArray(x: FetchResults | FetchResultsStream): x is FetchResults {
  return _.isArray(x)
}

export function fetchResultsIsStream(x: FetchResults | FetchResultsStream): x is FetchResultsStream {
  return !_.isArray(x)
}

function sqlExpressionFromQueryExpression(expression: QueryExpression, parameterCount = 0): SqlExpression {
  if (queryExpressionIsConstant(expression) || queryExpressionIsConstantList(expression)) {
    if (expression.constant == null) {
      return {
        expression: 'NULL',
        parameterValues: []
      }
    } else {
      return {
        expression: `$${parameterCount + 1}`,
        parameterValues: [expression.constant]
      }
    }
  } else if (queryExpressionIsPath(expression)) {
    return {
      expression: coerceType(sqlColumnFromPath(expression.path), expression.sqlType),
      parameterValues: []
    }
  } else if (queryExpressionIsFullText(expression)) {
    // TODO Support named full-text search contexts, like {text: 'default'}, {text: 'ids'}, {text: 'comments'}
    return {
      expression: 'data::text',
      parameterValues: []
    }
  } else if (queryExpressionIsFunction(expression)) {
    const subexpressions: string[] = []
    const parameterValues: any[] = []
    for (const jsonSubexpression of expression.parameters || []) {
      const {expression: subexpression, parameterValues: subexpressionParameterValues} =
          sqlExpressionFromQueryExpression(jsonSubexpression, parameterCount)
      if (subexpression) {
        subexpressions.push(subexpression)
      }
      if (subexpressionParameterValues) {
        Array.prototype.push.apply(parameterValues, subexpressionParameterValues)
        parameterCount += subexpressionParameterValues.length
      }
    }
    return {
      expression: `${expression.function}(${subexpressions.join(', ')})`,
      parameterValues
    }
  } else if (queryExpressionIsCoalesce(expression)) {
    const subexpressions: string[] = []
    const parameterValues: any[] = []
    for (const jsonSubexpression of expression.coalesce) {
      const {expression: subexpression, parameterValues: subexpressionParameterValues} =
          sqlExpressionFromQueryExpression(jsonSubexpression, parameterCount)
      if (subexpression) {
        subexpressions.push(subexpression)
      }
      if (subexpressionParameterValues) {
        Array.prototype.push.apply(parameterValues, subexpressionParameterValues)
        parameterCount += subexpressionParameterValues.length
      }
    }
    return {
      expression: `COALESCE(${subexpressions.join(', ')})`,
      parameterValues
    }
  } else if (queryExpressionIsOperator(expression)) {
    if (!ALLOWED_OPERATORS.includes(expression.operator.toUpperCase())) {
      throw new PersistenceError(
        'Bad JSON query expression: Unknown operator',
        {expression, operator: expression.operator}
      )
    }
    const subexpressions: string[] = []
    const parameterValues: any[] = []
    for (const operatorParameter of expression.parameters || []) {
      const {expression: subexpression, parameterValues: subexpressionParameterValues} =
          sqlExpressionFromQueryExpression(operatorParameter, parameterCount)
      if (subexpression) {
        subexpressions.push(subexpression)
      }
      if (subexpressionParameterValues) {
        Array.prototype.push.apply(parameterValues, subexpressionParameterValues)
        parameterCount += subexpressionParameterValues.length
      }
    }
    switch (expression.operator.toUpperCase()) {
      case 'NOT':
        return {
          expression: `(${expression.operator.toUpperCase()} ${subexpressions[0]})`,
          parameterValues
        }
      default:
        return {
          // TODO Check whether we need the parentheses here.
          expression: `(${subexpressions.join(` ${expression.operator.toUpperCase()} `)})`,
          parameterValues
        }
    }
  } else if (queryExpressionIsRange(expression)) {
    if (!_.isArray(expression.range) || expression.range.length != 2) {
      throw new PersistenceError('Bad JSON query expression', {expression})
    }
    const subexpressions: string[] = []
    const parameterValues: any[] = []
    for (const rangePart of expression.range) {
      const {expression: subexpression, parameterValues: subexpressionParameterValues} =
          sqlExpressionFromQueryExpression(rangePart, parameterCount)
      if (subexpression) {
        subexpressions.push(subexpression)
      }
      if (subexpressionParameterValues) {
        Array.prototype.push.apply(parameterValues, subexpressionParameterValues)
        parameterCount += subexpressionParameterValues.length
      }
    }
    return {
      expression: `${subexpressions.join(' AND ')}`,
      parameterValues
    }
  } else {
    throw new PersistenceError('Bad JSON query expression', {expression})
    /* return {
      expression: null,
      parameterValues: []
    }*/
  }
}

function propertyPathIsString(path: PropertyPath): path is string {
  return typeof(path) == 'string'
}

function propertyBlacklistElementFromPath(path: PropertyPath) {
  const pathArray = propertyPathIsString(path) ? propertyPathStringToArray(path, false) : path
  return `'{${pathArray.join(',')}}'`
}

function propertyBlacklistToPhrase(propertyBlacklist: PropertyPath[] | null | undefined) {
  return (propertyBlacklist && propertyBlacklist.length > 0) ?
      propertyBlacklist.map((path) => ` #- ${propertyBlacklistElementFromPath(path)}`).join('')
      : ''
}

function propertyPathStringToArray(pathStr: string, quoteNonIndexElements: boolean): (string | number)[] {
  return _.flatten(
    pathStr.split('.').map((component: string) => {
      const arrayIndices: number[] = []
      let match = component.match(/^(.*)\[(\d+)\]$/)
      while (match != null) {
        component = match[1]
        arrayIndices.unshift(_.parseInt(match[2]))
        match = component.match(/^(.*)\[(\d+)\]$/)
      }
      return [quoteNonIndexElements ? `'${component}'` : component, ...arrayIndices]
    })
  )
}

// TODO Perhaps support arrays etc. in paths.
function sqlColumnFromPath(path: string) {
  if (path == '_id') {
    return 'id'
  }
  const postgresComponents = propertyPathStringToArray(path, true)
  postgresComponents.unshift('data')
  return postgresComponents.slice(0, -1).join('->') + '->>' + _.last(postgresComponents)
}

function coerceType(sqlExpression: string, sqlType: string | null | undefined) {
  if (sqlType && SQL_TYPES.includes(sqlType.toLowerCase())) {
    return `(${sqlExpression})::${sqlType.toLowerCase()}`
  }
  return sqlExpression
}

function sqlQueryCriteriaClauseFromQueryClause(clause?: QueryClause, parameterCount = 0): SqlClause {
  if (clause === false) {
    // We don't even run the query in this case, but for completeness we will produce correct SQL.
    return {sqlClause: '0 = 1', parameterValues: []}
  } else if (clause == null || clause === true) {
    return {sqlClause: null, parameterValues: []}
  } else if (queryClauseIsAnd(clause) || queryClauseIsOr(clause)) {
    const subclauses = queryClauseIsAnd(clause) ? clause.and : clause.or
    const joinOperator = queryClauseIsAnd(clause) ? 'AND' : 'OR'
    const sqlSubclauses: (string | null)[] = []
    const parameterValues: any[] = []
    for (const subclause of subclauses) {
      const {sqlClause: sqlSubclause, parameterValues: subclauseParameterValues} =
          sqlQueryCriteriaClauseFromQueryClause(subclause, parameterCount)
      sqlSubclauses.push(sqlSubclause)
      if (subclauseParameterValues) {
        Array.prototype.push.apply(parameterValues, subclauseParameterValues)
        parameterCount += subclauseParameterValues.length
      }
    }
    if (sqlSubclauses.length == 0) {
      if (queryClauseIsAnd(clause)) {
        // Always true
        return {sqlClause: null, parameterValues: []}
      } else {
        // Always false
        return {sqlClause: '0 = 1', parameterValues: []}
      }
    } else if (sqlSubclauses.includes(null) && queryClauseIsOr(clause)) {
        // Always true
        return {sqlClause: null, parameterValues: []}
    } else {
      return {
        sqlClause: '(' + sqlSubclauses.filter((c) => c != null).join(`) ${joinOperator} (`) + ')',
        parameterValues
      }
    }
  } else if (queryClauseIsNot(clause)) {
    const subclause = clause.not
    const {sqlClause: sqlSubclause, parameterValues} = sqlQueryCriteriaClauseFromQueryClause(subclause, parameterCount)
    if (sqlSubclause) {
      return {
        sqlClause: `NOT (${sqlSubclause})`,
        parameterValues
      }
    } else {
      // Always false
      return {sqlClause: '0 = 1', parameterValues: []}
    }
  //} else if (queryClauseIsFullTextSearch(clause)) {
  } else {
    if (!queryClauseIsFullTextSearch(clause)) {
      // TODO The condition below used to be queryExpressionIsConstant(clause.l) && !queryExpressionIsConstant(clause.r).
      if (queryExpressionIsConstant(clause.l) && queryExpressionIsPath(clause.r)) {
        const swapTemp = clause.l
        clause.l = clause.r
        clause.r = swapTemp
      }
    }

    const {expression: leftExpression = null, parameterValues: leftExpressionParameterValues = null} =
        clause.l ? sqlExpressionFromQueryExpression(clause.l, parameterCount) : {}
    parameterCount += leftExpressionParameterValues ? leftExpressionParameterValues.length : 0
    const {expression: rightExpression = null, parameterValues: rightExpressionParameterValues = null} =
        clause.r ? sqlExpressionFromQueryExpression(clause.r, parameterCount) : {}
    parameterCount += rightExpressionParameterValues ? rightExpressionParameterValues.length : 0

    let operator: string | null = null
    let rightWrapper: (expr: string) => string = _.identity
    let clauseWrapper: (expr: string) => string = _.identity
    if (leftExpression && rightExpression) {
      switch (clause.operator) {
        case 'contains':
          if ((queryExpressionIsPath(clause.l) || queryExpressionIsFullText(clause.l))
              && queryExpressionIsConstant(clause.r) && (clause.r.constant != null)) {
            operator = 'ILIKE'
            rightWrapper = (expr) => `'%' || regexp_replace(${expr}, '([%_])', '\\\\\\1', 'g') || '%'`
          }
          // TODO Maybe we can support cases where the left side is constant: '"CONSTANTSTRING" contains column'
          break
        case 'like':
          operator = 'LIKE'
          break
        case '=':
        case undefined:
          operator = '='
          if (queryExpressionIsPath(clause.l) && queryExpressionIsConstant(clause.r) && (clause.r.constant == null)) {
            operator = 'IS'
            /*
            // We don't really need to use @? to allow the case where the path doesn't exist, and using @? slows the query
            // unless we have a GIN index on the data column.
            clauseWrapper = (clause) => `(NOT data @? '$.${jsonClause.l.path}') OR (${clause})`
            */
          }
          if (queryExpressionIsPath(clause.r) && queryExpressionIsConstant(clause.l) && (clause.l.constant == null)) {
            operator = 'IS'
            /*
            // We don't really need to use @? to allow the case where the path doesn't exist, and using @? slows the query
            // unless we have a GIN index on the data column.
            clauseWrapper = (clause) => `(NOT data @? '$.${jsonClause.r.path}') OR (${clause})`
            */
          }
          break
        case '!=':
          operator = clause.operator
          if (queryExpressionIsPath(clause.l) && queryExpressionIsConstant(clause.r)
              && (clause.r.constant == null)) {
            operator = 'IS NOT'
            /*
            // We don't really need to use @? to check that the path exists, and using @? slows the query unless we have a
            // GIN index on the data column.
            clauseWrapper = (clause) => `(data @? '$.${jsonClause.l.path}') AND (${clause})`
            */
          }
          break
        case '>=':
        case '>':
        case '<':
        case '<=':
          operator = clause.operator
          break
        case 'in':
          operator = '='
          rightWrapper = (expr) => `ANY (${expr})`
          // if (jsonExpressionIsPath(jsonClause.l) &&
          if (queryExpressionIsConstantList(clause.r) && clause.r.constant.includes(null)) {
            clauseWrapper = (sqlClause) => `((${leftExpression} IS NULL) OR (${sqlClause}))`
            /*
            // We don't really need to use @? to allow the case where the path doesn't exist, and using @? slows the query
            // unless we have a GIN index on the data column.
            clauseWrapper = (clause) =>
              `((NOT data @? '$.${jsonClause.l.path}') OR (${leftExpression} IS NULL) OR (${clause}))`
            */
          }
          break
        case 'between':
          if (!queryExpressionIsRange(clause.r)) {
            // TODO Use TypeScript type predicate on the whole clause to rule this case out.
            throw new PersistenceError('Bad JSON query clause', {clause})
          }
          operator = 'BETWEEN'
          break
        default:
          operator = null
      }
    }

    // TODO Before, we only checked if (operator).
    if (operator && leftExpression && rightExpression) {
      return {
        sqlClause: clauseWrapper(`${leftExpression} ${operator} ${rightWrapper(rightExpression)}`),
        parameterValues: ([] as any[]).concat(leftExpressionParameterValues || []).concat(rightExpressionParameterValues || [])
      }
    } else {
      return {sqlClause: null, parameterValues: []}
    }
  }
}

function makeSqlQueryCriteriaClauses(entityType: EntityType, query?: QueryClause) {
  const entityTypeClause = 'data->>\'_type\' = $1'
  const parameterValues = [entityType.name]
  const {sqlClause: querySqlClause = null, parameterValues: queryParameterValues} =
      sqlQueryCriteriaClauseFromQueryClause(query, parameterValues.length)
  Array.prototype.push.apply(parameterValues, queryParameterValues)
  return {
    sqlClause: '(' + [entityTypeClause, querySqlClause].filter(Boolean).join(') AND (') + ')',
    parameterValues
  }
}

function makeSqlQueryCriteriaClausesFromRawWhereClause(entityType: EntityType, whereClause: string | null, whereClauseParameters: any[]) {
  const entityTypeClause = `data->>'_type' = $${whereClauseParameters.length + 1}`
  const parameterValues = [...whereClauseParameters, entityType.name]
  return {
    sqlClause: '(' + [entityTypeClause, whereClause].filter(Boolean).join(') AND (') + ')',
    parameterValues
  }
}

function makeQueryOrderPhrase(entityType: EntityType, order: QueryOrder | null | undefined) {
  if ((order == null) || (order.length == 0)) {
    return ''
  }
  const orderPhrases = _.map(order, (orderElement) => {
    const path = _.isArray(orderElement) ? orderElement[0] : orderElement
    const direction = (_.isArray(orderElement) && (orderElement.length > 1)
        && ['asc', 'desc'].includes(orderElement[1].toString().toLowerCase())) ?
        orderElement[1].toString().toLowerCase() : 'asc'
    return `${sqlExpressionFromQueryExpression(path).expression} ${direction}`
  })
  return ` ORDER BY ${orderPhrases.join(', ')}`
}

interface CountOptions {
  client?: any
}

/** Options for calls to fetch, as passed by the caller. */
interface FetchOptionsInput {
  client?: any,
  order?: QueryOrder,
  offset?: number,
  limit?: number,
  stream?: any,
  propertyBlacklist?: PropertyPath[]
}

/** Options for calls to fetch, with optional parameters supplied by defaults. */
interface FetchOptions extends FetchOptionsInput {
  client?: any,
  order?: QueryOrder,
  offset?: number,
  limit?: number,
  stream?: any,
  propertyBlacklist?: PropertyPath[]
}

/** Default options for calls to fetch. */
const FETCH_DEFAULT_OPTIONS: FetchOptions = {}

const makeRawDao = function(entityType: EntityType) {
  return {
    entityType,

    count: async function(query?: QueryClause, options: CountOptions = {}) {
      if (!entityType.table) {
        throw new PersistenceError(`count failed because type "${entityType.name} has no table.`)
      }
      const {client} = options
      if (query === false) {
        return 0
      }
      const {sqlClause: clause, parameterValues} = makeSqlQueryCriteriaClauses(entityType, query)
      const whereClause = clause ? ` WHERE ${clause}` : ''
      const {rows} = await db.query(
        `SELECT count(*) AS count FROM "${entityType.table}"${whereClause}`, parameterValues, client
      )
      return rows[0].count
    },

    fetch: async function(query?: QueryClause, options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS): Promise<FetchResults | FetchResultsStream> {
      if (!entityType.table) {
        throw new PersistenceError(`fetch failed because type "${entityType.name} has no table.`)
      }
      if (query === false) {
        return []
      }
      const offsetPhrase = options.offset ? ` OFFSET ${options.offset}` : ''
      const limitPhrase = options.limit ? ` LIMIT ${options.limit}` : ''
      const {sqlClause: clause, parameterValues} = makeSqlQueryCriteriaClauses(entityType, query)
      const whereClause = clause ? ` WHERE ${clause}` : ''
      const propertyBlacklistPhrase = propertyBlacklistToPhrase(options.propertyBlacklist)
      const orderPhrase = makeQueryOrderPhrase(entityType, options.order)
      if (options.stream) {
        const rowToItem = new Transform({
          objectMode: true,
          transform: (row, _, callback) => callback(null, {...row.data, _id: row.id})
        })
        const queryStream = await db.queryStream(
          `SELECT id, data${propertyBlacklistPhrase} AS data FROM "${entityType.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
          parameterValues,
          options.client
        )
        return {run: queryStream.run, stream: queryStream.stream.pipe(rowToItem)}
        // return resultsStream.pipe(rowToItem)
      } else {
        const {rows} = await db.query(
          `SELECT id, data${propertyBlacklistPhrase} AS data FROM "${entityType.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
          parameterValues,
          options.client
        )
        return rows.map((row) => ({...row.data, _id: row.id}))
      }
    },

    fetchWithSql: async function(
        whereClauseSql: string | null = null,
        whereClauseParameters = [],
        options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS
    ) {
      if (!entityType.table) {
        throw new PersistenceError(`fetchWithSql failed because type "${entityType.name} has no table.`)
      }
      const offsetPhrase = options.offset ? ` OFFSET ${options.offset}` : ''
      const limitPhrase = options.limit ? ` LIMIT ${options.limit}` : ''
      const {sqlClause: clause, parameterValues} =
          makeSqlQueryCriteriaClausesFromRawWhereClause(entityType, whereClauseSql, whereClauseParameters)
      const whereClause = clause ? ` WHERE ${clause}` : ''
      const propertyBlacklistPhrase = propertyBlacklistToPhrase(options.propertyBlacklist)
      const orderPhrase = makeQueryOrderPhrase(entityType, options.order)
      const {rows} = await db.query(
        `SELECT id, data${propertyBlacklistPhrase} AS data FROM "${entityType.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
        parameterValues,
        options.client
      )
      return rows.map((row) => ({...row.data, _id: row.id}))
    },

    fetchAll: async function(options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS) {
      if (!entityType.table) {
        throw new PersistenceError(`fetchAll failed because type "${entityType.name} has no table.`)
      }
      const offsetPhrase = options.offset ? ` OFFSET ${options.offset}` : ''
      const limitPhrase = options.limit ? ` LIMIT ${options.limit}` : ''
      const whereClause = ' WHERE data->>\'_type\' = $1'
      const parameterValues = [entityType.name]
      const propertyBlacklistPhrase = propertyBlacklistToPhrase(options.propertyBlacklist)
      const orderPhrase = makeQueryOrderPhrase(entityType, options.order)
      if (options.stream) {
        const rowToItem = new Transform({
          objectMode: true,
          transform: (row, _, callback) => callback(null, {...row.data, _id: row.id})
        })
        const queryStream = await db.queryStream(
          `SELECT id, data${propertyBlacklistPhrase} AS data FROM "${entityType.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
          parameterValues,
          options.client
        )
        return {run: queryStream.run, stream: queryStream.stream.pipe(rowToItem)}
        // return resultsStream.pipe(rowToItem)
      } else {
        /*
        const {rows} = await db.query(
          `SELECT id, data FROM "${entityType.table}" WHERE`
              + ` data->>'_type' = $1${orderPhrase}${offsetPhrase}${limitPhrase}`,
          [entityType.name],
          client
        )
        */
        const {rows} = await db.query(
          `SELECT id, data${propertyBlacklistPhrase} AS data FROM "${entityType.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
          parameterValues,
          options.client
        )
        return rows.map((row) => ({...row.data, _id: row.id}))
      }
    },

    fetchById: async function(ids: Id[], options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS) {
      if (!entityType.table) {
        throw new PersistenceError(`fetchById failed because type "${entityType.name} has no table.`)
      }
      const offsetPhrase = options.offset ? ` OFFSET ${options.offset}` : ''
      const limitPhrase = options.limit ? ` LIMIT ${options.limit}` : ''
      const propertyBlacklistPhrase = propertyBlacklistToPhrase(options.propertyBlacklist)
      const orderPhrase = makeQueryOrderPhrase(entityType, options.order)
      const {rows} = await db.query(
        `SELECT data${propertyBlacklistPhrase} AS data FROM "${entityType.table}" WHERE`
            + ` id = ANY($1) AND data->>'_type' = $2${orderPhrase}${offsetPhrase}${limitPhrase}`,
        [ids, entityType.name],
        options.client
      )
      return rows.map((row) => ({...row.data, _id: row.id}))
    },

    fetchOneById: async function(id: Id, options: {
      client?: any,
      propertyBlacklist?: PropertyPath[]
    } = {}) {
      if (!entityType.table) {
        throw new PersistenceError(`fetchOneById failed because type "${entityType.name} has no table.`)
      }
      const propertyBlacklistPhrase = propertyBlacklistToPhrase(options.propertyBlacklist)
      const {rows} = await db.query(
        `SELECT data${propertyBlacklistPhrase} AS data FROM "${entityType.table}" WHERE id = $1 AND data->>'_type' = $2`,
        [id, entityType.name],
        options.client
      )
      if (rows.length == 1) {
        return {...rows[0].data, _id: id}
      } else {
        return null
      }
    },

    insertMultipleItems: async function(items: any[]) {
      if (!entityType.table) {
        throw new PersistenceError(`insertMultipleItems failed because type "${entityType.name} has no table.`)
      }
      if (items.length > 0) {
        const rows = items.map((item) => {
          if (item._id) {
            return {id: item._id, data: _.merge(_.omit(item, '_id'), {_type: entityType.name})}
          } else {
            return {id: uuidv4(), data: _.merge(item, {_type: entityType.name})}
          }
        })
        await db.insertMultipleRows(entityType.table, ['id', 'data'], rows)
      }
    },

    insert: async function(item: any, options: {client?: any} = {}) {
      if (!entityType.table) {
        throw new PersistenceError(`insert failed because type "${entityType.name} has no table.`)
      }
      if (!item._id) {
        item._id = uuidv4()
      }
      const id = item._id
      item._id = undefined
      item._type = entityType.name
      await db.query(`INSERT INTO "${entityType.table}" (id, data) VALUES ($1, $2)`, [id, item], options.client)
      item._id = id

      return item
    },

    update: async function(item: any, options: {client?: any} = {}) {
      if (!entityType.table) {
        throw new PersistenceError(`update failed because type "${entityType.name} has no table.`)
      }
      const id = item._id
      item._id = undefined
      item._type = entityType.name
      await db.query(
        `UPDATE "${entityType.table}" SET data = $2 WHERE id = $1 AND data->>'_type' = $3`,
        [id, item, entityType.name],
        options.client
      )
      item._id = id

      return item
    },

    updateMultipleItems: async function(items: any[]) {
      if (!entityType.table) {
        throw new PersistenceError(`updateMultipleItems failed because type "${entityType.name} has no table.`)
      }
      const rows = items.map((item) => {
        if (item._id) {
          return {id: item._id, data: _.merge(_.omit(item, '_id'), {_type: entityType.name})}
        } else {
          return null
        }
      }).filter((row): row is Exclude<typeof row, null> => row !== null)
      await db.updateMultipleRows(entityType.table, 'id', ['data'], rows)
    },

    deleteOneById: async function(id: Id, options: {client?: any} = {}) {
      if (!entityType.table) {
        throw new PersistenceError(`deleteById failed because type "${entityType.name} has no table.`)
      }
      await db.query(
        `DELETE FROM "${entityType.table}" WHERE id = $1 and data->>'_type' = $2`,
        [id, entityType.name],
        options.client
      )
    },

    delete: async function(query?: QueryClause, options: {client?: any} = {}) {
      if (!entityType.table) {
        throw new PersistenceError(`delete failed because type "${entityType.name} has no table.`)
      }
      const {sqlClause: clause, parameterValues} = makeSqlQueryCriteriaClauses(entityType, query)
      const whereClause = clause ? ` WHERE ${clause}` : ''
      if (whereClause.length < 1) {
        throw Error(`Attempt to delete all records from table ${entityType.table}`)
      }
      // const {clauses, parameterValues} = makeQueryCriteriaClauses(entityType, query)
      // const {rows} =
      await db.query(
        `DELETE FROM "${entityType.table}"${whereClause}`,
        parameterValues,
        options.client
      )
      // return rows.map(row => ({...row.data, _id: row.id}))
    }

  }
}

export default makeRawDao
