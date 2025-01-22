import _ from 'lodash'
import QueryStream from 'pg-query-stream'
import {arrayToDottedPath, type PropertyPath} from 'schema-fun'
import {Transform} from 'stream'
import {v4 as uuidv4} from 'uuid'

import * as db from './db.js'
import {Entity, EntityType, EntityTypeMapping, Id, PropertyMapping} from '../entity-types.js'
import {PersistenceError} from '../errors.js'
import {
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

function sqlExpressionFromQueryExpression(expression: QueryExpression, mapping: EntityTypeMapping, parameterCount = 0): SqlExpression {
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
      expression: coerceType(sqlColumnFromPath(expression.path, mapping), expression.sqlType),
      parameterValues: []
    }
  } else if (queryExpressionIsFullText(expression)) {
    // TODO Support named full-text search contexts, like {text: 'default'}, {text: 'ids'}, {text: 'comments'}
    if (!mapping.jsonColumn) {
      throw new PersistenceError(`Full-text search cannot be applied to an entity type that lacks a JSON column).`)
    }
    return {
      expression: `${mapping.jsonColumn}::text`,
      parameterValues: []
    }
  } else if (queryExpressionIsFunction(expression)) {
    const subexpressions: string[] = []
    const parameterValues: any[] = []
    for (const jsonSubexpression of expression.parameters || []) {
      const {expression: subexpression, parameterValues: subexpressionParameterValues} =
          sqlExpressionFromQueryExpression(jsonSubexpression, mapping, parameterCount)
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
          sqlExpressionFromQueryExpression(jsonSubexpression, mapping, parameterCount)
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
          sqlExpressionFromQueryExpression(operatorParameter, mapping, parameterCount)
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
          sqlExpressionFromQueryExpression(rangePart, mapping, parameterCount)
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
function sqlColumnFromPath(path: string, mapping: EntityTypeMapping) {
  const propertyMapping = (mapping.propertyMappings || []).find((m) => m.propertyPath == path)
  if (propertyMapping) {
    return propertyMapping.column
  }
  if (path == '_id') {
    return 'id'
  }
  if (!mapping.jsonColumn) {
    throw new PersistenceError(`Entity type has unmapped properties and lacks a JSON column.`, {propertyPath: path})
  } else {
    const postgresComponents = propertyPathStringToArray(path, true)
    postgresComponents.unshift(`"${mapping.jsonColumn}"`)
    return postgresComponents.slice(0, -1).join('->') + '->>' + _.last(postgresComponents)
  }
}

function coerceType(sqlExpression: string, sqlType: string | null | undefined) {
  if (sqlType && SQL_TYPES.includes(sqlType.toLowerCase())) {
    return `(${sqlExpression})::${sqlType.toLowerCase()}`
  }
  return sqlExpression
}

function sqlQueryCriteriaClauseFromQueryClause(
    clause: QueryClause | undefined, mapping: EntityTypeMapping, parameterCount = 0)
: SqlClause {
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
          sqlQueryCriteriaClauseFromQueryClause(subclause, mapping, parameterCount)
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
    const {sqlClause: sqlSubclause, parameterValues} = sqlQueryCriteriaClauseFromQueryClause(subclause, mapping, parameterCount)
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
        clause.l ? sqlExpressionFromQueryExpression(clause.l, mapping, parameterCount) : {}
    parameterCount += leftExpressionParameterValues ? leftExpressionParameterValues.length : 0
    const {expression: rightExpression = null, parameterValues: rightExpressionParameterValues = null} =
        clause.r ? sqlExpressionFromQueryExpression(clause.r, mapping, parameterCount) : {}
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
            // unless we have a GIN index on the JSON column.
            clauseWrapper = (clause) => `(NOT data @? '$.${jsonClause.l.path}') OR (${clause})`
            */
          }
          if (queryExpressionIsPath(clause.r) && queryExpressionIsConstant(clause.l) && (clause.l.constant == null)) {
            operator = 'IS'
            /*
            // We don't really need to use @? to allow the case where the path doesn't exist, and using @? slows the query
            // unless we have a GIN index on the JSON column.
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
            // GIN index on the JSON column.
            clauseWrapper = (clause) => `(data @? '$.${jsonClause.l.path}') AND (${clause})`
            */
          }
          break
        case '~':
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
            // unless we have a GIN index on the JSON column.
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
  if (!entityType.mapping) {
    throw new PersistenceError(
      `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
      {entityTypeName: entityType.name}
    )
  }
  const mapping = entityType.mapping
  const entityTypeClause = mapping.jsonColumn ? `"${mapping.jsonColumn}"->>\'_type\' = $1` : undefined
  const parameterValues = mapping.jsonColumn ? [entityType.name] : []
  const {sqlClause: querySqlClause = null, parameterValues: queryParameterValues} =
      sqlQueryCriteriaClauseFromQueryClause(query, entityType.mapping, parameterValues.length)
  Array.prototype.push.apply(parameterValues, queryParameterValues)
  const clauses = [entityTypeClause, querySqlClause].filter(Boolean)
  return {
    sqlClause: clauses.length > 0 ? '(' + clauses.join(') AND (') + ')' : null,
    parameterValues
  }
}

function makeSqlQueryCriteriaClausesFromRawWhereClause(entityType: EntityType, whereClause: string | null, whereClauseParameters: any[]) {
  if (!entityType.mapping) {
    throw new PersistenceError(
      `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
      {entityTypeName: entityType.name}
    )
  }
  const mapping = entityType.mapping
  const entityTypeClause = mapping.jsonColumn ? `"${mapping.jsonColumn}"->>\'_type\' = $${whereClauseParameters.length + 1}` : undefined
  const parameterValues = [...whereClauseParameters, ...mapping.jsonColumn ? [entityType.name] : []]
  return {
    sqlClause: '(' + [entityTypeClause, whereClause].filter(Boolean).join(') AND (') + ')',
    parameterValues
  }
}

function makeQueryOrderPhrase(entityType: EntityType, order: QueryOrder | null | undefined) {
  if (!entityType.mapping) {
    throw new PersistenceError(
      `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
      {entityTypeName: entityType.name}
    )
  }
  const mapping = entityType.mapping
  if ((order == null) || (order.length == 0)) {
    return ''
  }
  const orderPhrases = _.map(order, (orderElement) => {
    const path = _.isArray(orderElement) ? orderElement[0] : orderElement
    const direction = (_.isArray(orderElement) && (orderElement.length > 1)
        && ['asc', 'desc'].includes(orderElement[1].toString().toLowerCase())) ?
        orderElement[1].toString().toLowerCase() : 'asc'
    return `${sqlExpressionFromQueryExpression(path, mapping).expression} ${direction}`
  })
  return ` ORDER BY ${orderPhrases.join(', ')}`
}

function getMappedQueryColumns(mapping: EntityTypeMapping, propertiesToExclude: PropertyPath[]) {
  const propertyPathsToExclude = propertiesToExclude.map((p) => propertyPathIsString(p) ? p : arrayToDottedPath(p))
  return (mapping.propertyMappings || [])
      .filter((m) => !propertyPathsToExclude.includes(m.propertyPath))
      .map((m) => m.column)
}

function sqlFetchColumnList(mapping: EntityTypeMapping, propertiesToExclude: PropertyPath[]) {
  const propertyBlacklistPhrase = propertyBlacklistToPhrase(propertiesToExclude)
  return [
    mapping.idColumn,
    ...mapping.jsonColumn ? [`${mapping.jsonColumn}${propertyBlacklistPhrase} AS _docorm_data`] : [],
    ...getMappedQueryColumns(mapping, propertiesToExclude)
  ].join(', ')
}

function rowToEntity(row: any, mapping: EntityTypeMapping): Entity {
  const entity = {...row._docorm_data || {}, _id: row[mapping.idColumn]}
  for (const m of mapping.propertyMappings || []) {
    _.set(entity, m.propertyPath, row[m.column])
  }
  return entity
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
  propertyBlacklist?: PropertyPath[],
  idsOnly?: boolean
}

/** Options for calls to fetch, with optional parameters supplied by defaults. */
interface FetchOptions extends FetchOptionsInput {
  client?: any,
  order?: QueryOrder,
  offset?: number,
  limit?: number,
  stream?: any,
  propertyBlacklist?: PropertyPath[],
  idsOnly: boolean
}

/** Default options for calls to fetch. */
const FETCH_DEFAULT_OPTIONS: FetchOptions = {
  idsOnly: false
}

const makeRawDao = function(entityType: EntityType) {
  return {
    entityType,
    
    count: async function(query?: QueryClause, options: CountOptions = {}) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`count failed because type "${entityType.name} has no table.`)
      }
      const {client} = options
      if (query === false) {
        return 0
      }
      const {sqlClause: clause, parameterValues} = makeSqlQueryCriteriaClauses(entityType, query)
      const whereClause = clause ? ` WHERE ${clause}` : ''
      const {rows} = await db.query(
        `SELECT count(*) AS count FROM "${mapping.table}"${whereClause}`, parameterValues, client
      )
      return rows[0].count
    },

    fetch: async function(query?: QueryClause, options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS): Promise<FetchResults | FetchResultsStream> {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`fetch failed because type "${entityType.name}" has no table.`)
      }
      if (query === false) {
        return []
      }
      const columns = sqlFetchColumnList(mapping, options.propertyBlacklist || [])
      const offsetPhrase = options.offset ? ` OFFSET ${options.offset}` : ''
      const limitPhrase = options.limit ? ` LIMIT ${options.limit}` : ''
      const {sqlClause: clause, parameterValues} = makeSqlQueryCriteriaClauses(entityType, query)
      const whereClause = clause ? ` WHERE ${clause}` : ''
      const orderPhrase = makeQueryOrderPhrase(entityType, options.order)
      if (options.stream) {
        const rowToItem = new Transform({
          objectMode: true,
          transform: (row, _, callback) => callback(null, rowToEntity(row, mapping))
        })
        const queryStream = await db.queryStream(
          `SELECT ${columns} FROM "${mapping.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
          parameterValues,
          options.client
        )
        return {run: queryStream.run, stream: queryStream.stream.pipe(rowToItem)}
        // return resultsStream.pipe(rowToItem)
      } else {
        const {rows} = await db.query(
          `SELECT ${columns} FROM "${mapping.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
          parameterValues,
          options.client
        )
        return rows.map((row) => rowToEntity(row, mapping))
      }
    },

    fetchWithSql: async function(
        whereClauseSql: string | null = null,
        whereClauseParameters = [],
        options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS
    ) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`fetchWithSql failed because type "${entityType.name} has no table.`)
      }
      const columns = sqlFetchColumnList(mapping, options.propertyBlacklist || [])
      const offsetPhrase = options.offset ? ` OFFSET ${options.offset}` : ''
      const limitPhrase = options.limit ? ` LIMIT ${options.limit}` : ''
      const {sqlClause: clause, parameterValues} =
          makeSqlQueryCriteriaClausesFromRawWhereClause(entityType, whereClauseSql, whereClauseParameters)
      const whereClause = clause ? ` WHERE ${clause}` : ''
      const orderPhrase = makeQueryOrderPhrase(entityType, options.order)
      const {rows} = await db.query(
        `SELECT ${columns} FROM "${mapping.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
        parameterValues,
        options.client
      )
      return rows.map((row) => rowToEntity(row, mapping))
    },

    fetchAll: async function(options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`fetchAll failed because type "${entityType.name} has no table.`)
      }
      const columns = sqlFetchColumnList(mapping, options.propertyBlacklist || [])
      const offsetPhrase = options.offset ? ` OFFSET ${options.offset}` : ''
      const limitPhrase = options.limit ? ` LIMIT ${options.limit}` : ''
      const whereClause = mapping.jsonColumn ? ` WHERE "${mapping.jsonColumn}"->>\'_type\' = $1` : ''
      const parameterValues = mapping.jsonColumn ? [entityType.name] : []
      const orderPhrase = makeQueryOrderPhrase(entityType, options.order)
      if (options.stream) {
        const rowToItem = new Transform({
          objectMode: true,
          transform: (row, _, callback) => callback(null, rowToEntity(row, mapping))
        })
        const queryStream = await db.queryStream(
          `SELECT ${columns} FROM "${mapping.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
          parameterValues,
          options.client
        )
        return {run: queryStream.run, stream: queryStream.stream.pipe(rowToItem)}
      } else {
        const {rows} = await db.query(
          `SELECT ${columns} FROM "${mapping.table}"${whereClause}${orderPhrase}${offsetPhrase}${limitPhrase}`,
          parameterValues,
          options.client
        )
        return rows.map((row) => rowToEntity(row, mapping))
      }
    },

    fetchById: async function(ids: Id[], options: FetchOptionsInput = FETCH_DEFAULT_OPTIONS) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`fetchById failed because type "${entityType.name} has no table.`)
      }
      const columns = sqlFetchColumnList(mapping, options.propertyBlacklist || [])
      const offsetPhrase = options.offset ? ` OFFSET ${options.offset}` : ''
      const limitPhrase = options.limit ? ` LIMIT ${options.limit}` : ''
      const orderPhrase = makeQueryOrderPhrase(entityType, options.order)
      const {rows} = await db.query(
        `SELECT ${columns} FROM "${mapping.table}" WHERE "${mapping.idColumn}" = ANY($1)`
            + (mapping.jsonColumn ? ` AND "${mapping.jsonColumn}"->>\'_type\' = $2` : '')
            + `${orderPhrase}${offsetPhrase}${limitPhrase}`,
        [ids, ...mapping.jsonColumn ? [entityType.name] : []],
        options.client
      )
      return rows.map((row) => rowToEntity(row, mapping))
    },

    fetchOneById: async function(id: Id, options: {
      client?: any,
      propertyBlacklist?: PropertyPath[]
    } = {}) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`fetchOneById failed because type "${entityType.name} has no table.`)
      }
      const columns = sqlFetchColumnList(mapping, options.propertyBlacklist || [])
      const {rows} = await db.query(
        `SELECT ${columns} FROM "${mapping.table}" WHERE "${mapping.idColumn}" = $1`
            + (mapping.jsonColumn ? ` AND "${mapping.jsonColumn}"->>\'_type\' = $2` : ''),
        [id, ...mapping.jsonColumn ? [entityType.name] : []],
        options.client
      )
      if (rows.length == 1) {
        return rowToEntity(rows[0], mapping)
      } else {
        return null
      }
    },

    // TODO Add support for mapped columns.
    insertMultipleItems: async function(items: any[]) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`insertMultipleItems failed because type "${entityType.name} has no table.`)
      }
      if (items.length > 0) {
        const columns = [
          mapping.idColumn,
          ...mapping.jsonColumn ? [mapping.jsonColumn] : []
        ]
        const rows = items.map((item) => {
          if (item._id) {
            return {
              [mapping.idColumn]: item._id,
              ...mapping.jsonColumn ? {[mapping.jsonColumn]: _.merge(_.omit(item, '_id'), {_type: entityType.name})} : {}
            }
          } else {
            return {
              [mapping.idColumn]: uuidv4(),
              ...mapping.jsonColumn ? {[mapping.jsonColumn]: _.merge(item, {_type: entityType.name})} : {}
            }
          }
        })
        await db.insertMultipleRows(mapping.table, columns, rows)
      }
    },

    // TODO Add support for mapped columns.
    insert: async function(item: any, options: {client?: any} = {}) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`insert failed because type "${entityType.name} has no table.`)
      }
      if (!item._id) {
        item._id = uuidv4()
      }
      const id = item._id
      item._id = undefined
      item._type = entityType.name

      const columns = [
        mapping.idColumn,
        ...mapping.jsonColumn ? [mapping.jsonColumn] : []
      ]
      const parameters = [
        '$1',
        ...mapping.jsonColumn ? ['$2'] : []
      ]
      const parameterValues = [
        id,
        ...mapping.jsonColumn ? [item] : []
      ]
      await db.query(
        `INSERT INTO "${mapping.table}"`
            + ` (${columns.map((c) => `"${c}"`).join(', ')})`
            + ` VALUES (${parameters.join(', ')})`,
        parameterValues,
        options.client
      )

      item._id = id

      return item
    },

    // TODO Add support for mapped columns.
    update: async function(item: any, options: {client?: any} = {}) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`update failed because type "${entityType.name} has no table.`)
      }
      const id = item._id
      item._id = undefined
      item._type = entityType.name

      const parameterValues = [
        id,
        ...mapping.jsonColumn ? [item, entityType.name] : []
      ]
      if (mapping.jsonColumn) {
        await db.query(
          `UPDATE "${mapping.table}" SET "${mapping.jsonColumn}" = $2`
              + ` WHERE "${mapping.idColumn}" = $1 AND "${mapping.jsonColumn}"->>'_type' = $3`,
          parameterValues,
          options.client
        )
      }

      item._id = id

      return item
    },

    // TODO Add support for mapped columns.
    updateMultipleItems: async function(items: any[]) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`updateMultipleItems failed because type "${entityType.name} has no table.`)
      }
      const columnsToUpdate = [
        ...mapping.jsonColumn ? [mapping.jsonColumn] : []
      ]
      const rows = items.map((item) => {
        if (item._id) {
          return {
            [mapping.idColumn]: item._id,
            ...mapping.jsonColumn ? {[mapping.jsonColumn]: _.merge(_.omit(item, '_id'), {_type: entityType.name})} : {}
          }
        } else {
          return null
        }
      }).filter((row): row is Exclude<typeof row, null> => row !== null)
      await db.updateMultipleRows(mapping.table, mapping.idColumn, columnsToUpdate, rows)
    },

    deleteOneById: async function(id: Id, options: {client?: any} = {}) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`deleteById failed because type "${entityType.name} has no table.`)
      }
      const parameterValues = [
        id,
        ...mapping.jsonColumn ? [entityType.name] : []
      ]
      if (mapping.jsonColumn) {
        await db.query(
          `DELETE FROM "${mapping.table}" WHERE "${mapping.idColumn}" = $1 and "${mapping.jsonColumn}"->>'_type' = $2`,
          parameterValues,
          options.client
        )
      }
    },

    delete: async function(query?: QueryClause, options: {client?: any} = {}) {
      if (!entityType.mapping) {
        throw new PersistenceError(
          `Cannot make SQL query for an unmapped entity type (${entityType.name})).`,
          {entityTypeName: entityType.name}
        )
      }
      const mapping = entityType.mapping
      if (!mapping.table) {
        throw new PersistenceError(`delete failed because type "${entityType.name} has no table.`)
      }
      const {sqlClause: clause, parameterValues} = makeSqlQueryCriteriaClauses(entityType, query)
      const whereClause = clause ? ` WHERE ${clause}` : ''
      if (whereClause.length < 1) {
        throw Error(`Attempt to delete all records from table ${mapping.table}`)
      }
      await db.query(
        `DELETE FROM "${mapping.table}"${whereClause}`,
        parameterValues,
        options.client
      )
    }
  }
}

export default makeRawDao
