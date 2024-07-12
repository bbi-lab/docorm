import _ from 'lodash'

export type QueryConstant = string | number | boolean | null
export type QueryFullTextSearchContext = 'default'
export type QueryPath = string

export interface QueryCoalesceExpression {coalesce: (QueryConstantExpression | QueryPathExpression)[]}
export interface QueryConstantExpression {constant: QueryConstant}
export interface QueryConstantListExpression {constant: QueryConstant[]}
export interface QueryFullTextExpression {text: QueryFullTextSearchContext}
export interface QueryFunctionExpression {function: string, parameters: (QueryConstantExpression | QueryPathExpression)[]}
export interface QueryOperatorExpression {operator: string, parameters: (QueryConstantExpression | QueryPathExpression)[]}
export interface QueryPathExpression {path: QueryPath, sqlType?: string}
export interface QueryRangeExpression {range: [QueryConstantExpression, QueryConstantExpression]}
export type QueryExpression = QueryCoalesceExpression | QueryConstantExpression | QueryConstantListExpression
    | QueryFullTextExpression | QueryFunctionExpression | QueryOperatorExpression | QueryPathExpression
    | QueryRangeExpression

export interface QueryBetweenClause {
  operator: 'between',
  l: QueryConstantExpression | QueryPathExpression,
  r: QueryRangeExpression
}

export interface QueryComparisonClause {
  operator?: '=' | '<' | '>' | '<=' | '>=' | '!=' | 'like' | '~',
  l: QueryConstantExpression | QueryPathExpression,
  r: QueryConstantExpression | QueryPathExpression
}

export interface QueryInClause {
  operator: 'in',
  l: QueryConstantExpression | QueryPathExpression,
  r: QueryConstantListExpression
}

export interface QueryFullTextSearchClause {
  operator: 'contains'
  l: QueryFullTextExpression
  r: QueryConstantExpression | QueryPathExpression
}

export type QuerySimpleClause = QueryBetweenClause | QueryComparisonClause | QueryInClause | QueryFullTextSearchClause

export interface QueryAndClause {
  and: QueryClause[]
}

export interface QueryOrClause {
  or: QueryClause[]
}

export interface QueryNotClause {
  not: QueryClause
}

export type QueryClause = QuerySimpleClause | QueryAndClause | QueryOrClause | QueryNotClause | true | false

export type QueryOrderProperty = QueryPathExpression
export type QueryOrderDirection = 'asc' | 'desc' | 'ASC' | 'DESC'
export type QueryOrderElement = QueryOrderProperty | [QueryOrderProperty, QueryOrderDirection]
export type QueryOrder = QueryOrderElement[]

export interface SqlExpression {
  expression: string,
  parameterValues: any[]
}

export interface SqlClause {
  sqlClause: string | null,
  parameterValues: any[]
}

export function queryExpressionIsCoalesce(expression: QueryExpression): expression is QueryCoalesceExpression {
  return ((expression as QueryCoalesceExpression).coalesce !== undefined)
}

export function queryExpressionIsConstant(expression: QueryExpression): expression is QueryConstantExpression {
  return ((expression as QueryConstantExpression).constant !== undefined)
    && !Array.isArray((expression as QueryConstantExpression).constant)
}

export function queryExpressionIsConstantList(expression: QueryExpression): expression is QueryConstantListExpression {
  return ((expression as QueryConstantExpression).constant !== undefined)
    && Array.isArray((expression as QueryConstantExpression).constant)
}

export function queryExpressionIsFunction(expression: QueryExpression): expression is QueryFunctionExpression {
  return ((expression as QueryFunctionExpression).function !== undefined)
}

export function queryExpressionIsOperator(expression: QueryExpression): expression is QueryOperatorExpression {
  return ((expression as QueryOperatorExpression).operator !== undefined)
}

export function queryExpressionIsFullText(expression: QueryExpression): expression is QueryFullTextExpression {
  return ((expression as QueryFullTextExpression).text == 'default')
}

export function queryExpressionIsPath(expression: QueryExpression): expression is QueryPathExpression {
  return (expression as QueryPathExpression).path !== undefined
}

export function queryExpressionIsRange(expression: QueryExpression): expression is QueryRangeExpression {
  return (expression as QueryRangeExpression).range !== undefined
}

export function queryClauseIsBetween(clause: QueryClause): clause is QueryBetweenClause {
  return (clause as QueryBetweenClause).operator == 'between'
}

export function queryClauseIsComparison(clause: QueryClause): clause is QueryComparisonClause {
  const operator = (clause as QueryComparisonClause).operator
  return !queryClauseIsFullTextSearch(clause) && [undefined, '=', '<', '>', '<=', '>=', '!='].includes(operator)
}

export function queryClauseIsIn(clause: QueryClause): clause is QueryInClause {
  return (clause as QueryInClause).operator == 'in'
}

export function queryClauseIsFullTextSearch(clause: QueryClause): clause is QueryFullTextSearchClause {
  const l = (clause as QueryFullTextSearchClause).l
  const r = (clause as QueryFullTextSearchClause).r
  return (clause as QueryFullTextSearchClause).operator == 'contains'
      && queryExpressionIsFullText(l)
      && (queryExpressionIsConstant(r) || queryExpressionIsPath(r))
}

export function queryClauseIsSimple(clause: QueryClause): clause is QuerySimpleClause {
  return queryClauseIsBetween(clause) || queryClauseIsComparison(clause) || queryClauseIsIn(clause)
      || queryClauseIsFullTextSearch(clause)
}

export function queryClauseIsAnd(clause: QueryClause): clause is QueryAndClause {
  return (clause as QueryAndClause).and != null
}

export function queryClauseIsOr(clause: QueryClause): clause is QueryOrClause {
  return (clause as QueryOrClause).or != null
}

export function queryClauseIsNot(clause: QueryClause): clause is QueryNotClause {
  return (clause as QueryNotClause).not != null
}

export function calculateExpression(context: any, expression: QueryExpression): any {
  if (queryExpressionIsCoalesce(expression)) {
    for (const subexpression of expression.coalesce) {
      if (subexpression != null) {
        return subexpression
      }
    }
    return null
  } else if (queryExpressionIsConstant(expression)) {
    return expression.constant
  } else if (queryExpressionIsConstantList(expression)) {
    return expression.constant
  } else if (queryExpressionIsFullText(expression)) {
    return expression.text
  } else if (queryExpressionIsFunction(expression)) {
    throw 'Functions are not supported in query expressions outside a database context.'
  } else if (queryExpressionIsOperator(expression)) {
    throw 'Operators are not supported in query expressions outside a database context.'
  } else if (queryExpressionIsPath(expression)) {
    return _.get(context, expression.path)
    // TODO Should we support type enforcement when the optional sqlType property is present?
  } else if (queryExpressionIsRange(expression)) {
    return expression.range
  }
}

export function applyQuery(x: any, query: QueryClause): boolean {
  if (query === true) {
    return true
  } else if (query === false) {
    return false
  } else if (queryClauseIsAnd(query)) {
    for (const subclause of query.and) {
      if (!applyQuery(x, subclause)) {
        return false
      }
      return true
    }
  } else if (queryClauseIsOr(query)) {
    for (const subclause of query.or) {
      if (applyQuery(x, subclause)) {
        return true
      }
      return false
    }
  } else if (queryClauseIsNot(query)) {
    return !applyQuery(x, query.not)
  } else { // queryClauseIsSimple
    if (queryClauseIsBetween(query)) {
      const left = calculateExpression(x, query.l)
      const right = calculateExpression(x, query.r)
      if (!_.isArray(right) || right.length != 2) {
        throw 'For "between" queries, the right side must be a range (an array of size 2).'
      }
      if (typeof left == 'number') {
        if (typeof right[0] != 'number' || typeof right[1] != 'number') {
          throw 'For "between" queries, the range must consist of numbers if the left side is a number.'
        }
        return right[0] <= left && left <= right[1]
      } else if (typeof left == 'string') {
        return right[0].toString() <= left && left <= right[1].toString()
      } else {
        return right[0] <= left && left <= right[1]
      }
    } else if (queryClauseIsComparison(query)) {
      const left = calculateExpression(x, query.l)
      const right = calculateExpression(x, query.r)
      switch (query.operator) {
        case '<':
          return left < right
        case '>':
          return left > right
        case '<=':
          return left <= right
        case '>=':
          return left >= right
        case '!=':
          return left != right
        case '=':
        default:
          return left == right
      }
    } else if (queryClauseIsIn(query)) {
      const left = calculateExpression(x, query.l)
      const right = calculateExpression(x, query.r)
      if (!_.isArray(right)) {
        throw 'For "in" queries, the right side must be an array.'
      }
      return right.includes(left)
    } else if (queryClauseIsFullTextSearch(query)) {
      throw 'Full text search queries are not supported outside a database context.'
    }
  }
  return false
}
