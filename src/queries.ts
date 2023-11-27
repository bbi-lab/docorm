export type PropertyPath = string | string[]

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
  operator?: '=' | '<' | '>' | '<=' | '>=' | '!=' | 'like',
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

export function queryClauseIsComparisonn(clause: QueryClause): clause is QueryComparisonClause {
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
  return queryClauseIsBetween(clause) || queryClauseIsComparisonn(clause) || queryClauseIsIn(clause)
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
