type PropertyPath = string | string[]

type Id = string | number

type QueryConstant = string | number | boolean | null
type QueryFullTextSearchContext = 'default'
type QueryPath = string

interface QueryCoalesceExpression {coalesce: (QueryConstantExpression | QueryPathExpression)[]}
interface QueryConstantExpression {constant: QueryConstant}
interface QueryConstantListExpression {constant: QueryConstant[]}
interface QueryFullTextExpression {text: QueryFullTextSearchContext}
interface QueryFunctionExpression {function: string, parameters: (QueryConstantExpression | QueryPathExpression)[]}
interface QueryOperatorExpression {operator: string, parameters: (QueryConstantExpression | QueryPathExpression)[]}
interface QueryPathExpression {path: QueryPath, sqlType?: string}
interface QueryRangeExpression {range: [QueryConstant, QueryConstant]}
type QueryExpression = QueryCoalesceExpression | QueryConstantExpression | QueryConstantListExpression
    | QueryFullTextExpression | QueryFunctionExpression | QueryOperatorExpression | QueryPathExpression
    | QueryRangeExpression

interface QueryBetweenClause {
  operator: 'between',
  l: QueryConstantExpression | QueryPathExpression,
  r: QueryRangeExpression
}

interface QueryComparisonClause {
  operator?: '=' | '<' | '>' | '<=' | '>=' | '!=',
  l: QueryConstantExpression | QueryPathExpression,
  r: QueryConstantExpression | QueryPathExpression
}

interface QueryInClause {
  operator: 'in',
  l: QueryConstantExpression | QueryPathExpression,
  r: QueryConstantListExpression
}

interface QueryFullTextSearchClause {
  operator: 'contains'
  l: QueryFullTextExpression
  r: QueryConstantExpression | QueryPathExpression
}

type QuerySimpleClause = QueryBetweenClause | QueryComparisonClause | QueryInClause | QueryFullTextSearchClause

interface QueryAndClause {
  and: QueryClause[]
}

interface QueryOrClause {
  or: QueryClause[]
}

interface QueryNotClause {
  not: QueryClause
}

type QueryClause = QuerySimpleClause | QueryAndClause | QueryOrClause | QueryNotClause | true | false

type QueryOrderProperty = QueryPathExpression
type QueryOrderDirection = 'asc' | 'desc' | 'ASC' | 'DESC'
type QueryOrderElement = QueryOrderProperty | [QueryOrderProperty, QueryOrderDirection]
type QueryOrder = QueryOrderElement[]

interface SqlExpression {
  expression: string,
  parameterValues: any[]
}

interface SqlClause {
  sqlClause: string | null,
  parameterValues: any[]
}

function queryExpressionIsCoalesce(expression: QueryExpression): expression is QueryCoalesceExpression {
  return ((expression as QueryCoalesceExpression).coalesce !== undefined)
}

function queryExpressionIsConstant(expression: QueryExpression): expression is QueryConstantExpression {
  return ((expression as QueryConstantExpression).constant !== undefined)
    && !Array.isArray((expression as QueryConstantExpression).constant)
}

function queryExpressionIsConstantList(expression: QueryExpression): expression is QueryConstantListExpression {
  return ((expression as QueryConstantExpression).constant !== undefined)
    && Array.isArray((expression as QueryConstantExpression).constant)
}

function queryExpressionIsFunction(expression: QueryExpression): expression is QueryFunctionExpression {
  return ((expression as QueryFunctionExpression).function !== undefined)
}

function queryExpressionIsOperator(expression: QueryExpression): expression is QueryOperatorExpression {
  return ((expression as QueryOperatorExpression).operator !== undefined)
}

function queryExpressionIsFullText(expression: QueryExpression): expression is QueryFullTextExpression {
  return ((expression as QueryFullTextExpression).text == 'default')
}

function queryExpressionIsPath(expression: QueryExpression): expression is QueryPathExpression {
  return (expression as QueryPathExpression).path !== undefined
}

function queryExpressionIsRange(expression: QueryExpression): expression is QueryRangeExpression {
  return (expression as QueryRangeExpression).range !== undefined
}

function queryClauseIsBetween(clause: QueryClause): clause is QueryBetweenClause {
  return (clause as QueryBetweenClause).operator == 'between'
}

function queryClauseIsComparisonn(clause: QueryClause): clause is QueryComparisonClause {
  const operator = (clause as QueryComparisonClause).operator
  return !queryClauseIsFullTextSearch(clause) && [undefined, '=', '<', '>', '<=', '>=', '!='].includes(operator)
}

function queryClauseIsIn(clause: QueryClause): clause is QueryInClause {
  return (clause as QueryInClause).operator == 'in'
}

function queryClauseIsFullTextSearch(clause: QueryClause): clause is QueryFullTextSearchClause {
  const l = (clause as QueryFullTextSearchClause).l
  const r = (clause as QueryFullTextSearchClause).r
  return (clause as QueryFullTextSearchClause).operator == 'contains'
      && queryExpressionIsFullText(l)
      && (queryExpressionIsConstant(r) || queryExpressionIsPath(r))
}

function queryClauseIsSimple(clause: QueryClause): clause is QuerySimpleClause {
  return queryClauseIsBetween(clause) || queryClauseIsComparisonn(clause) || queryClauseIsIn(clause)
      || queryClauseIsFullTextSearch(clause)
}

function queryClauseIsAnd(clause: QueryClause): clause is QueryAndClause {
  return (clause as QueryAndClause).and != null
}

function queryClauseIsOr(clause: QueryClause): clause is QueryOrClause {
  return (clause as QueryOrClause).or != null
}

function queryClauseIsNot(clause: QueryClause): clause is QueryNotClause {
  return (clause as QueryNotClause).not != null
}
