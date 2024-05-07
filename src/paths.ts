import _ from 'lodash'

export type PropertyPathStr = string
export type PropertyPathArray = (string | number)[]
export type PropertyPath = PropertyPathStr | PropertyPathArray

export type JsonPathStr = string
export type JsonPointerStr = string

function hasKey<O extends object>(obj: O, key: PropertyKey): key is keyof O {
  return key in obj
}

export type PathTransformer = (path: string) => {path: string, additionalOptions?: {[key: string]: any}}

export function jsonPathToPropertyPath(jsonPath: string) {
  // TODO First ensure that the JSON path begins with $. and does not contain any complex elements.
  return jsonPath.substring(2)
}

export function dottedPathToArray(path: PropertyPathStr): PropertyPathArray {
  const pathArray = []
  let tail = path
  while (tail.length > 0) {
      const match = tail.match(/^([^\[]*)\[([0-9]+|\*)\]\.?(.*)$/)
      if (!match) {
          pathArray.push(tail)
          tail = ''
      } else {
          if (match[1] != null) {
              pathArray.push(match[1])
          }
          if (match[2] != null) {
              if (match[2] == '*') {
                  pathArray.push(-1)
              } else {
                  pathArray.push(parseInt(match[1]))
              }
          }
          tail = match[3] || ''
      }
  }
  return pathArray
}

export function pathDepth(path: PropertyPathStr) {
  return dottedPathToArray(path).length
}

export function arrayToDottedPath(pathArray: PropertyPathArray): PropertyPathStr {
  if (pathArray.length == 0) {
      return ''
  }
  const firstElement = pathArray[0]
  return [
      firstElement,
      ...pathArray.slice(1).map(
          (pathElement) => _.isNumber(pathElement) ? `[${pathElement == -1 ? '*' : pathElement}]` : `.${pathElement}`
      )
  ].join('')
}

export function shortenPath(path: PropertyPathStr, numComponentsToTrim: number): PropertyPathStr {
  const pathArray = dottedPathToArray(path)
  if (numComponentsToTrim > pathArray.length) {
      throw 'Error'
  } else {
      const shortenedPathArray = pathArray.slice(0, -numComponentsToTrim)
      return arrayToDottedPath(shortenedPathArray)
  }
}

export function tailPath(path: PropertyPathStr, numComponentsToTrim: number): PropertyPathStr {
  const pathArray = dottedPathToArray(path)
  if (numComponentsToTrim > pathArray.length) {
      throw 'Error'
  } else {
      const pathTailArray = pathArray.slice(-numComponentsToTrim)
      return arrayToDottedPath(pathTailArray)
  }
}

export function mapPaths<T>(x: T, transformPath: PathTransformer, visited: any[] = []): T {
  if (x == null) {
    return x
  }
  if (_.isArray(x)) {
    return x.map((element) => mapPaths(element, transformPath)) as T
  }
  if (!_.isObject(x)) {
    return x
  }
  visited.push(x)
  const mappedObject: {[key: string]: any} = {}
  for (const key in x) {
    //if (Object.prototype.hasOwnProperty.call(object, key)) {
    if (hasKey(x, key)) {
      const value = x[key]
      if (key == 'path' && _.isString(value)) {
        const {path, additionalOptions} = transformPath(value)
        mappedObject[key] = path
        if (additionalOptions) {
          _.assign(mappedObject, additionalOptions)
        }
      } else {
        if (!visited.includes(value)) {
          mappedObject[key] = mapPaths(value, transformPath, visited)
        }
      }
    }
  }
  return mappedObject as typeof x
}
