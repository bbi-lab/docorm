export class PersistenceError extends Error {
  context: object | undefined
  innerError: any

  constructor(message: any, context: object | undefined = undefined, innerError: any = undefined) {
    super(message)
    this.context = context
    this.innerError = innerError
  }
}

export class InternalError extends Error {
  context: object | undefined
  innerError: any

  constructor(message: string | undefined, context: object | undefined = undefined, innerError: any = undefined) {
    super(message)
    this.context = context
    this.innerError = innerError
  }
}
