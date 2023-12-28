/**
 * Utilities for dynamically importing JavaScript modules from a directory
 *
 * @module lib/import-dir.ts
 */

import {readdir, stat} from 'fs/promises'
import path from 'path'

interface DirectoryImportOptions {
  /** A flag indicating whether to import modules recursively from subdirectories. */
  recurse: boolean,
  /** An array of filename extensions to import. */
  extensions: string[]
  /** Type assertion (for importing JSON files). */
  assertType?: 'json'
}

const DEFAULT_DIRECTORY_IMPORT_OPTIONS = {
  recurse: false,
  extensions: ['js']
}

/**
 * Extract the extension from a filename.
 *
 * Any text following the last period in the filename is considered to be the extension.
 *
 * @param filename The filename.
 * @return The extension, or TODO the empty string if there is no extension.
 */
function filenameExtension(filename: string): string {
  return filename.slice((Math.max(0, filename.lastIndexOf('.')) || Infinity) + 1)
}

interface DirectoryImportCatalogue {
  [submoduleName: string]: string | DirectoryImportCatalogue
}

/**
 * Catalogue the JavaScript modules contained in a directory.
 *
 * This is typically used with an absolute path, as in the following example:
 *
 * <code>
 * import path, {dirname} from 'path'
 * import {fileURLToPath} from 'url'
 * import {importDirectory} from '../lib/import-dir.js'
 * const __filename = fileURLToPath(import.meta.url)
 * const __dirname = dirname(__filename)
 * const availableModules = await catalogueDirectoryImports(path.join(__dirname, '../some/directory'), {recurse: true})
 * </code>
 *
 * @param directoryPath The path of the directory to catalogue.
 * @param options Import options. The assertType property must be set when importing JSON files.
 * @return An object whose property keys are the module names (filenames without extensions) and property values are the
 *   paths of the module files. Paths begin with directoryPath, so they are absolute or relative according to whether
 *   directoryPath is absolute or relative.
 */
export async function catalogueDirectoryImports(directoryPath: string, options: DirectoryImportOptions = DEFAULT_DIRECTORY_IMPORT_OPTIONS): Promise<DirectoryImportCatalogue> {
  const {extensions, recurse} = Object.assign({}, DEFAULT_DIRECTORY_IMPORT_OPTIONS, options)
  const result: DirectoryImportCatalogue = {}
  const filenames = (await isDirectory(directoryPath)) ? (await readdir(directoryPath)) : []
  await Promise.all(
    filenames.map(async (filename: string) => {
      const fullPath = path.join(directoryPath, filename)
      const submoduleName = path.basename(filename, path.extname(filename))
      const stats = await stat(fullPath)
      if (stats.isDirectory()) {
        if (recurse) {
          result[submoduleName] = await catalogueDirectoryImports(fullPath, options)
        }
      } else {
        const extension = filenameExtension(filename)
        if (!extensions || extensions.includes(extension)) {
          result[submoduleName] = fullPath
        }
      }
    })
  )
  return result
}

/**
 * Import all JavaScript modules contained in a directory.
 *
 * This is typically used with an absolute path, as in the following example:
 *
 * <code>
 * import path, {dirname} from 'path'
 * import {fileURLToPath} from 'url'
 * import {importDirectory} from '../lib/import-dir.js'
 * const __filename = fileURLToPath(import.meta.url)
 * const __dirname = dirname(__filename)
 * const importedModules = await importDirectory(path.join(__dirname, '../some/directory'), {recurse: true})
 * </code>
 *
 * @param directoryPath - The path of the directory to import.
 * @param options - Import options, including an import assert
 * @return - An object whose property keys are the module names (filenames without extensions) and property
 *   values are the default exports of each module. If the import is recursive, there is also a key for each
 *   subdirectory, whose value is another object of the same sort.
 */
export async function importDirectory(directoryPath: string, options: DirectoryImportOptions = DEFAULT_DIRECTORY_IMPORT_OPTIONS): Promise<object> {
  const {extensions, recurse} = Object.assign({}, DEFAULT_DIRECTORY_IMPORT_OPTIONS, options)
  const result: {[submoduleName: string]: any} = {}
  const filenames = (await isDirectory(directoryPath)) ? (await readdir(directoryPath)) : []

  // TODO Order of filenames should not matter, but is being applied here to ensure entity types are loaded
  // in the same order each time, resulting in consistent calculated concrete schemas.
  for (const filename of filenames.sort().reverse()) {
    const fullPath = path.join(directoryPath, filename)
    const submoduleName = path.basename(filename, path.extname(filename))
    const stats = await stat(fullPath)
    if (stats.isDirectory()) {
      if (recurse) {
        result[submoduleName] = await importDirectory(fullPath, options)
      }
    } else {
      const extension = filenameExtension(filename)
      if (!extensions || extensions.includes(extension)) {
        // console.log(`Importing file ${fullPath}`)

        // Using file:// makes this work with Windows paths that begin with drive letters.
        const importOptions: ImportCallOptions = {}
        if (options.assertType) {
          importOptions.assert = {type: options.assertType}
        }
        const {default: defaultExport} = await import(`file://${fullPath}`, importOptions)
        result[submoduleName] = defaultExport
      }
    }
  }
  return result
}

/**
 * Determine whether a path names a directory in the filesystem.
 *
 * @param path - The path to check.
 * @returns - True if the path is a directory, false otherwise.
 */
async function isDirectory(path: string) {
  try {
    const stats = await stat(path)
    return stats.isDirectory()
  } catch {
    return false
  }
}
