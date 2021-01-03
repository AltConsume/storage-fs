const { promisify } = require(`util`)
const { resolve } = require(`path`)
const debug = require(`debug`)(`amos:storage:fs`)
const fs = require(`fs`)

class FsStorage {
  constructor(baseDir) {
    debug(`instantiating storage`)

    try {
      // Make sure the base directory is created
      fs.mkdirSync(baseDir)
    } catch (error) {
      console.log('dir already created')
    }

    debug(`set storage dir to ${baseDir}`)
    this.dir = baseDir
  }

  async read(ref, id) {
    debug(`read ${id} from ${ref}`)

    try {
      const path = resolve(this.dir, ref, id)

      const file = await promisify(fs.readFile)(path)

      return JSON.parse(file.toString())
    } catch (error) {
      console.error(`Error occurred while reading ${id} from ${ref}`, error)

      return {}
    }
  }

  async write(ref, records, opts) {
    let _records = records

    if (!Array.isArray(records)) {
      _records = [ records ]
    }

    const baseDir = resolve(this.dir, ref)

    debug(`writing ${_records.length} records to ${ref} at ${baseDir}`)

    try {
      await promisify(fs.mkdir)(baseDir)
    } catch (error) {
      debug(`${baseDir} already exists`)
    }

    const promises = _records.map((record) => {
      if (!record.about || !record.about.identifier) {
        return Promise.resolve()
      }

      const path = resolve(baseDir, record.about.identifier);

      debug(`attempting to write ${path}`)

      return promisify(fs.writeFile)(path, JSON.stringify(record), { flag: `wx+` })
        .then(() => {
          debug(`wrote ${path}`)
        })
        .catch((error) => {
          debug(`failed writing ${path} due to error`, error)
        })
    })

    // Get as many as you can, but don't fail everything if one fails
    return Promise.allSettled(promises)
  }

  async ls(ref) {
    const dirPath = resolve(this.dir, ref)

    debug(`listing all files at ${dirPath}`)

    const files = await promisify(fs.readdir)(dirPath)

    return files.filter((file) => file !== `meta`)
  }

  async feed(ref, identifiers) {
    debug(`getting feed of ${identifiers.length} IDs for ${ref}`)

    const promises = identifiers.map(async (identifier) => {
      const path = resolve(this.dir, ref, identifier)

      debug(`reading ${path} for feed request`)

      const entityBuffer = await promisify(fs.readFile)(path)

      return JSON.parse(entityBuffer.toString())
    })

    debug(`trying to return ${promises.length} files for feed`)

    // Get as many as you can, but don't fail everything if one fails
    return Promise.allSettled(promises)
  }
}

module.exports = FsStorage
