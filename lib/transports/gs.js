const JSONStream = require('JSONStream')
const { EOL } = require('os')
const base = require('./base.js')
const {Storage} = require('@google-cloud/storage')
const StreamSplitter = require('../splitters/gsStreamSplitter')
const { Readable, PassThrough, pipeline } = require('stream')
const zlib = require('zlib')
const gsurls = require('../gsurls')

class gs extends base {
  constructor (parent, file, options) {
    super(parent, file, options)
    this.streamSplitter = new StreamSplitter(file, parent.options, this)

    //initAws(parent.options)
    this._reading = false
  }

  async setupGet (offset) {
    // we don't support read from gs
  }

  // accept arr, callback where arr is an array of objects
  // return (error, writes)
  set (data, limit, offset, callback) {
    const error = null
    let lineCounter = 0

    if (!this._reading) {
      this._reading = true

      if (!this.shouldSplit) {
        this.stream = new Readable({
          read () {
            if (data.length === 0) {
              this.push(null)
            }
          }
        })

        let _throughStream = new PassThrough()
        if (this.parent.options.gsCompress) {
          _throughStream = zlib.createGzip()
        }

        const { Bucket, Key } = gsurls.fromUrl(this.file)
        const storage = new Storage()
        const bucket = storage.bucket(Bucket)
        const file = bucket.file(Key)
        this.stream.pipe(_throughStream).pipe(file.createWriteStream()
        ).on('error', error => {
          this.parent.emit('error', error)
          return callback(error)
        }).on('finish', () => {
          if(this.compress) {
            file.setMetadata({
              contentType: 'text/json',
              contentEncoding: 'gzip'
            })
          }
          this.parent.emit('log', `Uploaded ${Key}`)
        })
      }
    }

    if (data.length === 0) {
      this._reading = false

      if (this.shouldSplit) {
        return this.streamSplitter.end()
      }

      // close readable stream
      this.stream.push(null)
      this.stream.on('close', () => {
        delete this.stream
        return callback(null, lineCounter)
      })
    } else {
      lineCounter += this.__handleData(data)
      process.nextTick(() => callback(error, lineCounter))
    }
  }

  log (line) {
    if (this.shouldSplit) {
      this.streamSplitter.write(line)
    } else if (!line || line === '') {
      this.stream.push(null)
    } else {
      this.stream.push(line + EOL)
    }
  }
}

module.exports = {
  gs
}
