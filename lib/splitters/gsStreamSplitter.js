/**
 * Created by ferron on 10/3/18 1:04 PM
 * adapted from split-file-stream library
 * URL : https://github.com/dannycho7/split-file-stream/blob/master/index.js
 * credit to : @dannycho7
 */

const StreamSplitter = require('./streamSplitter')
const { PassThrough } = require('stream')
const {Storage} = require('@google-cloud/storage')
const zlib = require('zlib')
const gsurls = require('../gsurls')

class gsStreamSplitter extends StreamSplitter {
  constructor (file, options, context) {
    super(options)
    this.file = file
    this._ctx = context
    this.compress = options.gsCompress
  }

  _outStreamCreate (partitionNum) {
    let _throughStream = new PassThrough()
    if (this.compress) {
      _throughStream = zlib.createGzip()
    }

    const params = gsurls.fromUrl(this.file)
    const Key = StreamSplitter.generateFilePath(params.Key, partitionNum, this.compress)
    const storage = new Storage()
    const bucket = storage.bucket(params.Bucket)
    const file = bucket.file(Key)
    _throughStream.pipe(file.createWriteStream()
    ).on('error', error => {
      this._ctx.parent.emit('error', error)
    }).on('finish', () => {
      this._ctx.parent.emit('log', `Uploaded ${Key}`)
    })

    return _throughStream
  }
}

module.exports = gsStreamSplitter
