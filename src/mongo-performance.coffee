##
# mongo-performance
# https://github.com/yi/mongo-performance
#
# Copyright (c) 2013 yi
# Licensed under the MIT license.
##

mongoose = require 'mongoose'
Schema = mongoose.Schema
path = require "path"
fs = require "fs"
csv = require "fast-csv"
async = require "async"

{MongoClient} = require "mongodb"

DC_CITYS = ["北京","南京","上海","汉口","青岛","大连","沈阳","哈尔","西安","天津","重庆", "广州","深圳","香港","台湾","澳门" ]

COUNT_CSV = 0
COUNT_INSERTION = 0
CSV_FILES = []
CUR_CSV_FILE = null
DB_COLLECTION = null

console.log "[mongo-performance::init] %j", process.argv
#pathToCSVFile = path.join __dirname, process.argv[2]
pathToCSVFile = process.argv[2]
unless fs.existsSync(pathToCSVFile)
  console.log "[mongo-performance::init] missing csv folder at:#{pathToCSVFile}"
  process.exit(1)


try
  files = fs.readdirSync pathToCSVFile
  #console.log "[mongo-performance::file] #{files}"

  for file in files
    #console.log "[mongo-performance::file] #{path.extname(file)}"
    if path.extname(file) is ".csv"
      CSV_FILES.push(path.join(pathToCSVFile, file))


unless CSV_FILES.length > 0
  console.log "[mongo-performance::init] missing csv files at:#{pathToCSVFile}"
  process.exit(1)

console.log "[mongo-performance::init] CSV files to process:#{CSV_FILES}"

MongoClient.connect "mongodb://127.0.0.1:27017/kf", (err, db) ->
  throw err if err
  #DB_COLLECTION = db.collection("test_insert")
  DB_COLLECTION = db.collection("members")

  console.log "[mongo-performance::init] db is ready"
  async.eachSeries CSV_FILES, parseCSV, (err)->
    if err?
      console.log "[mongo-performance::each csv] error:#{err}"
    else
      console.log "DONE [mongo-performance::each csv] ALL DONE! csv entry:#{COUNT_CSV}, db etnry:#{COUNT_INSERTION}"

parseCSV = (filepath, next)->

  # init a csv parsing job
  job = csv filepath,
    headers : true

  countRead = 0
  countInsert = 0

  job.on "data", (data) ->
    ++COUNT_CSV
    ++countRead
    delete data["id"]
    delete data["Version"]
    delete data["Taste"]

    # process birth year
    idCardNum = String(data["CtfId"] || "")
    if idCardNum.length is 18
      data.birthYear = parseInt(idCardNum.substr(6,4))
      data.birthMonth = parseInt(idCardNum.substr(10,2))
      data.provinceCode = parseInt(idCardNum.substr(0,3))
      data.regionCode = parseInt(idCardNum.substr(0,6))
    else if idCardNum.length is 15
      data.birthYear = parseInt("19#{idCardNum.substr(6,2)}")
      data.birthMonth = parseInt(idCardNum.substr(8,2))
      data.provinceCode = parseInt(idCardNum.substr(0,3))
      data.regionCode = parseInt(idCardNum.substr(0,6))
    else
      #console.log "[mongo-performance::short] Birthday: #{data["Birthday"]}"
      data.birthYear = parseInt(String(data["Birthday"] || "").substr(0,4))
      data.birthMonth = parseInt(String(data["Birthday"] || "").substr(4,2))

    # process province
    address = data["Address"] || ""
    pos = address.indexOf("省")
    if pos > 0 and pos <  5
      province = address.substring(0, pos)
    else
      if (pos = DC_CITYS.indexOf(address.substr(0,2))) > 0
        province = DC_CITYS[pos]

    #console.log "[mongo-performance::csv::ondata] province:#{province}, address:#{address}"
    data["province"] = province if province?

    #console.log "[mongo-performance::csv::ondata] %j \n count:%d", data, count
    DB_COLLECTION.insert data, (err, docs) ->
      if err?
        console.log "[mongo-performance::db::insert] #{err}"
      else
        ++COUNT_INSERTION
        ++countInsert
        console.log "[mongo-performance::db::insert] succeed. ALL csv:#{COUNT_CSV}, insertion:#{COUNT_INSERTION}, CUR: csv:#{countRead}, insert:#{countInsert}, from:#{filepath}"

    return

  # when csv parsing error
  job.on "error", (err) ->
    console.error "[mongo-performance::job] csv paring error:#{err}"
    return

  job.on "end", ->
    job.removeAllListeners()
    console.log "[mongo-performance::csv::on end] complete #{filepath}"
    #db.close()
    #process.exit(0)
    next()
    return

  job.parse()
  return





