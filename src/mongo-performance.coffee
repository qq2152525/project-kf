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

{MongoClient} = require "mongodb"

# {
# "_id" : ObjectId("526c87bd11ba6145572c3475"),
# "Name" : "周晓晨",
# "CardNo" : "",
# "Descriot" : "",
# "CtfTp" :
# "ID",
# "CtfId" : NumberLong("370682199312040223"),
# "Gender" : "F",
# "Birthday" : NumberLong(19931204),
# "Address" : "",
# "Zip" : "",
# "Dirty" : "F",
# "District1" : "",
# "District2" : "CHN",
# "District3" : 37,
# "District4" : 370682,
# "District5" : "",
# "District6" : "",
# "FirstNm" : "",
# "LastNm" : "",
# "Duty" : "",
# "Mobile" : "",
# "Tel" : "",
# "Fax" : "",
# "EMail" : "",
# "Nation" : "",
# "Taste" : "",
# "Education" : "",
# "Company" : "",
# "CTel" : "",
# "CAddress" : "",
# "CZip" : "",
# "Family" : 0,
# "Version" : "2012-12-16 7:43:30",
# "id" : NumberLong(19720186)
# }


DC_CITYS = ["北京","南京","上海","汉口","青岛","大连","沈阳","哈尔","西安","天津","重庆", "广州","深圳","香港","台湾","澳门" ]


console.log "[mongo-performance::init] %j", process.argv
#pathToCSVFile = path.join __dirname, process.argv[2]
pathToCSVFile = process.argv[2]
unless fs.existsSync(pathToCSVFile)
  console.log "[mongo-performance::init] missing csv sample at:#{pathToCSVFile}"
  process.exit(1)

MongoClient.connect "mongodb://127.0.0.1:27017/kf", (err, db) ->
  throw err if err
  #collection = db.collection("test_insert")
  collection = db.collection("members")

  console.log "[mongo-performance::init] db is ready"

  count = 0
  countInsert = 0

  # init a csv parsing job
  job = csv pathToCSVFile,
    headers : true

  job.on "data", (data) ->
    ++count
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

    console.log "[mongo-performance::csv::ondata] %j \n count:%d", data, count
    collection.insert data, (err, docs) ->
      if err?
        console.log "[mongo-performance::db::insert] #{err}"
      else
        ++countInsert
        console.log "[mongo-performance::db::insert] succeed:%j \n insert count:%d", docs, countInsert

    return

  # when csv parsing error
  job.on "error", (err) ->
    console.error "[mongo-performance::job] csv paring error:#{err}"
    return

  job.on "end", ->
    job.removeAllListeners()
    console.log "[mongo-performance::csv::on end] processed %d records", count
    #db.close()
    #process.exit(0)
    return

  job.parse()





