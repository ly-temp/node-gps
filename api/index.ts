const express = require("express")
import mysql from 'mysql2/promise'

const fetch = require("node-fetch")
const mqtt = require("mqtt")

const app = express()
const DB_PW_URI = process.env.DB_PW_URI
const DB_NAME = 'gps'
const MQTT_URI = process.env.MQTT_URI
const MQTT_PORT = process.env.MQTT_PORT
const MQTT_USER = process.env.MQTT_USER
const MQTT_PW = process.env.MQTT_PW
const MQTT_CLIENT_ID = 'vercel_mysql'

const API_KEY_SYNC = process.env.API_KEY_SYNC
const API_KEY_GET = process.env.API_KEY_GET

const GEO_API_KEY = process.env.GEO_API_KEY
const GEO_API_URI = `https://api.geoapify.com/v1/geocode/reverse?lang=zh&apiKey=${GEO_API_KEY}&`

const MQTT_TOPIC = 'gps/#'

//const KEY_NAME = "service"
//const KEY_VALUE = process.env.DB_INDEX
//const IN_API_KEY = process.env.API_KEY

const sleep = ms => new Promise(res => setTimeout(res, ms))

async function reverse_geo(lat, lon){
    if(!lat || !lon)
        return null
    const json = await (await fetch(GEO_API_URI + `lat=${lat}&lon=${lon}`)).json()
    return json.features[0].properties.formatted
}

class DB{
    async put_msg(device, timestamp, msg){
        try {
            var [results, fields] = await this.connection.query(
                'CREATE TABLE IF NOT EXISTS `gps-table` ( \
                    `device` VARCHAR(200) NOT NULL, \
                    `timestamp` INT NOT NULL, \
                    `message` TEXT, \
                    PRIMARY KEY (`device`,`timestamp`) \
                )'
            )

            var [results, fields] = await this.connection.execute(
                {sql: 'INSERT IGNORE INTO `gps-table` (`device`, `timestamp`, `message`) VALUES (?,?,?)'},
                [device, timestamp, msg]
            )

        } catch (err) {
            console.log(err)
        }  
    }

    async to_json(results, req_short=false, is_zh=false, geocoding=false){   //array like
        for(var e of results){
            const date = new Date((e.timestamp + 8*60*60)*1000)
            e.timestamp = date.toLocaleString("en-GB")
            e.message = JSON.parse(e.message)
            let m = e.message
            if(geocoding)
                e.rev_geo = await reverse_geo(m.lat, m.lon)
            if(req_short){
                var short_msg = {
                    _type: m.type,
                    lat: m.lat,
                    lon: m.lon,
                    alt: m.alt,
                    batt: m.batt,
                    bs: m.bs,
                    vel: m.vel,
                    conn: m.conn,
                    SSID: m.SSID
                }
                //remove empty entry
                short_msg = Object.fromEntries(Object.entries(short_msg).filter(([, v]) => v != null))
                e.message = short_msg
            }
            if(is_zh){
                const zh_arr = {
                    'lat': '緯度',
                    'lon': '經度',
                    'alt': '海拔',
                    'batt': '電量',
                    'bs': '電池狀態',
                    'vel': '速度',
                    'SSID': 'WiFi名字',
                    'conn': '網絡方式'
                }
                e['設備'] = e.device
                delete e.device
                e['時間'] = e.timestamp
                delete e.timestamp
                var zh_message = {}
                Object.entries(e.message).forEach(([k,v]) => {
                    if(k in zh_arr)
                        zh_message[zh_arr[k]] = v
                    else
                        zh_message[k] = v

                })
                e['信息'] = zh_message
                delete e.message
                e['地點'] = e.rev_geo
                delete e.rev_geo
            }
        }
    }

    async get_msg(device, req_short=false, geocoding=false, is_zh=false, limit=5){
        var [results, fields] = await this.connection.execute(
            {sql: 'SELECT * FROM `gps-table` WHERE `device` = ? ORDER BY `timestamp` DESC LIMIT '+limit},  //not working with ?
            [device]
        )
        //console.log(results);console.log(fields)
        await this.to_json(results, req_short, is_zh, geocoding)
        return results
    }

    async get_all_msg(req_short=false, geocoding=false, is_zh=false, limit=5){
        var [results, fields] = await this.connection.query(
            'SELECT * FROM `gps-table` ORDER BY `timestamp` DESC LIMIT ' + limit
        )
        await this.to_json(results, req_short, is_zh, geocoding)
        return results
    }

    async start_db(){
        this.pool = mysql.createPool(DB_PW_URI)
        this.connection = await this.pool.getConnection()
    }
    async end_db(){
        await this.connection.release()
    }    
}

class MQTT{
    async set_db(db){
        this.db = db
    }

    async start_mqtt(){
        this.client = mqtt.connect(MQTT_URI, {
            port: MQTT_PORT,
            username: MQTT_USER,
            password: MQTT_PW,
            clean: false,
            clientId: MQTT_CLIENT_ID
        })
    }
    async parse_json(topic, msg){
        try{
            const obj = JSON.parse(msg)
            //this.db.put_msg(msg)
            const device = topic.substring(topic.indexOf('/') + 1)
            await this.db.put_msg(device, obj.tst, msg)
            //console.log(`[${topic}][${device}]:`)
            //console.log(obj)
        }catch(e){
            console.log(`json err[${topic}]\n${e}`)
        }
    }
    async fetch_msg(){
        this.client.subscribe(
            MQTT_TOPIC,
            {qos: 1, rh: true},
            (error, granted)=>{
                if(error)
                    console.log(error)
            }
        )
        this.client.on('message', (topic, msg)=>{
            //console.log(`[${topic}]: ${msg}`)
            this.parse_json(topic, msg)
        })
        await sleep(3000)
        this.client.end()
    }
}

//shared
const db = new DB()
const mq = new MQTT()

app.get('/sync', async (req, res) => {
    if(req.query.pw !== API_KEY_SYNC)
        res.send(401, 'unauthorized')
    await db.start_db()

    mq.set_db(db)

    await mq.start_mqtt()
    await mq.fetch_msg()

    await db.end_db()
    res.send('synced')
})

app.get('/get', async (req, res)=>{
    if(req.query.pw !== API_KEY_GET)
        res.send(401, 'unauthorized')
    await db.start_db()
    const limit = req.query.n || 5
    const results = req.query.dev ? await db.get_msg(req.query.dev, req.query.short, req.query.geo, req.query.zh, limit) : await db.get_all_msg(req.query.short, req.query.geo, req.query.zh, limit)
    await db.end_db()

    if(req.query.html){
        res.type('text/plain')
        res.send(await JSON.stringify(results,null,2))
    }else
        res.json(results)
})

app.use(express.json())
app.use(express.urlencoded())
app.use(express.urlencoded({extended: true}))
app.listen(3000, () => console.log("Server ready on port 3000."))

