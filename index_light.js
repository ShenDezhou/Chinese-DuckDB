const fs = require('fs');
var {globSync} = require("glob");
const express = require("express");
const bodyParser = require("body-parser");
const duckdb_url = "http://127.0.0.1:8000/sql";
var request = require('request')
// request = request.defaults({'proxy':'http://127.0.0.1:1087'});


let bot = null;

const app = express();

app.use(bodyParser.json({ limit: "50mb" }));
// urlencoded pass key-value pairs, then use `querystring` library to parse them.
app.use(bodyParser.urlencoded({ limit: "50mb", extended: false }));

function handleError(err) {
    console.log(err)
}


function onDisconnect(message) {
    console.log(message);
    bot = null;
}


function observe() {
    const all = fs.readdirSync(".", {withFileTypes: true})
    const folders = all.filter(i=>i.isDirectory())
                .filter(i=>!i.name.startsWith(".") && !i.name.startsWith("_"))
                .filter(i=>!i.name.startsWith('dicache') && !i.name.startsWith('node_modules') && !i.name.startsWith('recycle')).map(i=> i.name)
    observation = {}
    for (var dir of folders){
        const results =  globSync(`${dir}/*/**`)
        observation[dir] = results
        if (results.length == 0) {
            const results =  globSync(`${dir}/*`)
            observation[dir] = results
        }
    }
    return observation
}

function urlMapToPath(url) {
    // url="https://huggingface.co/datasets/clue/resolve/refs%2Fconvert%2Fparquet/tnews/clue-train.parquet"
    const matches = url.match(/https:\/\/huggingface\.co\/datasets\/([-\w]+)\/resolve\/refs%2Fconvert%2Fparquet\/([\.-\w]+)\/([\w-]+\.parquet)/);
    console.log(matches)
    if (!fs.existsSync(matches[1]+'/'+matches[2])) {
        fs.mkdirSync(matches[1]+'/'+matches[2], {recursive: true})
    }
    return matches[1]+'/'+matches[2] +'/'+matches[3]
}

app.post("/start", (req, res) => {
    if (bot) onDisconnect("Restarting bot");
    console.log(req.body);
    // var db = new duckdb.Database(':memory:'); 
    // bot = db.connect();
    // bot.run("INSTALL httpfs;")
    // bot.run("LOAD httpfs;")
    res.json({
        message: "Bot started",
        status: observe()
    })
});

function urlMapToDSCache(url) {
    // url="https://huggingface.co/datasets/clue/blob/main/dataset_infos.json"
    const matches = url.match(/https:\/\/huggingface\.co\/datasets\/([-\w]+)\/raw\/main\/dataset_infos.json/);
    console.log(matches)
    if (!fs.existsSync('dicache/'+matches[1])) {
        fs.mkdirSync('dicache/'+matches[1], {recursive: true})
    }
    return 'dicache/'+matches[1] +'/dataset_infos.json'
}

app.post("/query_subdb", (req, res) => {
    
    console.log(req.body);
    // https://huggingface.co/datasets/clue/raw/main/dataset_infos.json
    url = `https://huggingface.co/datasets/${req.body.dataset_name}/raw/main/dataset_infos.json`
    console.log(url);
    local_dijson = urlMapToDSCache(url)
    if (fs.existsSync(local_dijson)) {
        fs.readFile(local_dijson, {encoding: 'utf8'}, (err, data) => {
            if (err) throw err;
            console.log(data);
            const result = JSON.parse(data);
            res.json(
                {'data': Object.keys(result)}
            )
        });
        return
    }
    request(url, function(err, response, body) {
        console.log(response);
        console.log(body);
        fs.writeFileSync(local_dijson, body)
        console.log(`file ${local_dijson} wrote.`)
        const result = JSON.parse(body);
        res.json(
            {'data': Object.keys(result)}
        )
    })
});

app.post("/query_subdb_split", (req, res) => {
    console.log(req.body);
    if (req.body.total_part !== undefined && req.body.current_part !== undefined ) {
        const current_part = req.body.current_part.toString().padStart(5, "0")
        const total_part = req.body.total_part.toString().padStart(5, "0")
        url = `https://huggingface.co/datasets/${req.body.dataset_name}/resolve/refs%2Fconvert%2Fparquet/${req.body.working_dataset_name}/${req.body.dataset_name}-${req.body.split}-${current_part}-of-${total_part}.parquet`
    } else {
        url = `https://huggingface.co/datasets/${req.body.dataset_name}/resolve/refs%2Fconvert%2Fparquet/${req.body.working_dataset_name}/${req.body.dataset_name}-${req.body.split}.parquet`    
    }
    console.log(url);
    res.json(
        {'data': url}
    )
});

app.post("/peek_parquet", (req, res) => {
    console.log(req.body);
    if (!req.body.url) {
        res.json({
            status: observe()
        })
        return
    }
    local_parquet = urlMapToPath(req.body.url)
    console.log(local_parquet);
    function query_parquet(local_parquet) {
        var data = {
            "sql": req.body.sql,
            "table": local_parquet,
            "return_fmt": "polar"
        }
        request.post({url: duckdb_url,
            body:data,
            headers: { "content-type": "application/json"},
            json: true}, 
            function(err, response, body) {
                if (err) {
                    throw err;
                }
                console.log(response)
                res.json({
                    data:body['data'],
                    status: observe()
                })
        })
        
    }
    if (fs.existsSync(local_parquet)) {
        query_parquet(local_parquet);
        return
    }
    console.log(req.body.url)
    request.get({url:req.body.url, encoding: null}, function(err, response, body) {
        console.log(response);
        console.log(body);
        fs.writeFileSync(local_parquet, body)
        console.log(`file ${local_parquet} wrote.`)
        query_parquet(local_parquet);
    })
});

app.post("/step", async (req, res) => {
    console.log(req.body);
    if (!req.body.url) {
        res.json({
            status: observe()
        })
        return
    }
    local_parquet = urlMapToPath(req.body.url)
    console.log(local_parquet);
    function query_parquet(local_parquet) {
        var data = {
            "sql": req.body.sql,
            "table": local_parquet,
            "return_fmt": "polar"
        }
        request.post({url: duckdb_url,
            body:data,
            headers: { "content-type": "application/json"},
            json: true}, 
            function(err, response, body) {
                if (err) {
                    throw err;
                }
                console.log(response)
                res.json({
                    data:body['data'],
                    status: observe()
                })
        })
    }
    if (fs.existsSync(local_parquet)) {
        query_parquet(local_parquet);
        return
    }
    console.log(req.body.url)
    request.get({url:req.body.url, encoding: null}, function(err, response, body) {
        console.log(response);
        console.log(body);
        fs.writeFileSync(local_parquet, body)
        console.log(`file ${local_parquet} wrote.`)
        query_parquet(local_parquet);
    })
});

app.post("/stop", (req, res) => {
    bot = null;
    res.json({
        message: "Bot stopped",
    });
});

app.post("/pause", (req, res) => {
    if (!bot) {
        res.status(400).json({ error: "Bot not spawned" });
        return;
    }
    res.json({ message: "Success" });
});

// Server listening to PORT 3000

const DEFAULT_PORT = 3000;
const PORT = process.argv[2] || DEFAULT_PORT;
app.listen(PORT, () => {
    console.log(`Server started on port ${PORT}`);
});
