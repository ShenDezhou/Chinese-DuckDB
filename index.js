const fs = require("fs");
const express = require("express");
const bodyParser = require("body-parser");
var duckdb = require('duckdb');
var request = require('request')
request = request.defaults({'proxy':'http://127.0.0.1:1087'});


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

function urlMapToPath(url) {
    // url="https://huggingface.co/datasets/clue/resolve/refs%2Fconvert%2Fparquet/tnews/clue-train.parquet"
    const matches = url.match(/https:\/\/huggingface\.co\/datasets\/(\w+)\/resolve\/refs%2Fconvert%2Fparquet\/(\w+)\/([\w-]+\.parquet)/);
    console.log(matches)
    if (!fs.existsSync(matches[1]+'/'+matches[2])) {
        fs.mkdirSync(matches[1]+'/'+matches[2], {recursive: true})
    }
    return matches[1]+'/'+matches[2] +'/'+matches[3]
}

app.post("/start", (req, res) => {
    if (bot) onDisconnect("Restarting bot");
    bot = null;
    console.log(req.body);
    // bot = new duckdb.Database(':memory:');
    var db = new duckdb.Database(':memory:'); 
    bot = db.connect();
    bot.run("INSTALL httpfs;")
    bot.run("LOAD httpfs;")
    res.json({
        message: "Bot started",
    })
});

function urlMapToDSCache(url) {
    // url="https://huggingface.co/datasets/clue/blob/main/dataset_infos.json"
    const matches = url.match(/https:\/\/huggingface\.co\/datasets\/(\w+)\/raw\/main\/dataset_infos.json/);
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
    url = `https://huggingface.co/datasets/${req.body.dataset_name}/resolve/refs%2Fconvert%2Fparquet/${req.body.working_dataset_name}/${req.body.dataset_name}-${req.body.split}.parquet`
    console.log(url);
    res.json(
        {'data': url}
    )
});

app.post("/peek_parquet", (req, res) => {
    console.log(req.body);
    local_parquet = urlMapToPath(req.body.url)
    console.log(local_parquet);
    function query_parquet(local_parquet) {
        if (req.body.sql) {
            console.log(req.body.sql)
            sql = req.body.sql.replace('local_parquet', `${local_parquet}`)
        } else {
            sql = `SELECT count(*)
                   FROM '${local_parquet}' 
                   LIMIT(10)
                   `
        }
        console.log(sql);

        bot.all(sql, function(err, response) {
            if (err) {
                throw err;
            }
            console.log(response)
            res.json({
                data:response
            })
        });
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
    // import useful package
    let response_sent = false;
    console.log(req.body)

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





//const mineflayer = require("mineflayer");
//
//const skills = require("./lib/skillLoader");
//const { initCounter, getNextTime } = require("./lib/utils");
//const obs = require("./lib/observation/base");
//const OnChat = require("./lib/observation/onChat");
//const OnError = require("./lib/observation/onError");
//const { Voxels, BlockRecords } = require("./lib/observation/voxels");
//const Status = require("./lib/observation/status");
//const Inventory = require("./lib/observation/inventory");
//const OnSave = require("./lib/observation/onSave");
//const Chests = require("./lib/observation/chests");
//// require a package 'plugin' function as a name 'tool'.
//// const { plugin: tool } = require("mineflayer-tool");
//
//// START
//// const { pathfinder } = require("mineflayer-pathfinder");
//const tool = require("mineflayer-tool").plugin;
//const collectBlock = require("mineflayer-collectblock").plugin;
//const pvp = require("mineflayer-pvp").plugin;
//const minecraftHawkEye = require("minecrafthawkeye");
//// END
//
//// START
//const mineData =  require("minecraft-data")
//const {
//    Movements,
//    goals: {
//        Goal,
//        GoalBlock,
//        GoalNear,
//        GoalXZ,
//        GoalNearXZ,
//        GoalY,
//        GoalGetToBlock,
//        GoalLookAtBlock,
//        GoalBreakBlock,
//        GoalCompositeAny,
//        GoalCompositeAll,
//        GoalInvert,
//        GoalFollow,
//        GoalPlaceBlock,
//    },
//    pathfinder,
//    Move,
//    ComputedPath,
//    PartiallyComputedPath,
//    XZCoordinates,
//    XYZCoordinates,
//    SafeBlock,
//    GoalPlaceBlockOptions,
//} = require("mineflayer-pathfinder");
//const { Vec3 } = require("vec3");
//// END
//
//// BEGIN VIEWER
//const mineflayerViewer=require('prismarine-viewer').mineflayer
//// END
//
//// BEGIN FUNCTION
//function onConnectionFailed(e) {
//    console.log(e.message);
//    bot = null;
//    res.status(400).json({ error: e });
//}
//
//function onDisconnect(message) {
//    console.log(message);
//    if (bot.viewer) {
//        bot.viewer.close();
//    }
//    bot.end();
//    bot = null;
//}
//// END
//
//// BEGIN FUNCTION
//function otherError(err) {
//    console.log("Uncaught Error" + err.message);
//    bot.emit("error", handleError(err));
//    bot.waitForTicks(bot.waitTicks).then(() => {
//        if (!response_sent) {
//            response_sent = true;
//            res.json(bot.observe());
//        }
//    });
//}
//
//function onTick() {
//    bot.globalTickCounter++;
//    if (bot.pathfinder.isMoving()) {
//        bot.stuckTickCounter++;
//        if (bot.stuckTickCounter >= 100) {
//            onStuck(1.5);
//            bot.stuckTickCounter = 0;
//        }
//    }
//}
//
//async function evaluateCode(code, programs) {
//    // Echo the code produced for players to see it. Don't echo when the bot code is already producing dialog or it will double echo
//    try {
//        await eval("(async () => {" + programs + "\n" + code + "})()");
//        // await eval(`(async () => {${programs} \n ${code} })()`);
//        return "success";
//    } catch (err) {
//        return err;
//    }
//}
//
//function onStuck(posThreshold) {
//    const currentPos = bot.entity.position;
//    bot.stuckPosList.push(currentPos);
//
//    // Check if the list is full
//    if (bot.stuckPosList.length === 5) {
//        const oldestPos = bot.stuckPosList[0];
//        const posDifference = currentPos.distanceTo(oldestPos);
//
//        if (posDifference < posThreshold) {
//            teleportBot(); // execute the function
//        }
//
//        // Remove the oldest time from the list
//        bot.stuckPosList.shift();
//    }
//}
//
//function teleportBot() {
//    const blocks = bot.findBlocks({
//        matching: (block) => {
//            return block.type === 0;
//        },
//        maxDistance: 1,
//        count: 27,
//    });
//
//    if (blocks) {
//        const randomIndex = Math.floor(Math.random() * blocks.length);
//        const block = blocks[randomIndex];
//        bot.chat(`/tp @s ${block.x} ${block.y} ${block.z}`);
//    } else {
//        bot.chat("/tp @s ~ ~1.25 ~");
//    }
//}
//
//function returnItems(mcData) {
//    bot.chat("/gamerule doTileDrops false");
//    const crafting_table = bot.findBlock({
//        matching: mcData.blocksByName.crafting_table.id,
//        maxDistance: 128,
//    });
//    if (crafting_table) {
//        bot.chat(
//            `/setblock ${crafting_table.position.x} ${crafting_table.position.y} ${crafting_table.position.z} air destroy`
//        );
//        bot.chat("/give @s crafting_table");
//    }
//    const furnace = bot.findBlock({
//        matching: mcData.blocksByName.furnace.id,
//        maxDistance: 128,
//    });
//    if (furnace) {
//        bot.chat(
//            `/setblock ${furnace.position.x} ${furnace.position.y} ${furnace.position.z} air destroy`
//        );
//        bot.chat("/give @s furnace");
//    }
//    if (bot.inventoryUsed() >= 32) {
//        // if chest is not in bot's inventory
//        if (!bot.inventory.items().find((item) => item.name === "chest")) {
//            bot.chat("/give @s chest");
//        }
//    }
//    // if iron_pickaxe not in bot's inventory and bot.iron_pickaxe
//    if (
//        bot.iron_pickaxe &&
//        !bot.inventory.items().find((item) => item.name === "iron_pickaxe")
//    ) {
//        bot.chat("/give @s iron_pickaxe");
//    }
//    bot.chat("/gamerule doTileDrops true");
//}
//
//function handleError(err, programs="", code="") {
//    let stack = err.stack;
//    if (!stack) {
//        return err;
//    }
//    console.log(stack);
//    const final_line = stack.split("\n")[1];
//    const regex = /<anonymous>:(\d+):\d+\)/;
//
//    const programs_length = programs.split("\n").length;
//    let match_line = null;
//    for (const line of stack.split("\n")) {
//        const match = regex.exec(line);
//        if (match) {
//            const line_num = parseInt(match[1]);
//            if (line_num >= programs_length) {
//                match_line = line_num - programs_length;
//                break;
//            }
//        }
//    }
//    if (!match_line) {
//        return err.message;
//    }
//    let f_line = final_line.match(
//        /\((?<file>.*):(?<line>\d+):(?<pos>\d+)\)/
//    );
//    if (f_line && f_line.groups && fs.existsSync(f_line.groups.file)) {
//        const { file, line, pos } = f_line.groups;
//        const f = fs.readFileSync(file, "utf8").split("\n");
//        // let filename = file.match(/(?<=node_modules\\)(.*)/)[1];
//        let source = file + `:${line}\n${f[line - 1].trim()}\n `;
//
//        const code_source =
//            "at " +
//            code.split("\n")[match_line - 1].trim() +
//            " in your code";
//        return source + err.message + "\n" + code_source;
//    } else if (
//        f_line &&
//        f_line.groups &&
//        f_line.groups.file.includes("<anonymous>")
//    ) {
//        const { file, line, pos } = f_line.groups;
//        let source =
//            "Your code" +
//            `:${match_line}\n${code.split("\n")[match_line - 1].trim()}\n `;
//        let code_source = "";
//        if (line < programs_length) {
//            source =
//                "In your program code: " +
//                programs.split("\n")[line - 1].trim() +
//                "\n";
//            code_source = `at line ${match_line}:${code
//                .split("\n")
//                [match_line - 1].trim()} in your code`;
//        }
//        return source + err.message + "\n" + code_source;
//    }
//    return err.message;
//}
// END
