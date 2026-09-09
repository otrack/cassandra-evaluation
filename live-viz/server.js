const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Tail } = require('tail');
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const cassandra = require('cassandra-driver');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const slowMode = process.env.SLOW_MODE !== 'false'; // default true
let isInterleaved = true;
let isPaused = false;
let globalEventQueue = [];
let pendingTxs = {}; // coordinatorIp -> txObject
let txCounter = 0;

// Fake database state
let fakeBalances = {}; // y_id -> balance
let dcStates = {}; // dcName -> [{y_id, field0}]
let dbInitialized = false;

app.use(express.static(path.join(__dirname, 'public')));
app.use('/robots', express.static(path.join(__dirname, 'robots')));

// read robots
const robotsDir = path.join(__dirname, 'robots');
let robotFiles = [];
if (fs.existsSync(robotsDir)) {
  robotFiles = fs.readdirSync(robotsDir).filter(f => f.endsWith('.png') || f.endsWith('.jpg') || f.endsWith('.jpeg'));
}

// read latencies.csv
const csvPath = fs.existsSync('/app/latencies.csv') ? '/app/latencies.csv' : path.join(__dirname, '..', 'latencies.csv');
let locations = {};
if (fs.existsSync(csvPath)) {
  const content = fs.readFileSync(csvPath, 'utf8');
  const records = parse(content, { columns: true, skip_empty_lines: true });
  records.forEach(r => {
    locations[r.loc] = [parseFloat(r.lon), parseFloat(r.lat)];
  });
}

const logsDir = path.join(__dirname, 'logs', 'demo');
const tails = {};
const finishedFiles = new Set();
const datacenterMap = {};
let dbClient = null;
let dbClientConnecting = false;
let dbConnected = false;

// Regex patterns
const dcRegex = /INFO site\.ycsb\.db\.CassandraCQLClient - Datacenter: (\w+); Host: [^\/]*\/?([\d\.]+):\d+;/;
const sendRegex = /\[(\d+)\] Sending (\w+) message to [^\/]*\/?([\d\.]+):\d+ message size \d+ bytes @ [^\/]*\/?([\d\.]+)/;
const recvRegex = /\[(\d+)\] (\w+) message received from [^\/]*\/?([\d\.]+):\d+ @ [^\/]*\/?([\d\.]+)/;
const transUpdate1Regex = /UPDATE usertable SET field0 -= 1 WHERE y_id = '([^']+)';/;
const transUpdate2Regex = /UPDATE usertable SET field0 \+= 1 WHERE y_id = '([^']+)';/;
const prepRegex = /\[(\d+)\] Preparing statement @ [^\/]*\/?([\d\.]+)/;
const traceRegex = /Trace ID: [^,]+, type: [^,]+, duration: (\d+)us/;
const localExecRegex = /\[(\d+)\] Local Execute for (\[\[[^\]]+\]\])/;
const localReqRegex = /\[(\d+)\] Local (PreAccept|Accept) for/;

let fileStates = {};

async function initDBState() {
  if (!dbClient || !dbConnected || dbInitialized) return false;
  
  try {
    const query = 'SELECT y_id FROM ycsb.usertable';
    // Use SERIAL consistency for a strongly consistent/serializable read
    const rs = await dbClient.execute(query, [], { consistency: cassandra.types.consistencies.serial });
    const users = rs.rows.map(r => r.y_id);
    
    if (users.length > 0) {
      console.log(`\n[Init] DB Populated. Fetched ${users.length} users with SERIAL consistency.`);
      users.forEach(uid => {
          fakeBalances[uid] = 100;
      });
      dbInitialized = true;
      broadcastState();

      // Fetch weakly consistent state per DC
      const dcs = [...new Set(Object.values(datacenterMap))];
      for (const dc of dcs) {
          const ip = Object.keys(datacenterMap).find(key => datacenterMap[key] === dc);
          console.log(`[Init] Fetching weakly consistent state for DC ${dc} via ${ip}`);
          const tempClient = new cassandra.Client({ 
              contactPoints: [ip], 
              localDataCenter: dc, 
              keyspace: 'ycsb' 
          });
          try {
              await tempClient.connect();
              const rsOne = await tempClient.execute('SELECT y_id FROM usertable', [], { consistency: cassandra.types.consistencies.one });
              dcStates[dc] = [...new Set(rsOne.rows.map(r => r.y_id))];
              console.log(`[Init] DC ${dc} keys:`, dcStates[dc]);
              await tempClient.shutdown();
          } catch (e) {
              console.error(`[Init] Error fetching state for DC ${dc}:`, e.message);
          }
      }
      io.emit('dc_states', dcStates);

      return true;
    }
  } catch (e) {
    if (!e.message.includes('table') && !e.message.includes('keyspace') && !e.message.includes('unconfigured')) {
      console.error('\n[Init] DB Error:', e.message);
    }
  }
  return false;
}

function getBalancesArray() {
    return Object.keys(fakeBalances).map(uid => ({
        y_id: uid,
        field0: fakeBalances[uid]
    }));
}

function broadcastState() {
    io.emit('db_state', getBalancesArray());
}

function updateFakeBalances(tx) {
    if (fakeBalances[tx.from] !== undefined) fakeBalances[tx.from] -= 1;
    if (fakeBalances[tx.to] !== undefined) fakeBalances[tx.to] += 1;
    broadcastState();
}

function handlePlaybackEvent(event) {
    if (event.type === 'transfer_start') io.emit('transfer_start', event.data);
    if (event.type === 'message_flow') io.emit('message_flow', event.data);
    if (event.type === 'transfer_complete') {
        io.emit('transfer_complete', event.data);
        updateFakeBalances(event.data);
    }
}

if (slowMode) {
  (async () => {
    console.log('Slow mode playback starting. Waiting for log files...');
    while (Object.keys(tails).length === 0) {
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log(`Log files detected. Waiting for DB connection and users...`);
    while (!dbInitialized) {
      if (dbConnected) {
          await initDBState();
      }
      if (!dbInitialized) {
          await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }

    console.log('Starting transaction playback loop...');

    while (true) {
      if (isPaused) {
        await new Promise(resolve => setTimeout(resolve, 500));
        continue;
      }
      if (globalEventQueue.length > 0) {
        globalEventQueue.sort((a, b) => a.timestamp - b.timestamp);

        if (isInterleaved) {
            const event = globalEventQueue.shift();
            handlePlaybackEvent(event);
            await new Promise(resolve => setTimeout(resolve, 800));
        } else {
            const startIdx = globalEventQueue.findIndex(e => e.type === 'transfer_start');
            if (startIdx === -1) {
                if (finishedFiles.size > 0 && finishedFiles.size === Object.keys(tails).length && globalEventQueue.length === 0) {
                    console.log('All transactions replayed. Exiting.');
                    process.exit(0);
                }
                await new Promise(resolve => setTimeout(resolve, 500));
                continue;
            }

            const startEvent = globalEventQueue[startIdx];
            const txId = startEvent.data.id;
            const txEvents = globalEventQueue.filter(e => e.data && e.data.id === txId);
            globalEventQueue = globalEventQueue.filter(e => !e.data || e.data.id !== txId);

            for (const event of txEvents) {
                handlePlaybackEvent(event);
                if (event.type === 'transfer_start') await new Promise(resolve => setTimeout(resolve, 1500));
                else if (event.type === 'message_flow') await new Promise(resolve => setTimeout(resolve, 1000));
                else if (event.type === 'transfer_complete') await new Promise(resolve => setTimeout(resolve, 2500));
            }
        }
      } else {
        if (finishedFiles.size > 0 && finishedFiles.size === Object.keys(tails).length && Object.keys(pendingTxs).length === 0) {
            console.log('No more events and all logs finished. Exiting in 5s...');
            await new Promise(resolve => setTimeout(resolve, 5000));
            process.exit(0);
        }
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
  })();
}

function pushPendingTx(coordIp) {
  if (pendingTxs[coordIp]) {
    const tx = pendingTxs[coordIp];
    const quorumSet = new Set();
    
    // Always include coordinator DC
    const coordDC = tx.coordinatorDC || datacenterMap[tx.coordinator];
    if (coordDC) quorumSet.add(coordDC);

    // Consider all responses as part of the quorum
    tx.replies.forEach(r => {
        const dc = datacenterMap[r.source];
        if (dc) quorumSet.add(dc);
    });

    tx.quorum = Array.from(quorumSet);
    
    const lastMsgTs = tx.messages.length > 0 ? tx.messages[tx.messages.length - 1].timestamp : tx.timestamp;
    
    globalEventQueue.push({
        type: 'transfer_complete',
        timestamp: lastMsgTs + 1,
        data: tx
    });
    
    delete pendingTxs[coordIp];
  }
}

setInterval(() => {
    const now = Date.now();
    Object.keys(pendingTxs).forEach(ip => {
        if (now - pendingTxs[ip].lastActivityRealTime > 5000) {
            pushPendingTx(ip);
        }
    });
}, 1000);

function isSimplifiedProtocolMsg(type) {
    const t = type.toUpperCase();
    return t.includes('PRE_ACCEPT') || t.includes('PREACCEPT') || t.includes('SIMPLE_RSP') || 
           (t.includes('ACCEPT') && !t.includes('PRE_ACCEPT') && !t.includes('PREACCEPT'));
}

function processLine(line, filePath) {
  if (line.includes('[OVERALL], RunTime(ms)')) {
    finishedFiles.add(filePath);
    Object.keys(pendingTxs).forEach(ip => {
        if (filePath.includes(datacenterMap[ip])) {
            pushPendingTx(ip);
        }
    });
    return;
  }

  if (!fileStates[filePath]) {
    fileStates[filePath] = { transFrom: null, transTo: null, lastTimestamp: 0, pendingDuration: null };
  }
  const state = fileStates[filePath];

  let tsMatch = line.match(/\[(\d+)\]/);
  if (tsMatch) state.lastTimestamp = parseInt(tsMatch[1]);

  let dcMatch = line.match(dcRegex);
  if (dcMatch) {
    const dcName = dcMatch[1], ip = dcMatch[2];
    if (datacenterMap[ip] !== dcName) {
      datacenterMap[ip] = dcName;
      io.emit('datacenter_mapping', datacenterMap);
      if (!dbClient && !dbClientConnecting) {
        dbClientConnecting = true;
        const connectDB = () => {
          dbClient = new cassandra.Client({ contactPoints: [ip], localDataCenter: dcName, keyspace: 'ycsb' });
          dbClient.connect().then(() => {
            console.log('Connected to Cassandra at', ip);
            dbConnected = true; dbClientConnecting = false;
          }).catch(e => {
            dbClient = null; dbClientConnecting = false;
            setTimeout(connectDB, 5000);
          });
        };
        connectDB();
      }
    }
    return;
  }

  let u1 = line.match(transUpdate1Regex);
  if (u1) { state.transFrom = u1[1]; return; }
  let u2 = line.match(transUpdate2Regex);
  if (u2) { state.transTo = u2[1]; return; }

  let traceMatch = line.match(traceRegex);
  if (traceMatch) {
    const durationUs = parseInt(traceMatch[1]);
    state.pendingDuration = (durationUs / 1000.0).toFixed(2);
    return;
  }

  let prepMatch = line.match(prepRegex);
  if (prepMatch && state.transFrom && state.transTo) {
    const coordIp = prepMatch[2];
    pushPendingTx(coordIp);
    txCounter++;

    const tx = {
      id: txCounter,
      label: `TX #${txCounter}: ${state.transFrom} -> ${state.transTo}`,
      from: state.transFrom,
      to: state.transTo,
      coordinator: coordIp,
      coordinatorDC: datacenterMap[coordIp],
      timestamp: parseInt(prepMatch[1]),
      duration: state.pendingDuration,
      messages: [],
      replies: [],
      localExecTs: Infinity,
      hasLoopback: false,
      isFastPath: true,
      lastActivityRealTime: Date.now()
    };
    
    state.pendingDuration = null;
    pendingTxs[coordIp] = tx;
    globalEventQueue.push({
        type: 'transfer_start',
        timestamp: tx.timestamp,
        data: tx
    });

    state.transFrom = null;
    state.transTo = null;
    return;
  }

  let execMatch = line.match(localExecRegex);
  if (execMatch) {
    const ts = parseInt(execMatch[1]);
    Object.keys(pendingTxs).forEach(ip => {
        if (filePath.includes(datacenterMap[ip])) {
            pendingTxs[ip].localExecTs = ts;
            pendingTxs[ip].lastActivityRealTime = Date.now();
        }
    });
  }

  let localReqMatch = line.match(localReqRegex);
  if (localReqMatch) {
      Object.keys(pendingTxs).forEach(ip => {
          if (filePath.includes(datacenterMap[ip])) {
              pendingTxs[ip].hasLoopback = true;
              if (localReqMatch[2] === 'Accept') {
                  pendingTxs[ip].isFastPath = false;
              }
              pendingTxs[ip].lastActivityRealTime = Date.now();
          }
      });
  }

  let sendMatch = line.match(sendRegex);
  if (sendMatch) {
    const msgType = sendMatch[2].toUpperCase();
    if (!isSimplifiedProtocolMsg(msgType)) return;
    const isRsp = msgType.includes('RSP') || msgType.includes('REPLY') || msgType.includes('SIMPLE') || msgType.includes('OK');
    if (isRsp) return; 

    const srcIp = sendMatch[4], dstIp = sendMatch[3], ts = parseInt(sendMatch[1]);
    if (srcIp === dstIp) {
        if (pendingTxs[srcIp]) {
            pendingTxs[srcIp].hasLoopback = true;
            if (msgType.includes('ACCEPT') && !msgType.includes('PRE')) {
                pendingTxs[srcIp].isFastPath = false;
            }
        }
        return;
    }

    if (pendingTxs[srcIp]) {
      const msg = { type: sendMatch[2], source: srcIp, target: dstIp, timestamp: ts, id: pendingTxs[srcIp].id };
      pendingTxs[srcIp].messages.push(msg);
      if (msgType.includes('ACCEPT') && !msgType.includes('PRE')) {
          pendingTxs[srcIp].isFastPath = false;
      }
      pendingTxs[srcIp].lastActivityRealTime = Date.now();
      globalEventQueue.push({ type: 'message_flow', timestamp: ts, data: msg });
    }
    return;
  }

  let recvMatch = line.match(recvRegex);
  if (recvMatch) {
    const msgType = recvMatch[2].toUpperCase();
    if (!isSimplifiedProtocolMsg(msgType)) return;
    const isRsp = msgType.includes('RSP') || msgType.includes('REPLY') || msgType.includes('SIMPLE') || msgType.includes('OK');
    if (!isRsp) return; 

    const srcIp = recvMatch[3], dstIp = recvMatch[4], ts = parseInt(recvMatch[1]);
    if (srcIp === dstIp) return;

    if (pendingTxs[dstIp]) {
        const msg = { type: recvMatch[2], source: srcIp, target: dstIp, timestamp: ts, id: pendingTxs[dstIp].id };
        pendingTxs[dstIp].messages.push(msg);
        if (msgType.includes('ACCEPT') && !msgType.includes('PRE')) {
            pendingTxs[dstIp].isFastPath = false;
        }
        pendingTxs[dstIp].replies.push({ source: srcIp, timestamp: ts });
        pendingTxs[dstIp].lastActivityRealTime = Date.now();
        globalEventQueue.push({ type: 'message_flow', timestamp: ts, data: msg });
    }
  }
}

function watchLogsDir() {
  if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });
  const checkFiles = () => {
    fs.readdir(logsDir, (err, files) => {
      if (err) return;
      files.forEach(filename => {
        if (filename.startsWith('accord') && filename.endsWith('.dat')) {
          const filePath = path.join(logsDir, filename);
          if (!tails[filePath]) {
            try {
              const tail = new Tail(filePath, { fromBeginning: true });
              tail.on('line', (line) => processLine(line, filePath));
              tails[filePath] = tail;
              console.log(`Started tailing ${filename}`);
            } catch (e) { console.error(`Failed to tail ${filename}:`, e); }
          }
        }
      });
    });
  };
  checkFiles();
  fs.watch(logsDir, (eventType, filename) => {
    if (filename && filename.startsWith('accord') && filename.endsWith('.dat')) checkFiles();
  });
}

io.on('connection', (socket) => {
  socket.emit('datacenter_mapping', datacenterMap);
  socket.emit('locations', locations);
  socket.emit('robot_images', robotFiles);
  socket.emit('slow_mode', slowMode);
  socket.emit('interleaved_mode', isInterleaved);
  socket.emit('paused_mode', isPaused);
  socket.emit('dc_states', dcStates);
  
  socket.on('toggle_interleaved', () => {
      isInterleaved = !isInterleaved;
      io.emit('interleaved_mode', isInterleaved);
  });

  socket.on('toggle_pause', () => {
      isPaused = !isPaused;
      io.emit('paused_mode', isPaused);
  });

  if (dbInitialized) {
      socket.emit('db_state', getBalancesArray());
  }
});

server.listen(3000, () => {
  console.log('Server listening on *:3000');
  watchLogsDir();
});
