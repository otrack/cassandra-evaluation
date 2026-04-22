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

const slowMode = process.env.SLOW_MODE === 'true';
const eventQueue = [];

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
const datacenterMap = {};
let dbClient = null;
let dbClientConnecting = false;

// Regex patterns
const dcRegex = /INFO site\.ycsb\.db\.CassandraCQLClient - Datacenter: (\w+); Host: [^\/]*\/?([\d\.]+):\d+;/;
const sendRegex = /\[(\d+)\] Sending (\w+) message to [^\/]*\/?([\d\.]+):\d+ message size \d+ bytes @ [^\/]*\/?([\d\.]+)/;
const transUpdate1Regex = /UPDATE usertable SET field0 -= 1 WHERE y_id = '([^']+)';/;
const transUpdate2Regex = /UPDATE usertable SET field0 \+= 1 WHERE y_id = '([^']+)';/;

let transFrom = null;

function emitEvent(name, data) {
  if (slowMode) {
    eventQueue.push({ name, data });
  } else {
    io.emit(name, data);
  }
}

if (slowMode) {
  setInterval(() => {
    if (eventQueue.length > 0) {
      const { name, data } = eventQueue.shift();
      io.emit(name, data);
    }
  }, 250); // Emit an event every 250ms in slow mode
}

function processLine(line) {
  let dcMatch = line.match(dcRegex);
  if (dcMatch) {
    const dcName = dcMatch[1];
    const ip = dcMatch[2];
    if (datacenterMap[ip] !== dcName) {
      datacenterMap[ip] = dcName;
      io.emit('datacenter_mapping', datacenterMap);
      
      // connect db
      if (!dbClient && !dbClientConnecting) {
        dbClientConnecting = true;
        dbClient = new cassandra.Client({
          contactPoints: [ip],
          localDataCenter: dcName,
          keyspace: 'ycsb'
        });
        dbClient.connect()
          .then(() => console.log('Connected to Cassandra at', ip))
          .catch(e => {
             console.error('Failed to connect to Cassandra:', e);
             dbClient = null;
             dbClientConnecting = false;
          });
      }
    }
    return;
  }

  let sendMatch = line.match(sendRegex);
  if (sendMatch) {
    const timestamp = parseInt(sendMatch[1]);
    const msgType = sendMatch[2];
    const destIp = sendMatch[3];
    const srcIp = sendMatch[4];

    emitEvent('message_flow', {
      timestamp,
      type: msgType,
      source: srcIp,
      target: destIp
    });
    return;
  }

  let u1 = line.match(transUpdate1Regex);
  if (u1) {
    transFrom = u1[1];
    return;
  }

  let u2 = line.match(transUpdate2Regex);
  if (u2 && transFrom) {
    emitEvent('transaction', { from: transFrom, to: u2[1] });
    transFrom = null;
  }
}

function watchLogsDir() {
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }

  const checkFiles = () => {
    fs.readdir(logsDir, (err, files) => {
      if (err) return;
      files.forEach(filename => {
        if (filename.startsWith('accord') && filename.endsWith('.dat')) {
          const filePath = path.join(logsDir, filename);
          if (!tails[filePath]) {
            try {
              const tail = new Tail(filePath, { fromBeginning: true });
              tail.on('line', processLine);
              tail.on('error', (error) => console.error('Tail error:', error));
              tails[filePath] = tail;
              console.log(`Started tailing ${filename}`);
            } catch (e) {
              console.error(`Failed to tail ${filename}:`, e);
            }
          }
        }
      });
    });
  };

  checkFiles();

  fs.watch(logsDir, (eventType, filename) => {
    if (filename && filename.startsWith('accord') && filename.endsWith('.dat')) {
      checkFiles();
    }
  });
}

// DB Poll interval
setInterval(async () => {
  if (dbClient) {
    try {
      const rs = await dbClient.execute('SELECT y_id, field0 FROM ycsb.usertable', [], { consistency: cassandra.types.consistencies.one });
      const balances = rs.rows.map(r => ({ y_id: r.y_id, field0: r.field0 }));
      io.emit('db_state', balances);
    } catch (e) {
      console.error('DB fetch error:', e);
    }
  }
}, 1000);

io.on('connection', (socket) => {
  console.log('Client connected');
  socket.emit('datacenter_mapping', datacenterMap);
  socket.emit('locations', locations);
  socket.emit('robot_images', robotFiles);
  socket.emit('slow_mode', slowMode);
});

server.listen(3000, () => {
  console.log('Server listening on *:3000');
  watchLogsDir();
});
