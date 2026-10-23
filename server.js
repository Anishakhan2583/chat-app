// server.js
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const mongoose = require('mongoose');
const path = require('path');
const Message = require('./models/Message');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/chatapp';

mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 5000 })
  .then(() => console.log('✓ MongoDB Connected Successfully'))
  .catch(err => console.error('⚠️ MongoDB Connection Error:', err.message));

app.use(express.static(path.join(__dirname, 'public')));

const activeUsers = new Map(); // ws -> username

function broadcastUserStatus() {
  const usersList = Array.from(activeUsers.values());
  const statusData = JSON.stringify({
    type: 'userStatus',
    count: wss.clients.size,
    users: usersList
  });

  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(statusData);
    }
  });
}

function sendToUser(targetUsername, data) {
  for (let [client, username] of activeUsers.entries()) {
    if (username === targetUsername && client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
      break;
    }
  }
}

wss.on('connection', async (ws) => {
  try {
    const recentMessages = await Message.find().sort({ timestamp: 1 }).limit(50);
    ws.send(JSON.stringify({ type: 'history', data: recentMessages }));
  } catch (err) {
    console.error('Error fetching history:', err);
  }

  broadcastUserStatus();

  ws.on('message', async (rawData) => {
    try {
      const parsed = JSON.parse(rawData);

      if (parsed.type === 'join' && parsed.username) {
        activeUsers.set(ws, parsed.username);
        broadcastUserStatus();
      }

      if (parsed.type === 'typing') {
        wss.clients.forEach((client) => {
          if (client !== ws && client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({
              type: 'typing',
              username: parsed.username,
              isTyping: parsed.isTyping
            }));
          }
        });
      }

      if (parsed.type === 'message' && parsed.username && parsed.text) {
        const newMessage = new Message({ username: parsed.username, text: parsed.text });
        await newMessage.save();

        wss.clients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'message', data: newMessage }));
          }
        });
      }

      // WebRTC Signaling Channels
      if (parsed.type === 'call-user') sendToUser(parsed.to, { type: 'call-made', offer: parsed.offer, from: parsed.from });
      if (parsed.type === 'make-answer') sendToUser(parsed.to, { type: 'answer-made', answer: parsed.answer, from: parsed.from });
      if (parsed.type === 'ice-candidate') sendToUser(parsed.to, { type: 'ice-candidate', candidate: parsed.candidate });
      if (parsed.type === 'end-call') sendToUser(parsed.to, { type: 'end-call' });

    } catch (err) {
      console.error('Error handling packet:', err);
    }
  });

  ws.on('close', () => {
    activeUsers.delete(ws);
    broadcastUserStatus();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server is running on http://localhost:${PORT}`));
