// src/shared/relay.ts
function readPairing(hash) {
  const params = new URLSearchParams(hash.replace(/^#/, ""));
  const relayUrl = params.get("u") ?? "";
  const relayKey = params.get("a") ?? "";
  const room = params.get("r") ?? "";
  const secret = params.get("k") ?? "";
  if (!/^https:\/\//.test(relayUrl) || !relayKey || room.length < 16 || secret.length < 40) return null;
  return { relayUrl, relayKey, room, secret };
}
async function importSecret(secret) {
  return crypto.subtle.importKey("raw", fromBase64Url(secret), "AES-GCM", false, ["encrypt", "decrypt"]);
}
async function seal(key, value) {
  const iv = new Uint8Array(12);
  crypto.getRandomValues(iv);
  const plain = new TextEncoder().encode(JSON.stringify(value));
  const ct = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, plain);
  return { iv: toBase64Url(iv), ct: toBase64Url(new Uint8Array(ct)) };
}
async function open(key, sealed) {
  if (!sealed || typeof sealed !== "object") return null;
  const { iv, ct } = sealed;
  if (typeof iv !== "string" || typeof ct !== "string") return null;
  try {
    const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: fromBase64Url(iv) }, key, fromBase64Url(ct));
    return JSON.parse(new TextDecoder().decode(plain));
  } catch {
    return null;
  }
}
function toBase64Url(data) {
  let binary = "";
  for (const byte of data) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function fromBase64Url(text) {
  const padded = text.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((text.length + 3) % 4);
  const binary = atob(padded);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}
var HEARTBEAT_MS = 25e3;
var MAX_BACKOFF_MS = 15e3;
var RelayChannel = class {
  constructor(pairing, side, onMessage, onStatus = () => void 0, makeSocket = (url) => new WebSocket(url)) {
    this.pairing = pairing;
    this.side = side;
    this.onMessage = onMessage;
    this.onStatus = onStatus;
    this.makeSocket = makeSocket;
    this.topic = `realtime:physio-ai-${pairing.room}`;
  }
  pairing;
  side;
  onMessage;
  onStatus;
  makeSocket;
  socket = null;
  key = null;
  ref = 0;
  heartbeat = null;
  retry = null;
  backoff = 1e3;
  closed = false;
  topic;
  async connect() {
    this.closed = false;
    this.key ??= await importSecret(this.pairing.secret);
    this.open();
  }
  close() {
    this.closed = true;
    this.stopTimers();
    this.socket?.close();
    this.socket = null;
    this.onStatus("closed");
  }
  get isOpen() {
    return this.socket?.readyState === 1;
  }
  /** Dropped silently while disconnected; the phone asks for a fresh copy on reconnect. */
  async send(message) {
    if (!this.key || !this.isOpen) return;
    const sealed = await seal(this.key, { from: this.side, body: message });
    this.push("broadcast", { type: "broadcast", event: "m", payload: sealed });
  }
  open() {
    const base = this.pairing.relayUrl.replace(/^http/, "ws");
    const url = `${base}/realtime/v1/websocket?apikey=${encodeURIComponent(this.pairing.relayKey)}&vsn=1.0.0`;
    this.onStatus("connecting");
    const socket = this.makeSocket(url);
    this.socket = socket;
    socket.onopen = () => {
      this.backoff = 1e3;
      this.push("phx_join", {
        config: { broadcast: { self: false, ack: false }, presence: { key: "" }, private: false }
      });
      this.heartbeat = setInterval(() => {
        this.send_raw({ topic: "phoenix", event: "heartbeat", payload: {}, ref: String(++this.ref) });
      }, HEARTBEAT_MS);
    };
    socket.onmessage = (event) => {
      void this.receive(event.data);
    };
    socket.onclose = () => {
      this.stopTimers();
      if (this.socket !== socket) return;
      this.socket = null;
      if (this.closed) return;
      this.onStatus("connecting");
      this.retry = setTimeout(() => this.open(), this.backoff);
      this.backoff = Math.min(this.backoff * 2, MAX_BACKOFF_MS);
    };
    socket.onerror = () => {
    };
  }
  async receive(data) {
    if (typeof data !== "string" || !this.key) return;
    let frame;
    try {
      frame = JSON.parse(data);
    } catch {
      return;
    }
    if (frame.topic !== this.topic) return;
    if (frame.event === "phx_reply" && frame.payload?.status === "ok") {
      this.onStatus("open");
      return;
    }
    if (frame.event !== "broadcast") return;
    const inner = frame.payload?.payload;
    const opened = await open(this.key, inner);
    if (!opened || opened.from === this.side || opened.body === void 0) return;
    this.onMessage(opened.body);
  }
  push(event, payload) {
    this.send_raw({ topic: this.topic, event, payload, ref: String(++this.ref) });
  }
  send_raw(frame) {
    if (this.socket?.readyState === 1) this.socket.send(JSON.stringify(frame));
  }
  stopTimers() {
    if (this.heartbeat) clearInterval(this.heartbeat);
    if (this.retry) clearTimeout(this.retry);
    this.heartbeat = null;
    this.retry = null;
  }
};

// src/phone/phone.ts
var PAIRING_KEY = "physio-ai-pairing";
function must(selector) {
  const found = document.querySelector(selector);
  if (!found) throw new Error(`Missing ${selector}`);
  return found;
}
function el(tag, className = "", text = "") {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text) node.textContent = text;
  return node;
}
var feed = must("#feed");
var intro = must("#intro");
var siteLine = must("#site");
var cards = must("#cards");
var conn = must("#conn");
var input = must("#input");
var sendButton = must("#send");
var stopButton = must("#stop");
var unpaired = must("#unpaired");
var working = false;
var status = "connecting";
var steps = /* @__PURE__ */ new Map();
var boxes = /* @__PURE__ */ new Map();
function loadPairing() {
  const fromHash = readPairing(location.hash);
  if (fromHash) {
    try {
      sessionStorage.setItem(PAIRING_KEY, JSON.stringify(fromHash));
    } catch {
    }
    history.replaceState(null, "", location.pathname);
    return fromHash;
  }
  try {
    const saved = JSON.parse(sessionStorage.getItem(PAIRING_KEY) ?? "null");
    if (!saved) return null;
    return readPairing(`#${new URLSearchParams({ u: saved.relayUrl, a: saved.relayKey, r: saved.room, k: saved.secret })}`);
  } catch {
    return null;
  }
}
function scrollDown() {
  feed.scrollTop = feed.scrollHeight;
}
function setWorking(on) {
  working = on;
  sendButton.hidden = on;
  stopButton.hidden = !on;
  renderConn();
}
function renderConn() {
  conn.classList.toggle("on", status === "open");
  conn.classList.toggle("busy", status === "open" && working);
  conn.textContent = status === "open" ? working ? "Working" : "PC connected" : status === "connecting" ? "Connecting\u2026" : "Offline";
}
function resetFeed() {
  for (const child of [...feed.children]) if (child !== intro) child.remove();
  steps.clear();
  boxes.clear();
  intro.hidden = false;
}
function append(node) {
  intro.hidden = true;
  feed.append(node);
  scrollDown();
}
function renderCards(list) {
  cards.replaceChildren();
  for (const item of list) {
    const card = el("button", "card");
    card.append(el("b", "", item.title), el("small", "", item.prompt.trim().slice(0, 70)));
    card.addEventListener("click", () => {
      if (item.prompt.endsWith(" ")) {
        input.value = item.prompt;
        input.focus();
      } else {
        void command({ k: "send", text: item.prompt });
      }
    });
    cards.append(card);
  }
}
function settle(id, outcome) {
  const box = boxes.get(id);
  if (!box || box.classList.contains("settled")) return;
  box.classList.add("settled");
  box.querySelector(".btns")?.remove();
  const notes = box.querySelector(".notes");
  if (notes) notes.readOnly = true;
  box.append(el("div", "outcome", outcome === "approved" ? "Approved" : "Cancelled"));
}
function planBox(event) {
  const box = el("div", "box");
  const head = el("div", "box-head", event.title);
  head.append(el("span", "tag", "Draft only"));
  const notes = document.createElement("textarea");
  notes.className = "notes";
  notes.value = event.notes;
  box.append(head, el("div", "label", "Text to write (tap to edit)"), notes);
  if (event.assumed.length > 0) {
    box.append(el("div", "label", "Assumed: check these"));
    const chips = el("div", "chips");
    for (const item of event.assumed) chips.append(el("span", "chip", item));
    box.append(chips);
  }
  const buttons = el("div", "btns");
  const approve = el("button", "btn primary", "Approve plan");
  const cancel = el("button", "btn", "Cancel");
  approve.addEventListener("click", () => {
    if (!notes.value.trim()) return;
    void command({ k: "plan", id: event.id, approved: true, notes: notes.value.trim() });
  });
  cancel.addEventListener("click", () => void command({ k: "plan", id: event.id, approved: false, notes: "" }));
  buttons.append(approve, cancel);
  box.append(buttons);
  return box;
}
function approvalBox(event) {
  const box = el("div", "box ask");
  box.append(el("div", "box-head", event.label));
  if (event.detail) box.append(el("div", "detail", event.detail));
  if (event.why) box.append(el("div", "why", event.why));
  if (event.pcOnly) {
    box.append(el("div", "why", "This one can only be answered on the PC."));
    return box;
  }
  const buttons = el("div", "btns");
  const allow = el("button", "btn primary", "Allow");
  const deny = el("button", "btn", "Deny");
  allow.addEventListener("click", () => void command({ k: "approval", id: event.id, approved: true }));
  deny.addEventListener("click", () => void command({ k: "approval", id: event.id, approved: false }));
  buttons.append(allow, deny);
  box.append(buttons);
  return box;
}
function show(event) {
  switch (event.k) {
    case "hello":
      resetFeed();
      siteLine.textContent = event.site ? `Hospital PC \xB7 ${event.site}` : "Hospital PC";
      renderCards(event.cards);
      setWorking(event.working);
      return;
    case "site":
      siteLine.textContent = event.site ? `Hospital PC \xB7 ${event.site}` : "Hospital PC";
      return;
    case "clear":
      resetFeed();
      return;
    case "working":
      setWorking(event.on);
      return;
    case "user":
      append(el("div", "me", event.text));
      return;
    case "assistant":
      append(el("div", "say", event.text));
      return;
    case "note":
      append(el("div", "note", event.text));
      return;
    case "error":
      append(el("div", "error", event.text));
      return;
    case "step": {
      let row = steps.get(event.id);
      if (!row) {
        row = el("div", "step");
        steps.set(event.id, row);
        append(row);
      }
      row.textContent = event.label;
      row.classList.toggle("done", event.state === "done");
      row.classList.toggle("fail", event.state === "fail");
      return;
    }
    case "plan": {
      const box = planBox(event);
      boxes.set(event.id, box);
      append(box);
      return;
    }
    case "approval": {
      const box = approvalBox(event);
      boxes.set(event.id, box);
      append(box);
      return;
    }
    case "settled":
      settle(event.id, event.outcome);
      return;
  }
}
var channel = null;
async function command(message) {
  if (!channel?.isOpen) return;
  await channel.send(message);
}
async function main() {
  const pairing = loadPairing();
  if (!pairing) {
    feed.hidden = true;
    must(".composer").hidden = true;
    unpaired.hidden = false;
    conn.textContent = "Not paired";
    return;
  }
  channel = new RelayChannel(
    pairing,
    "phone",
    (event) => show(event),
    (next) => {
      status = next;
      renderConn();
      if (next === "open") void command({ k: "hello" });
    }
  );
  await channel.connect();
}
sendButton.addEventListener("click", () => {
  const text = input.value.trim();
  if (!text || working) return;
  void command({ k: "send", text });
  input.value = "";
});
input.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    sendButton.click();
  }
});
stopButton.addEventListener("click", () => void command({ k: "stop" }));
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") void command({ k: "hello" });
});
void main();
