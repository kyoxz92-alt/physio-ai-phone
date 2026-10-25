// src/shared/relay.ts
function isPhoneMode(value) {
  return value === "manual" || value === "auto";
}
function validPairing(value) {
  if (!value || typeof value !== "object") return null;
  const saved = value;
  if (typeof saved.relayUrl !== "string" || typeof saved.relayKey !== "string" || typeof saved.room !== "string" || typeof saved.secret !== "string") return null;
  return readPairing(`#${new URLSearchParams({
    u: saved.relayUrl,
    a: saved.relayKey,
    r: saved.room,
    k: saved.secret
  })}`);
}
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

// src/sidepanel/markdown.ts
var FENCE = /^ {0,3}(`{3,}|~{3,})\s*([\w+.-]*)\s*$/;
var HEADING = /^ {0,3}(#{1,6})\s+(.*?)\s*#*\s*$/;
var RULE = /^ {0,3}([-*_])(?:\s*\1){2,}\s*$/;
var QUOTE = /^ {0,3}>/;
var LIST_ITEM = /^ {0,3}([-*+]|\d{1,3}[.)])\s+(.*)$/;
var TABLE_DIVIDER = /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$/;
function parseMarkdown(source) {
  const lines = source.replace(/\r\n?/g, "\n").split("\n");
  const blocks = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (!line.trim()) {
      i++;
      continue;
    }
    const fence = FENCE.exec(line);
    if (fence) {
      const marker = fence[1][0];
      const close = new RegExp(`^ {0,3}${marker}{3,}\\s*$`);
      const body = [];
      i++;
      while (i < lines.length && !close.test(lines[i])) {
        body.push(lines[i]);
        i++;
      }
      i++;
      blocks.push({ type: "code", language: fence[2] ?? "", text: body.join("\n") });
      continue;
    }
    const heading = HEADING.exec(line);
    if (heading) {
      blocks.push({ type: "heading", level: heading[1].length, text: heading[2] });
      i++;
      continue;
    }
    if (RULE.test(line)) {
      blocks.push({ type: "rule" });
      i++;
      continue;
    }
    if (line.includes("|") && i + 1 < lines.length && TABLE_DIVIDER.test(lines[i + 1])) {
      const header = splitRow(line);
      i += 2;
      const rows = [];
      while (i < lines.length && lines[i].includes("|") && lines[i].trim()) {
        rows.push(splitRow(lines[i]));
        i++;
      }
      blocks.push({ type: "table", header, rows });
      continue;
    }
    if (QUOTE.test(line)) {
      const quote = [];
      while (i < lines.length && QUOTE.test(lines[i])) {
        quote.push(lines[i].replace(/^ {0,3}> ?/, ""));
        i++;
      }
      blocks.push({ type: "quote", text: quote.join("\n") });
      continue;
    }
    const item = LIST_ITEM.exec(line);
    if (item) {
      const ordered = /^\d/.test(item[1]);
      const items = [];
      while (i < lines.length) {
        const next = LIST_ITEM.exec(lines[i]);
        if (next) {
          if (/^\d/.test(next[1]) !== ordered) break;
          items.push(next[2]);
          i++;
          continue;
        }
        if (items.length > 0 && lines[i].trim() && /^\s{2,}/.test(lines[i]) && !isBlockStart(lines[i])) {
          items[items.length - 1] += ` ${lines[i].trim()}`;
          i++;
          continue;
        }
        break;
      }
      blocks.push({ type: "list", ordered, items });
      continue;
    }
    const paragraph = [line];
    i++;
    while (i < lines.length && lines[i].trim() && !isBlockStart(lines[i])) {
      if (lines[i].includes("|") && i + 1 < lines.length && TABLE_DIVIDER.test(lines[i + 1])) break;
      paragraph.push(lines[i]);
      i++;
    }
    blocks.push({ type: "paragraph", text: paragraph.join("\n") });
  }
  return blocks;
}
function isBlockStart(line) {
  return FENCE.test(line) || HEADING.test(line) || RULE.test(line) || QUOTE.test(line) || LIST_ITEM.test(line);
}
function splitRow(line) {
  let text = line.trim();
  if (text.startsWith("|")) text = text.slice(1);
  if (text.endsWith("|")) text = text.slice(0, -1);
  return text.split("|").map((cell) => cell.trim());
}
function parseInline(text) {
  const out = [];
  let buffer = "";
  let i = 0;
  const flush = () => {
    if (buffer) {
      out.push({ type: "text", text: buffer });
      buffer = "";
    }
  };
  while (i < text.length) {
    const ch = text[i];
    if (ch === "`") {
      const end = text.indexOf("`", i + 1);
      if (end > i + 1) {
        flush();
        out.push({ type: "code", text: text.slice(i + 1, end) });
        i = end + 1;
        continue;
      }
    }
    if (ch === "*" && text[i + 1] === "*") {
      const end = text.indexOf("**", i + 2);
      if (end > i + 2) {
        flush();
        out.push({ type: "strong", children: parseInline(text.slice(i + 2, end)) });
        i = end + 2;
        continue;
      }
    }
    if (ch === "*") {
      const end = text.indexOf("*", i + 1);
      if (end > i + 1) {
        flush();
        out.push({ type: "em", children: parseInline(text.slice(i + 1, end)) });
        i = end + 1;
        continue;
      }
    }
    if (ch === "[") {
      const close = text.indexOf("]", i + 1);
      if (close > i && text[close + 1] === "(") {
        const paren = text.indexOf(")", close + 2);
        if (paren > close + 1) {
          flush();
          const label = text.slice(i + 1, close);
          const href = safeHref(text.slice(close + 2, paren).trim());
          if (href) out.push({ type: "link", href, children: parseInline(label) });
          else out.push({ type: "text", text: label });
          i = paren + 1;
          continue;
        }
      }
    }
    if (ch === "\n") {
      flush();
      out.push({ type: "break" });
      i++;
      continue;
    }
    buffer += ch;
    i++;
  }
  flush();
  return out;
}
function safeHref(value) {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:" ? url.href : null;
  } catch {
    return null;
  }
}
function renderMarkdown(container, source) {
  container.replaceChildren(...parseMarkdown(source).map(renderBlock));
}
function renderBlock(block) {
  switch (block.type) {
    case "paragraph": {
      const p = document.createElement("p");
      appendInline(p, block.text);
      return p;
    }
    case "heading": {
      const level = Math.min(6, Math.max(1, block.level));
      const node = document.createElement(`h${level}`);
      appendInline(node, block.text);
      return node;
    }
    case "code": {
      const pre = document.createElement("pre");
      const code = document.createElement("code");
      if (block.language) code.className = `language-${block.language}`;
      code.textContent = block.text;
      pre.append(code);
      return pre;
    }
    case "list": {
      const list = document.createElement(block.ordered ? "ol" : "ul");
      for (const item of block.items) {
        const li = document.createElement("li");
        appendInline(li, item);
        list.append(li);
      }
      return list;
    }
    case "quote": {
      const quote = document.createElement("blockquote");
      appendInline(quote, block.text);
      return quote;
    }
    case "rule":
      return document.createElement("hr");
    case "table": {
      const table = document.createElement("table");
      const head = document.createElement("thead");
      const headRow = document.createElement("tr");
      for (const cell of block.header) {
        const th = document.createElement("th");
        appendInline(th, cell);
        headRow.append(th);
      }
      head.append(headRow);
      table.append(head);
      if (block.rows.length > 0) {
        const body = document.createElement("tbody");
        for (const cells of block.rows) {
          const row = document.createElement("tr");
          for (const cell of cells) {
            const td = document.createElement("td");
            appendInline(td, cell);
            row.append(td);
          }
          body.append(row);
        }
        table.append(body);
      }
      return table;
    }
  }
}
function appendInline(parent, text) {
  for (const node of renderInline(parseInline(text))) parent.append(node);
}
function renderInline(tokens) {
  return tokens.map((token) => {
    switch (token.type) {
      case "text":
        return document.createTextNode(token.text);
      case "break":
        return document.createElement("br");
      case "code": {
        const code = document.createElement("code");
        code.textContent = token.text;
        return code;
      }
      case "strong": {
        const strong = document.createElement("strong");
        strong.append(...renderInline(token.children));
        return strong;
      }
      case "em": {
        const em = document.createElement("em");
        em.append(...renderInline(token.children));
        return em;
      }
      case "link": {
        const link = document.createElement("a");
        link.href = token.href;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.append(...renderInline(token.children));
        return link;
      }
    }
  });
}

// src/phone/phone.ts
var PAIRING_KEY = "physio-ai-pairing";
var MAX_COMPOSER_HEIGHT = 160;
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
var composer = must(".composer");
var composerHint = must("#composer-hint");
var sendButton = must("#send");
var stopButton = must("#stop");
var unpaired = must("#unpaired");
var forgetButton = must("#forget");
var newChatButton = must("#new-chat");
var usageLine = must("#usage");
var modeOptions = [...document.querySelectorAll("#mode-seg .mode-opt")];
var working = false;
var sending = false;
var paired = true;
var status = "connecting";
var mode = "manual";
var steps = /* @__PURE__ */ new Map();
var boxes = /* @__PURE__ */ new Map();
function loadPairing() {
  const fromHash = readPairing(location.hash);
  if (fromHash) {
    let durable = false;
    try {
      localStorage.setItem(PAIRING_KEY, JSON.stringify(fromHash));
      durable = true;
    } catch {
    }
    try {
      if (durable) sessionStorage.removeItem(PAIRING_KEY);
      else sessionStorage.setItem(PAIRING_KEY, JSON.stringify(fromHash));
    } catch {
    }
    history.replaceState(null, "", location.pathname);
    return fromHash;
  }
  try {
    const saved = validPairing(JSON.parse(localStorage.getItem(PAIRING_KEY) ?? "null"));
    if (saved) return saved;
  } catch {
  }
  try {
    return validPairing(JSON.parse(sessionStorage.getItem(PAIRING_KEY) ?? "null"));
  } catch {
    return null;
  }
}
function isNearBottom() {
  return feed.scrollHeight - feed.scrollTop - feed.clientHeight < 96;
}
function append(node) {
  const follow = isNearBottom() || !intro.hidden;
  intro.hidden = true;
  feed.append(node);
  if (follow) feed.scrollTop = feed.scrollHeight;
}
function resizeComposer() {
  input.style.height = "auto";
  const maxHeight = Math.min(MAX_COMPOSER_HEIGHT, window.innerHeight * 0.32);
  input.style.height = `${Math.min(input.scrollHeight, maxHeight)}px`;
}
function canUseLink() {
  return paired && status === "open";
}
function updateComposer() {
  const online = canUseLink();
  const sendable = online && !working && !sending && input.value.trim().length > 0;
  sendButton.hidden = working;
  sendButton.disabled = !sendable;
  stopButton.hidden = !working;
  stopButton.disabled = !online;
  newChatButton.disabled = !online || working;
  for (const option of modeOptions) option.disabled = !online;
  input.setAttribute("aria-disabled", String(!online));
  composer.classList.toggle("offline", !online);
  composer.classList.toggle("busy", online && working);
  for (const card of cards.querySelectorAll("button")) card.disabled = !online || working;
  updateCardStatus();
  if (composerHint.classList.contains("send-error")) return;
  composerHint.textContent = !online ? paired && status === "connecting" ? "Reconnecting\u2026 Your draft is kept." : "Offline \xB7 Your draft is kept. Try again when connected." : working ? "Working on your PC \xB7 Stop to interrupt" : "Enter to send \xB7 Shift + Enter for a new line";
}
function updateCardStatus() {
  for (const box of boxes.values()) {
    const pending = box.classList.contains("pending") || box.classList.contains("settled");
    for (const button of box.querySelectorAll(".btn")) button.disabled = !canUseLink() || pending;
  }
}
function renderConn() {
  conn.classList.toggle("on", status === "open");
  conn.classList.toggle("busy", status === "open" && working);
  conn.classList.toggle("offline", paired && status === "closed");
  conn.textContent = !paired ? "Not paired" : status === "open" ? working ? "Working" : "PC connected" : status === "connecting" ? "Connecting\u2026" : "Offline";
  updateComposer();
}
function setMode(next) {
  mode = next;
  for (const option of modeOptions) option.setAttribute("aria-checked", String(option.dataset.mode === next));
}
function setWorking(on) {
  working = on;
  renderConn();
}
function resetFeed() {
  for (const child of [...feed.children]) if (child !== intro) child.remove();
  steps.clear();
  boxes.clear();
  intro.hidden = false;
}
function renderCards(list) {
  cards.replaceChildren();
  for (const item of list) {
    const fillsComposer = item.prompt.endsWith(" ");
    const card = el("button", "card");
    card.type = "button";
    const icon = el("span", "card-icon", fillsComposer ? "\u270E" : "\u2192");
    icon.setAttribute("aria-hidden", "true");
    const copy = el("span", "card-copy");
    copy.append(el("b", "", item.title), el("small", "", item.prompt.trim().slice(0, 100)));
    const arrow = el("span", "card-arrow", fillsComposer ? "\u21B5" : "\u203A");
    arrow.setAttribute("aria-hidden", "true");
    card.append(icon, copy, arrow);
    card.setAttribute("aria-label", `${item.title}. ${fillsComposer ? "Fill message box" : "Send to PC"}`);
    card.addEventListener("click", () => {
      if (!canUseLink() || working) return;
      if (fillsComposer) {
        input.value = item.prompt;
        resizeComposer();
        updateComposer();
        input.focus({ preventScroll: true });
        input.setSelectionRange(input.value.length, input.value.length);
      } else {
        void command({ k: "send", text: item.prompt });
      }
    });
    cards.append(card);
  }
  if (list.length === 0) cards.append(el("p", "start-note", "No shortcuts saved. Type a task below to get started."));
  updateComposer();
}
function folded(text, summary = "Show full text", code = false) {
  if (text.length < 700 && text.split("\n").length < 12) return el("div", code ? "detail" : "", text);
  const details = el("details", "fold");
  const body = el("div", "fold-body");
  body.append(el("div", code ? "detail" : "", text));
  details.append(el("summary", "", summary), body);
  return details;
}
function settle(id, outcome) {
  const box = boxes.get(id);
  if (!box || box.classList.contains("settled")) return;
  box.classList.add("settled");
  box.classList.remove("pending");
  box.querySelector(".btns")?.remove();
  const notes = box.querySelector(".notes");
  if (notes) notes.readOnly = true;
  const message = box.querySelector(".pc-only")?.textContent;
  box.querySelector(".pc-only")?.remove();
  const outcomeText = outcome === "approved" ? "Approved" : "Cancelled";
  const outcomeNode = el("div", `outcome${outcome === "cancelled" ? " cancelled" : ""}`, message ? `${outcomeText} \xB7 ${message}` : outcomeText);
  box.append(outcomeNode);
  if (box.contains(document.activeElement)) {
    box.tabIndex = -1;
    box.focus({ preventScroll: true });
  }
}
function setCardPending(box, pending, text) {
  box.classList.toggle("pending", pending);
  for (const button of box.querySelectorAll(".btn")) button.disabled = pending || !canUseLink();
  const hint = box.querySelector(".card-status") ?? el("div", "card-status note");
  hint.textContent = text;
  hint.setAttribute("role", "status");
  if (!hint.isConnected) box.append(hint);
}
async function answer(box, message) {
  if (box.classList.contains("pending") || box.classList.contains("settled")) return;
  if (!canUseLink()) {
    setCardPending(box, false, "Not connected. Your answer was not sent; try again.");
    return;
  }
  setCardPending(box, true, "Sending to your PC\u2026");
  const sent = await command(message);
  if (!sent && !box.classList.contains("settled")) setCardPending(box, false, "Not sent. Check the connection and try again.");
}
function planBox(event) {
  const box = el("div", "box plan");
  const head = el("div", "box-head");
  head.append(el("span", "head-text", event.title), el("span", "tag", "Draft only"));
  const notes = el("textarea", "notes");
  notes.value = event.notes;
  notes.setAttribute("aria-label", "Plan text to write");
  box.append(head, el("div", "label", "Text to write \xB7 review and edit"), notes);
  if (event.assumed.length > 0) {
    box.append(el("div", "label", "Assumed \xB7 check these"));
    const chips = el("div", "chips");
    for (const item of event.assumed) chips.append(el("span", "chip", item));
    box.append(chips);
  }
  const buttons = el("div", "btns");
  const approve = el("button", "btn primary", "Approve plan");
  const cancel = el("button", "btn", "Cancel");
  approve.type = cancel.type = "button";
  approve.addEventListener("click", () => {
    if (!notes.value.trim()) {
      setCardPending(box, false, "Add plan text before approving.");
      notes.focus();
      return;
    }
    void answer(box, { k: "plan", id: event.id, approved: true, notes: notes.value.trim() });
  });
  cancel.addEventListener("click", () => void answer(box, { k: "plan", id: event.id, approved: false, notes: "" }));
  buttons.append(approve, cancel);
  box.append(buttons);
  return box;
}
function approvalBox(event) {
  const box = el("div", "box ask");
  const head = el("div", "box-head");
  head.append(el("span", "head-text", event.label), el("span", "tag", "Approval"));
  box.append(head);
  if (event.detail) box.append(folded(event.detail, "Review action details", true));
  if (event.why) box.append(el("div", "why", event.why));
  if (event.pcOnly) {
    box.append(el("div", "why pc-only", "This one can only be answered on the PC."));
    return box;
  }
  const buttons = el("div", "btns");
  const allow = el("button", "btn primary", "Allow");
  const deny = el("button", "btn", "Deny");
  allow.type = deny.type = "button";
  allow.addEventListener("click", () => void answer(box, { k: "approval", id: event.id, approved: true }));
  deny.addEventListener("click", () => void answer(box, { k: "approval", id: event.id, approved: false }));
  buttons.append(allow, deny);
  box.append(buttons);
  return box;
}
function handleEvent(event) {
  switch (event.k) {
    case "hello":
      resetFeed();
      siteLine.textContent = event.site ? `Hospital PC \xB7 ${event.site}` : "Hospital PC";
      renderCards(event.cards);
      if (isPhoneMode(event.mode)) setMode(event.mode);
      usageLine.textContent = typeof event.usage === "string" ? event.usage : "";
      setWorking(event.working);
      for (const item of event.events) handleEvent(item);
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
    case "mode":
      setMode(event.mode);
      return;
    case "usage":
      usageLine.textContent = event.text;
      return;
    case "user":
      append(el("div", "me", event.text));
      return;
    case "assistant": {
      const say = el("div", "say");
      renderMarkdown(say, event.text);
      if (event.text.length > 1400 || event.text.split("\n").length > 24) {
        const details = el("details", "fold reply-fold");
        const body = el("div", "fold-body");
        body.append(say);
        details.append(el("summary", "", "Show full reply"), body);
        append(details);
      } else {
        append(say);
      }
      return;
    }
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
        row.append(el("span", "step-label"));
        steps.set(event.id, row);
        append(row);
      }
      row.querySelector(".step-label").textContent = event.label;
      row.classList.toggle("done", event.state === "done");
      row.classList.toggle("fail", event.state === "fail");
      row.classList.toggle("compact", event.state !== "run");
      row.setAttribute("aria-label", `${event.label}: ${event.state === "run" ? "working" : event.state === "done" ? "done" : "failed"}`);
      return;
    }
    case "plan": {
      if (boxes.has(event.id)) return;
      const box = planBox(event);
      boxes.set(event.id, box);
      append(box);
      updateComposer();
      return;
    }
    case "approval": {
      if (boxes.has(event.id)) return;
      const box = approvalBox(event);
      boxes.set(event.id, box);
      append(box);
      updateComposer();
      return;
    }
    case "settled":
      settle(event.id, event.outcome);
      return;
  }
}
var channel = null;
async function command(message) {
  if (!canUseLink() || !channel?.isOpen) return false;
  try {
    await channel.send(message);
    return channel.isOpen;
  } catch {
    return false;
  }
}
async function sendDraft() {
  const text = input.value.trim();
  if (!text || working || sending || !canUseLink()) {
    updateComposer();
    return;
  }
  sending = true;
  composerHint.classList.remove("send-error");
  sendButton.disabled = true;
  const sent = await command({ k: "send", text });
  sending = false;
  if (sent) {
    input.value = "";
    resizeComposer();
  } else {
    composerHint.classList.add("send-error");
    composerHint.textContent = "Not sent. Your draft is kept; try again when connected.";
  }
  updateComposer();
}
for (const option of modeOptions) {
  option.addEventListener("click", () => {
    const next = option.dataset.mode;
    if (!isPhoneMode(next) || next === mode) return;
    if (next === "auto" && !confirm("Switch to Auto? The agent will run ordinary actions without asking. Complete, Sign and scripts still need the PC.")) return;
    void command({ k: "mode", mode: next });
  });
}
newChatButton.addEventListener("click", () => {
  if (working || !canUseLink()) return;
  if (!confirm("Start a new conversation? This clears the conversation on the PC too.")) return;
  void command({ k: "new" });
});
forgetButton.addEventListener("click", () => {
  if (!confirm("Forget this PC? You will need to scan its QR code again.")) return;
  let cleared = true;
  try {
    localStorage.removeItem(PAIRING_KEY);
  } catch {
    cleared = false;
  }
  try {
    sessionStorage.removeItem(PAIRING_KEY);
  } catch {
    cleared = false;
  }
  if (cleared) location.reload();
  else alert("This browser blocked access to saved pairing. Clear this site\u2019s data in browser settings.");
});
async function main() {
  const pairing = loadPairing();
  if (!pairing) {
    paired = false;
    feed.hidden = true;
    composer.hidden = true;
    unpaired.hidden = false;
    renderConn();
    return;
  }
  forgetButton.hidden = false;
  newChatButton.hidden = false;
  channel = new RelayChannel(
    pairing,
    "phone",
    handleEvent,
    (next) => {
      status = next;
      renderConn();
      if (next === "open") {
        updateCardStatus();
        void command({ k: "hello" });
      }
    }
  );
  await channel.connect();
}
input.addEventListener("input", () => {
  composerHint.classList.remove("send-error");
  resizeComposer();
  updateComposer();
});
input.addEventListener("focus", () => {
  setTimeout(() => input.scrollIntoView({ block: "nearest", behavior: "auto" }), 250);
});
sendButton.addEventListener("click", () => void sendDraft());
stopButton.addEventListener("click", () => void command({ k: "stop" }));
input.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    void sendDraft();
  }
});
window.addEventListener("resize", resizeComposer);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") void command({ k: "hello" });
});
resizeComposer();
updateComposer();
void main();
export {
  handleEvent,
  loadPairing
};
