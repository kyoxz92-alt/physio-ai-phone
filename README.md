# Physio AI phone page

The phone remote for the Physio AI browser extension. Static files only: this
page holds no keys, addresses or data. Pairing details (relay, room and the
encryption key) arrive in the QR link's #fragment, which browsers never send to
a server, and every message is encrypted end to end.

Built from the main project with `npm run build` (output `phone-dist/`).
