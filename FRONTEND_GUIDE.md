# LiveEdit Frontend Developer Guide

This document provides all essential information for frontend developers working with the LiveEdit project, beyond the Swagger API documentation. It covers authentication, REST API usage, WebSocket protocol, real-time collaboration, security, and best practices for integrating with the backend.

---

## 1. Authentication & Authorization

- **JWT-based Auth:**
  - All protected REST and WebSocket endpoints require a valid JWT access token.
  - Obtain tokens via `/api/user/login` or `/api/user/register` (see Swagger for details).
  - Refresh tokens using `/api/user/refresh`.
- **Token Storage:**
  - Store JWT tokens securely (e.g., in-memory, not in cookies or localStorage for production).
- **REST Requests:**
  - Pass the access token in the `Authorization` header as `Bearer <token>`.
- **WebSocket:**
  - Pass the access token as a query parameter: `ws://<host>:8080/ws/editor?token=...`

---

## 2. REST API Usage

- **Base URL:**
  - REST API is available at `http://<host>:8080/` (see Swagger for all endpoints).
- **Main Use Cases:**
  - **Authentication:** login, register, refresh token
  - **Documents:** create, list, get, update, delete
  - **Profile:** get user profile
  - **Access Control:** invite links, accept invites, set roles, revoke access
  - **Explicit Save:** PATCH document content (optional, as autosave is enabled)
- **Swagger:**
  - All request/response schemas and endpoints are described in `swagger/combined.json`.

---

## 3. WebSocket API (Real-Time Collaboration)

- **Endpoint:**
  - `ws://<host>:8080/ws/editor?token=...`
- **Protocol:**
  - JSON messages with fields: `type` (string), `data` (object)
- **Session Initialization:**
  1. Open WebSocket connection with token.
  2. Send `{ "type": "init", "data": { "documentId": <id> } }`.
  3. Receive `{ "type": "init", "data": { "documentId": <id>, "content": "..." } }` with current document content.
- **Editing:**
  - Send `{ "type": "edit", "data": { "position": <int>, "text": <string>, "isInsert": <bool> } }` on every text change.
  - Receive broadcasted edits from all users: `{ "type": "edit", "data": { "clientId": <string>, "position": <int>, "text": <string>, "isInsert": <bool>, "operationId": <string> } }`.
- **Cursor Updates:**
  - Send `{ "type": "cursor", "data": { "position": <int> } }` on cursor move.
  - Receive broadcasted cursors: `{ "type": "cursor", "data": { "clientId": <string>, "position": <int> } }`.
- **Undo/Redo:**
  - Send `{ "type": "undo" }` or `{ "type": "redo" }`.
  - Receive broadcast: `{ "type": "undo", "data": { "clientId": <string> } }` or `{ "type": "redo", "data": { "clientId": <string> } }`.
- **Reconnect Logic:**
  - On disconnect, reconnect and repeat handshake (`init`).
  - Restore document state from server response.
- **Session Management:**
  - Each document has a separate session.
  - All real-time events are broadcast to all connected clients of the document.

---

## 4. Real-Time Editing Logic

- **Operational Transformation (OT):**
  - The backend applies OT to resolve concurrent edits.
  - Edits are identified by `operationId` and `clientId`.
- **Client Responsibilities:**
  - Apply incoming edits/cursors/undo/redo to the local editor state.
  - Maintain local cursor and selection state.
  - Debounce or throttle outgoing events to avoid flooding the server.
- **Autosave:**
  - The server autosaves document content periodically.
  - Optionally, the client can trigger explicit save via REST PATCH.

---

## 5. UI/UX Recommendations

- **Document List:**
  - Fetch via REST `/api/document`.
- **Open Document:**
  - Fetch document info via REST, then open WebSocket for real-time editing.
- **Editor Integration:**
  - Use a rich text/code editor (e.g., Monaco, CodeMirror) and sync with WebSocket events.
- **Error Handling:**
  - Handle WebSocket errors, token expiration, and reconnect logic gracefully.
- **Access Control:**
  - Use invite links and role management endpoints for sharing and permissions.

---

## 6. Security Best Practices

- **Always use HTTPS/WSS** in production.
- **Never store JWT in cookies** (to prevent XSS/CSRF).
- **Validate all user input** on the frontend before sending to the backend.
- **Handle token expiration**: refresh tokens as needed, prompt user to re-login if necessary.

---

## 7. Project Structure & Static Assets

- **Swagger UI:**
  - Available at `http://<host>:8080/swagger/` for API exploration.
- **Static Files:**
  - Served from `/swagger/` path (see backend for details).

---

## 8. Example Frontend Workflow

1. **User logs in** via REST, receives JWT tokens.
2. **Fetches document list** via REST.
3. **Opens a document**:
   - Fetches document info via REST.
   - Opens WebSocket, sends `init`.
   - Receives document content.
4. **Edits document**:
   - Sends `edit`/`cursor`/`undo`/`redo` via WebSocket.
   - Receives real-time updates from other users.
5. **Saves document** (optional):
   - Sends PATCH via REST or relies on autosave.
6. **Manages access**:
   - Uses invite links, role management, and access revocation via REST.

---

## 9. Useful Tips

- **WebSocket reconnect:** implement exponential backoff and state restoration.
- **Editor state:** always apply server events in the order received.
- **Testing:** use Swagger UI for REST, and browser dev tools for WebSocket debugging.

---

## 10. Further Reading

- See `WEBSOCKET_API.md` for detailed WebSocket message schemas and scenarios.
- See `swagger/combined.json` for full REST API schemas.
- Ask backend developers for any protocol or integration clarifications.

---

**Happy coding!**
