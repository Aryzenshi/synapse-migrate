# Browser dashboard

This directory contains the user-facing dashboard as static browser files. It has no JavaScript framework, frontend build step, CDN, or runtime package manager. FastAPI serves `index.html` at `/` and this directory's other files under `/dashboard/`.

## Files and responsibilities

- `index.html` owns semantic page structure, navigation labels, accessible dialogs, and stable element IDs. It contains no workflow logic or visual CSS.
- `styles.css` owns design tokens, layout, reusable component styling, and mobile breakpoints. Keep styles here rather than adding inline presentation to the HTML.
- `js/api.js` is the only module that should handle HTTP, bearer credentials, API base URLs, or response errors. It also persists the browser's API settings.
- `js/dom.js` owns safe text escaping and small DOM utilities shared by renderers and controllers.
- `js/views.js` renders display-only tables from API data. It should not initiate requests or mutate workflow state.
- `js/app.js` wires events, owns transient dashboard state, submits forms, and composes the API and view modules. Opening a schema project also fetches its read-only `HEAD 5` sample through `/api/schema/ir/{project_id}/sample`; running a real migration is a separate, confirmation-gated action from a generated plan.

## Feature workflow

1. Add or edit the page structure in `index.html` and style in `styles.css`.
2. Put API calls behind the functions in `js/api.js`.
3. Put reusable, display-only table rendering in `js/views.js`; escape all dynamic text.
4. Add user actions and orchestration in `js/app.js`; report errors through its notification helper.
5. Verify the static assets are served, run `pytest -q`, and syntax-check JavaScript with `node --check dashboard/js/*.js` when Node.js is installed.

The bundled SQLite demo file is deliberately stored at the project level (`demo/synapsemigrate_demo.db`), not inside this UI directory. The API returns the server-side path through the authenticated `/api/demo/sqlite` endpoint so the browser never guesses a container or host filesystem path.
