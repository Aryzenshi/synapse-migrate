/** Small DOM helpers used by view modules; dynamic text is escaped before HTML rendering. */
export const byId = (id) => document.getElementById(id);

export function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (character) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[character]);
}

export function setHtml(id, html) { byId(id).innerHTML = html; }
export function setText(id, text) { byId(id).textContent = text; }

/** Render a consistent status pill, keeping untrusted status strings escaped. */
export function statusBadge(status) {
  const safe = escapeHtml(status || "unknown");
  const className = String(status || "").toLowerCase().replace(/[^a-z0-9_-]/g, "");
  return `<span class="badge ${className}">${safe}</span>`;
}
