/** Pure rendering functions for connection, project, and job tables. */
import { escapeHtml, statusBadge } from "./dom.js";

function table(headers, rows) {
  return `<div class="table-wrap"><table class="data-table"><thead><tr>${headers.map((item) => `<th>${item}</th>`).join("")}</tr></thead><tbody>${rows.join("")}</tbody></table></div>`;
}

export function connectionTable(items) {
  if (!items.length) return `<div class="empty-state">No connections registered yet. Connect the bundled SQLite demo or add a database.</div>`;
  return table(["CONNECTION", "ENGINE", "ROLE", "LIVE STATUS", "ACTIONS"], items.map((item) => {
    const status = item.connection_status || "unknown";
    const label = status === "successful" ? "Connection successful" : status === "failed" ? "Connection failed" : "Not checked";
    const statusClass = status === "successful" ? "completed" : status === "failed" ? "failed" : "";
    const checkedAt = item.last_checked_at ? new Date(item.last_checked_at).toLocaleString() : "Not checked yet";
    return `<tr>
      <td><div class="cell-title">${escapeHtml(item.name)}</div><div class="cell-sub monospace">${escapeHtml(item.database || item.host || "")}</div></td>
      <td>${escapeHtml(item.engine)}</td><td>${statusBadge(item.role)}</td>
      <td><span class="badge ${statusClass}">${label}</span><div class="cell-sub">Checked ${escapeHtml(checkedAt)}</div>${status === "failed" && item.status_detail ? `<div class="cell-sub connection-error">${escapeHtml(item.status_detail)}</div>` : ""}</td>
      <td><button class="row-action" data-test-connection="${escapeHtml(item.connection_id)}">Test now</button> · <button class="row-action" data-remove-connection="${escapeHtml(item.connection_id)}">Remove</button></td></tr>`;
  }));
}

export function projectTable(items) {
  if (!items.length) return `<div class="empty-state">No schema projects yet. Analyze a source connection to get started.</div>`;
  return table(["PROJECT", "SOURCE", "DIRECTION", "REVISION", "ACTION"], items.map((item) => `<tr>
    <td><div class="cell-title">${escapeHtml(item.project_id)}</div><div class="cell-sub">${escapeHtml(new Date(item.created_at).toLocaleString())}</div></td>
    <td>${escapeHtml(item.source_engine)}</td><td>${escapeHtml(item.direction.replaceAll("_", " "))}</td><td>r${escapeHtml(item.revision)}</td>
    <td><button class="row-action" data-view-project="${escapeHtml(item.project_id)}">Review schema</button></td></tr>`));
}

export function databaseTable(items) {
  if (!items.length) return `<div class="empty-state">No active saved databases are available to browse. Add or renew a database connection first.</div>`;
  return table(["DATABASE", "ENGINE", "REGISTERED AS", "ACTION"], items.map((item) => `<tr>
    <td><div class="cell-title">${escapeHtml(item.database)}</div><div class="cell-sub">${escapeHtml(item.endpoint || "")}</div></td>
    <td>${escapeHtml(item.engine)}</td><td>${escapeHtml(item.roles.join(" + "))}</td>
    <td><button class="row-action" data-browse-database="${escapeHtml(item.browser_id)}">Browse</button></td></tr>`));
}

export function databaseObjectList(items) {
  if (!items.length) return `<div class="empty-state compact-empty">No tables, views, or collections found.</div>`;
  return `<div class="browser-object-list">${items.map((item) => `<button class="browser-object" data-browse-object="${escapeHtml(item.name)}"><span>${escapeHtml(item.name)}</span><small>${escapeHtml(item.kind)}</small></button>`).join("")}</div>`;
}

export function jobTable(items) {
  if (!items.length) return `<div class="empty-state">No migration jobs yet. A target connection is required to execute a plan.</div>`;
  return table(["JOB", "STATUS", "DIRECTION", "PROGRESS", "CREATED", "ACTIONS"], items.map((item) => {
    const resumable = ["failed", "failed_threshold", "paused", "waiting_for_credentials"].includes(item.status);
    return `<tr>
      <td><div class="cell-title">${escapeHtml(item.job_id)}</div><div class="cell-sub">Plan ${escapeHtml(item.plan_id)}</div></td>
      <td>${statusBadge(item.status)}${item.error ? `<div class="job-error"><strong>${escapeHtml(item.error_code || "Job error")}</strong><br>${escapeHtml(item.error)}${item.error_location ? `<br><small>At ${escapeHtml(item.error_location)}</small>` : ""}</div>` : item.error_summary?.total ? `<div class="job-error"><strong>${escapeHtml(item.error_summary.total)} row error(s)</strong><br><button class="row-action" data-view-job-errors="${escapeHtml(item.job_id)}">View first 25</button>${item.status === "completed_with_errors" ? `<br><button class="row-action" data-replay-job-errors="${escapeHtml(item.job_id)}">Replay eligible errors</button>` : ""}</div>` : ""}</td><td>${escapeHtml((item.direction || "").replaceAll("_", " "))}</td>
      <td>${escapeHtml(item.progress?.rows_done ?? 0)} rows</td><td>${escapeHtml(new Date(item.created_at).toLocaleString())}</td>
      <td>${resumable ? `<button class="row-action" data-resume-job="${escapeHtml(item.job_id)}">Resume same job</button>` : "—"}</td></tr>`;
  }));
}
