/** Dashboard controller: navigation, forms, API calls, and simple workflow orchestration. */
import { get, post, remove, getSettings, saveSettings } from "./api.js";
import { byId, escapeHtml, setHtml, setText } from "./dom.js";
import { connectionTable, databaseObjectList, databaseTable, jobTable, projectTable } from "./views.js";

const state = { connections: [], projects: [], jobs: [], databases: [], selectedDatabase: null, databaseObjects: [], currentObject: null, sampleOffset: 0, plan: null, planTargetConnectionId: "", selectedProject: null };
const pageNames = { overview: "Overview", connections: "Connections", projects: "Schema projects", jobs: "Migration jobs", "database-browser": "Database browser" };
const LOCATION_STORAGE_KEY = "synapsemigrate.current-location";

function readSavedLocation() {
  try {
    const location = JSON.parse(window.sessionStorage.getItem(LOCATION_STORAGE_KEY) || "{}");
    return location && typeof location === "object" ? location : {};
  } catch { return {}; }
}

function saveLocation(patch) {
  try { window.sessionStorage.setItem(LOCATION_STORAGE_KEY, JSON.stringify({ ...readSavedLocation(), ...patch })); }
  catch { /* The URL hash still preserves the top-level view when storage is unavailable. */ }
}

function notify(message, kind = "success") {
  const region = byId("toast-region");
  region.innerHTML = `<div class="toast ${kind}">${escapeHtml(message)}</div>`;
  window.clearTimeout(notify.timer);
  notify.timer = window.setTimeout(() => { region.innerHTML = ""; }, 6500);
}

function showError(error) { notify(error.message || String(error), "error"); }

async function safeRun(action) {
  try { return await action(); } catch (error) { showError(error); return null; }
}

/** Load small overview lists together so the dashboard's counts stay in sync. */
async function loadWorkspace({ checkConnectionHealth = false } = {}) {
  const [connections, projects, jobs] = await Promise.all([
    get("/connections"), get("/schema/projects?limit=25"), get("/migrations?limit=25"),
  ]);
  state.connections = connections.items || [];
  if (checkConnectionHealth) {
    state.connections = await Promise.all(state.connections.map(async (connection) => {
      try {
        return { ...connection, ...await post(`/connections/${encodeURIComponent(connection.connection_id)}/test`, {}) };
      } catch (error) {
        return { ...connection, connection_status: "failed", last_checked_at: new Date().toISOString(),
          status_detail: error.message || "The live connection check failed." };
      }
    }));
  }
  state.projects = projects.items || [];
  state.jobs = jobs.items || [];
  const destinationSelect = byId("plan-result-target-connection");
  if (destinationSelect && state.plan) {
    const selected = destinationSelect.value || state.planTargetConnectionId;
    destinationSelect.innerHTML = targetConnectionOptions(state.plan.target_engine, selected);
    destinationSelect.value = selected;
  }
  renderWorkspace();
  updateApiStatus(true);
}

function renderWorkspace() {
  setText("metric-connections", state.connections.length);
  setText("metric-projects", state.projects.length);
  setText("metric-jobs", state.jobs.length);
  setHtml("connections-list", connectionTable(state.connections));
  setHtml("projects-list", projectTable(state.projects));
  setHtml("jobs-list", jobTable(state.jobs));
  const recent = state.projects.slice(0, 4);
  setHtml("overview-projects", recent.length ? projectTable(recent) : `<div class="empty-state compact-empty">No projects yet. Connect a source and analyze its schema.</div>`);
  bindRowActions();
}

function updateApiStatus(online) {
  byId("api-dot").className = `status-dot ${online ? "good" : "bad"}`;
  setText("api-status", online ? "API connected" : "API unavailable");
}

function navigate(view, { syncHash = true } = {}) {
  if (!pageNames[view]) return;
  if (syncHash && window.location.hash !== `#${view}`) window.history.replaceState(window.history.state, "", `#${view}`);
  const savedLocation = readSavedLocation();
  if (savedLocation.view === "projects" && view !== "projects") saveLocation({ projectId: null });
  saveLocation({ view });
  document.querySelectorAll(".view").forEach((section) => section.classList.toggle("active-view", section.id === `view-${view}`));
  document.querySelectorAll(".nav-item").forEach((button) => button.classList.toggle("active", button.dataset.view === view));
  setText("breadcrumb-current", pageNames[view] || "Workspace");
  if (view === "connections") safeRun(() => loadWorkspace({ checkConnectionHealth: true }));
  if (view === "projects") safeRun(loadWorkspace);
  if (view === "jobs") safeRun(loadWorkspace);
  if (view === "database-browser") safeRun(loadDatabaseBrowser);
}

function bindRowActions() {
  document.querySelectorAll("[data-test-connection]").forEach((button) => button.addEventListener("click", async () => {
    const connectionId = button.dataset.testConnection;
    button.disabled = true;
    try {
      const status = await post(`/connections/${encodeURIComponent(connectionId)}/test`, {});
      state.connections = state.connections.map((item) => item.connection_id === connectionId ? { ...item, ...status } : item);
      renderWorkspace();
      notify(status.connection_status === "successful" ? "Connection test succeeded." : status.status_detail, status.connection_status === "successful" ? "success" : "error");
    } catch (error) {
      notify(error.message || "Connection test failed.", "error");
      button.disabled = false;
    }
  }));
  document.querySelectorAll("[data-remove-connection]").forEach((button) => button.addEventListener("click", async () => {
    if (!window.confirm("Remove this connection and its stored credentials?")) return;
    const result = await safeRun(() => remove(`/connections/${encodeURIComponent(button.dataset.removeConnection)}`));
    if (result === null) return;
    notify("Connection removed.");
    await safeRun(loadWorkspace);
  }));
  document.querySelectorAll("[data-resume-job]").forEach((button) => button.addEventListener("click", async () => {
    if (!window.confirm("Resume this same job using its saved checkpoints and the same destination? This avoids starting a new migration job.")) return;
    button.disabled = true;
    const result = await safeRun(() => post(`/migrations/${encodeURIComponent(button.dataset.resumeJob)}/resume`, {}));
    if (!result) { button.disabled = false; return; }
    notify(`Resumed job ${result.job_id}.`);
    await safeRun(loadWorkspace);
  }));
  document.querySelectorAll("[data-view-job-errors]").forEach((button) => button.addEventListener("click", async () => {
    const jobId = button.dataset.viewJobErrors;
    const parentRow = button.closest("tr");
    const existing = parentRow.nextElementSibling;
    if (existing?.dataset.errorJob === jobId) { existing.remove(); return; }
    button.disabled = true;
    const result = await safeRun(() => get(`/migrations/${encodeURIComponent(jobId)}/errors?limit=25&resolved=false`));
    button.disabled = false;
    if (!result) return;
    const records = result.items || [];
    const details = records.length ? records.map((item) => `<div class="dlq-item"><strong>${escapeHtml(item.error_code || "Error")} · ${escapeHtml(item.unit || "unknown unit")} · ${escapeHtml(item.stage || "")}</strong><p>${escapeHtml(item.error || "No message")}</p>${item.hint ? `<small>Hint: ${escapeHtml(item.hint)}</small>` : ""}</div>`).join("") : `<div class="empty-state compact-empty">No unresolved error records returned.</div>`;
    parentRow.insertAdjacentHTML("afterend", `<tr class="job-error-detail-row" data-error-job="${escapeHtml(jobId)}"><td colspan="6"><section class="dlq-details"><strong>Row-level errors · ${escapeHtml(records.length)} shown${result.has_more ? " · more available" : ""}</strong>${details}</section></td></tr>`);
  }));
  document.querySelectorAll("[data-replay-job-errors]").forEach((button) => button.addEventListener("click", async () => {
    const jobId = button.dataset.replayJobErrors;
    if (!window.confirm("Replay all eligible unresolved row errors using the current application code, saved checkpoints/ID map, and the same target database? This writes the recovered rows to MongoDB.")) return;
    button.disabled = true;
    const result = await safeRun(() => post(`/migrations/${encodeURIComponent(jobId)}/errors/replay`, { max_items: 10000 }));
    if (!result) { button.disabled = false; return; }
    notify(`Queued replay job ${result.replay_job_id} for ${result.selected} row error(s).`);
    await safeRun(loadWorkspace);
  }));
  document.querySelectorAll("[data-view-project]").forEach((button) => button.addEventListener("click", async () => {
    await openProject(button.dataset.viewProject);
    navigate("projects");
  }));
}

async function connectSqliteDemo() {
  const demo = await safeRun(() => get("/demo/sqlite"));
  if (!demo) return;
  const existing = state.connections.find((item) => item.engine === "sqlite" && item.role === "source" && item.database === demo.database);
  if (existing) {
    notify("SQLite demo is already connected. Reuse the existing source under Connections.");
    navigate("connections");
    return;
  }
  const result = await safeRun(() => post("/connections", {
    engine: "sqlite", role: "source", name: "Bundled SQLite demo",
    database: demo.database, ttl_seconds: 28800,
  }));
  if (!result) return;
  notify("SQLite demo source connected. You can now analyze its schema.");
  await safeRun(loadWorkspace);
  navigate("connections");
}

function updateConnectionFields() {
  const engine = byId("connection-engine").value;
  const role = byId("connection-role");
  const sqlite = engine === "sqlite";
  const mongo = engine === "mongodb";
  role.innerHTML = sqlite ? `<option value="source">Source</option>` : `<option value="source">Source</option><option value="target">Target</option>`;
  byId("field-database").classList.toggle("hidden", sqlite);
  byId("connection-database").required = !sqlite;
  byId("field-sqlite-path").classList.toggle("hidden", !sqlite);
  byId("connection-path").required = sqlite;
  byId("field-uri").classList.toggle("hidden", !mongo);
  ["field-host", "field-port", "field-user", "field-password"].forEach((id) => byId(id).classList.toggle("hidden", sqlite || mongo));
  const errorBox = byId("connection-form-error");
  errorBox.textContent = "";
  errorBox.classList.add("hidden");
  byId("connection-port").value = engine === "postgresql" ? "5432" : "3306";
}

async function submitConnection(event) {
  event.preventDefault();
  const engine = byId("connection-engine").value;
  const body = {
    engine, role: byId("connection-role").value,
    name: byId("connection-name").value.trim() || `My ${engine} connection`,
    ttl_seconds: 28800,
  };
  if (engine === "sqlite") body.database = byId("connection-path").value.trim();
  else if (engine === "mongodb") {
    body.uri = byId("connection-uri").value.trim();
    body.database = byId("connection-database").value.trim();
  } else Object.assign(body, {
    database: byId("connection-database").value.trim(), host: byId("connection-host").value.trim(),
    port: Number(byId("connection-port").value), user: byId("connection-user").value.trim(),
    password: byId("connection-password").value,
  });
  const button = event.submitter;
  const errorBox = byId("connection-form-error");
  errorBox.textContent = "";
  errorBox.classList.add("hidden");
  button.disabled = true;
  let result;
  try {
    result = await post("/connections", body);
  } catch (error) {
    // Keep connection-test errors inside the open dialog so the user can correct fields immediately.
    errorBox.textContent = error.message || "Could not verify this connection.";
    errorBox.classList.remove("hidden");
    button.disabled = false;
    return;
  }
  button.disabled = false;
  byId("connection-dialog").close();
  byId("connection-form").reset();
  updateConnectionFields();
  notify(`Verified and saved “${result.name}”.`);
  await safeRun(() => loadWorkspace({ checkConnectionHealth: true }));
}

function openAnalyzeDialog() {
  const sources = state.connections.filter((item) => item.role === "source");
  const select = byId("analysis-source");
  select.innerHTML = sources.map((item) => `<option value="${escapeHtml(item.connection_id)}">${escapeHtml(item.name)} · ${escapeHtml(item.engine)}</option>`).join("");
  if (!sources.length) {
    notify("Add or connect a source before starting schema analysis.", "error");
    navigate("connections");
    return;
  }
  byId("analyze-dialog").showModal();
}

async function submitAnalysis(event) {
  event.preventDefault();
  const sourceId = byId("analysis-source").value;
  const connection = state.connections.find((item) => item.connection_id === sourceId);
  const options = { infer_implicit_fks: true };
  if (connection?.engine === "sqlite") options.exact_row_counts = true;
  const button = event.submitter;
  button.disabled = true;
  button.textContent = "Analyzing…";
  const project = await safeRun(() => post("/schema/introspect", {
    source_type: connection.engine, connection_id: sourceId, options, wait: true,
  }));
  button.disabled = false;
  button.textContent = "Analyze schema";
  if (!project) return;
  byId("analyze-dialog").close();
  notify(`Schema analyzed: ${project.summary?.tables ?? project.summary?.collections ?? 0} objects found.`);
  await safeRun(loadWorkspace);
  await openProject(project.project_id);
  navigate("projects");
}

async function openProject(projectId) {
  saveLocation({ view: "projects", projectId });
  const project = await safeRun(() => get(`/schema/ir/${encodeURIComponent(projectId)}`));
  if (!project) return;
  state.selectedProject = project;
  renderProjectDetail(project);
  await loadProjectSamples(project.project_id);
}

async function loadProjectSamples(projectId) {
  const preview = await safeRun(() => get(`/schema/ir/${encodeURIComponent(projectId)}/sample?limit=5`));
  if (!preview) return;
  const target = byId("sample-preview");
  if (!target || state.selectedProject?.project_id !== projectId) return;
  const groups = preview.items.map((item) => `<details class="source-sample-card"><summary><strong>${escapeHtml(item.name)}</strong><span>${item.rows.length} of up to 5 rows</span></summary>
    ${item.rows.length ? `<div class="source-sample-rows">${item.rows.map((row, index) => `<div class="source-sample-row"><span class="sample-row-index">${index + 1}</span><pre>${escapeHtml(JSON.stringify(row, null, 2))}</pre></div>`).join("")}</div>` : `<p class="section-note">No records in this table or collection.</p>`}</details>`).join("");
  target.innerHTML = `<div class="panel-heading"><div><div class="eyebrow">SOURCE DATA PREVIEW</div><h2>Head 5 records per table or collection</h2></div><span class="badge">${escapeHtml(preview.source_engine)}</span></div>
    <p class="section-note">Read-only preview from the source connected to this project. Values are shown as JSON for clarity.</p><div class="source-sample-list">${groups}</div>`;
}

function renderProjectDetail(project) {
  const detail = byId("project-detail");
  detail.classList.remove("hidden");
  const tables = project.tables || [];
  const relationships = project.relationships || [];
  const collections = project.collections || [];
  const objectName = project.ir_kind === "relational" ? "tables" : "collections";
  const objectItems = project.ir_kind === "relational" ? tables.map((item) => `${item.schema_name ? `${item.schema_name}.` : ""}${item.table_name}`) : collections.map((item) => item.name);
  const relItems = project.ir_kind === "relational" ? relationships.map((item) => `${item.parent_table} → ${item.child_table} · ${item.proposed_mapping}`) : (project.decisions || []).slice(0, 40).map((item) => `${item.collection}.${item.path} · ${item.proposed_action}`);
  const advisories = project.advisories || [];
  const targetOptions = project.direction === "RELATIONAL_TO_DOCUMENT" ? ["mongodb"] : ["mysql", "postgresql"];
  detail.innerHTML = `<div class="panel-heading"><div><div class="eyebrow">PROJECT DETAILS</div><h2>${escapeHtml(project.project_id)}</h2></div><button id="detail-close" class="icon-button" aria-label="Hide details">×</button></div>
    <div class="detail-summary"><span>Source <strong>${escapeHtml(project.source_engine)}</strong></span><span>Direction <strong>${escapeHtml(project.direction.replaceAll("_", " "))}</strong></span><span>Revision <strong>${escapeHtml(project.revision)}</strong></span><span>Objects <strong>${objectItems.length}</strong></span><span>Relationships/decisions <strong>${relItems.length}</strong></span></div>
    <div class="detail-grid"><section class="detail-card"><h3>${project.ir_kind === "relational" ? "Tables" : "Collections"}</h3><ul>${objectItems.map((item) => `<li>${escapeHtml(item)}</li>`).join("") || "<li>None detected</li>"}</ul></section>
    <section class="detail-card"><h3>${project.ir_kind === "relational" ? "Relationships" : "Field decisions"}</h3><ul>${relItems.map((item) => `<li>${escapeHtml(item)}</li>`).join("") || "<li>None inferred</li>"}</ul></section></div>
    ${advisories.length ? `<div class="detail-card" style="margin-top:12px"><h3>Advisories (${advisories.length})</h3><ul>${advisories.map((item) => `<li>${escapeHtml(item.severity)} · ${escapeHtml(item.message)}</li>`).join("")}</ul></div>` : ""}
    <section id="sample-preview" class="source-sample-section"><p class="section-note">Loading the first five source rows for each table or collection…</p></section>
    <div class="panel-heading" style="margin-top:18px"><div><div class="eyebrow">NEXT STEP</div><h2>Generate a plan</h2></div><div class="button-group"><button id="generate-plan" class="button button-secondary">Plan options</button>${project.source_engine === "sqlite" && project.direction === "RELATIONAL_TO_DOCUMENT" ? `<button id="visualize-demo" class="button button-primary">Run guided demo →</button>` : ""}</div></div>
    <p class="section-note">${targetOptions.join(" or ")} are valid target engines for this direction. A connected target is only required when you are ready to execute writes.</p>
    <div id="plan-result"></div><div id="demo-result"></div>`;
  byId("detail-close").addEventListener("click", () => {
    detail.classList.add("hidden");
    saveLocation({ projectId: null });
  });
  byId("generate-plan").addEventListener("click", () => openPlanDialog(project));
  byId("visualize-demo")?.addEventListener("click", () => runGuidedDemo(project));
}

async function runGuidedDemo(project) {
  const trigger = byId("visualize-demo");
  trigger.disabled = true;
  trigger.textContent = "Preparing walkthrough…";
  const plan = await safeRun(() => post("/schema/generate", {
    project_id: project.project_id,
    target_engine: "mongodb",
    if_revision: project.revision,
    options: { etl: { batch_size: 2, on_drift: "widen", on_orphan: "DLQ", max_error_rate: 0.01 } },
    acknowledge_advisories: [],
  }));
  if (!plan) {
    trigger.disabled = false;
    trigger.textContent = "Run guided demo →";
    return;
  }
  state.plan = plan;
  trigger.textContent = "Reading sample rows…";
  const result = await safeRun(() => post("/demo/visualize", { plan_id: plan.plan_id, preview_rows_per_table: 4 }));
  trigger.disabled = false;
  trigger.textContent = "Run guided demo →";
  if (!result) return;
  renderGuidedDemo(result);
  notify("Guided walkthrough complete. No target database was contacted and no data was written.");
}

function renderGuidedDemo(result) {
  const stages = result.stages.map((stage, index) => `<div class="demo-stage ${stage.status === "simulated" ? "stage-simulated" : "stage-complete"}">
    <div class="stage-icon">${stage.status === "simulated" ? "Ⅱ" : "✓"}</div><div class="stage-copy"><strong>${index + 1}. ${escapeHtml(stage.label)}</strong><small>${escapeHtml(stage.detail)}</small></div><span class="badge ${stage.status === "simulated" ? "queued" : "completed"}">${escapeHtml(stage.status)}</span></div>`).join("");
  const cards = result.tables.map((table) => `<details class="demo-table-card"><summary><span><strong>${escapeHtml(table.table)}</strong><small>${escapeHtml(table.source_rows)} source rows · ${escapeHtml(table.preview_rows)} previewed</small></span><span class="demo-badge">${escapeHtml(table.primary_key.length ? `KEY ${table.primary_key.join(" + ")}` : "NO PRIMARY KEY")}</span></summary>
    <div class="sample-documents">${table.sample_documents.map((document) => `<pre>${escapeHtml(JSON.stringify(document, null, 2))}</pre>`).join("") || `<p class="section-note">No sample rows in this table.</p>`}</div></details>`).join("");
  byId("demo-result").innerHTML = `<section class="demo-run-panel"><div class="panel-heading"><div><div class="eyebrow">GUIDED WALKTHROUGH · VISUALIZATION ONLY</div><h2>Migration flow</h2></div><span class="simulation-pill">NO DATABASE WRITES</span></div>
    <div class="simulation-notice"><strong>Illustrative demo, not an ETL execution.</strong> The source row counts and previews below were read from the SQLite example database. The MongoDB write and verification stages are intentionally shown as simulated.</div>
    <div class="demo-summary-grid"><div><strong>${escapeHtml(result.summary.tables)}</strong><small>tables read</small></div><div><strong>${escapeHtml(result.summary.source_rows)}</strong><small>source rows</small></div><div><strong>${escapeHtml(result.summary.preview_documents)}</strong><small>document previews</small></div><div><strong>${escapeHtml(result.summary.batches_visualized)}</strong><small>visual batches</small></div><div><strong>0</strong><small>target writes</small></div></div>
    <div class="demo-timeline">${stages}</div><details class="demo-data-details"><summary>Inspect sample document previews</summary><div class="demo-table-list">${cards}</div></details>
    <p class="section-note">Plan <span class="monospace">${escapeHtml(result.plan_id)}</span> · hash <span class="monospace">${escapeHtml(result.plan_hash)}</span></p>
    <div class="real-run-panel"><div><strong>Ready to migrate for real?</strong><small>Select the destination explicitly. This action performs real writes; the walkthrough above did not.</small></div><select id="guided-target-connection">${targetConnectionOptions("mongodb")}</select><button id="guided-add-target" class="button button-secondary">Add target</button><button id="start-real-migration" class="button button-primary">Start real migration</button></div></section>`;
  byId("start-real-migration").addEventListener("click", () => startRealMigration(state.plan, byId("guided-target-connection").value));
  byId("guided-add-target").addEventListener("click", () => openTargetConnectionDialog("mongodb"));
}

async function startRealMigration(plan, targetConnectionId) {
  const target = state.connections.find((item) => item.connection_id === targetConnectionId && item.role === "target" && item.engine === plan.target_engine && String(item.database || "").trim());
  if (!target) {
    notify("Choose a valid destination connection before starting the migration.", "error");
    return;
  }
  if (!window.confirm(`Run a real migration now? Records will be written to ${target.name} (${target.database}). This cannot be treated as a visualization.`)) return;
  const button = byId("start-real-migration") || byId("run-generated-plan");
  if (button) { button.disabled = true; button.textContent = "Queueing migration…"; }
  const queued = await safeRun(() => post("/migrations", {
    plan_id: plan.plan_id, target_connection_id: target.connection_id,
    mode: "run", options: { verify_after: true },
  }));
  if (!queued) {
    if (button) { button.disabled = false; button.textContent = "Start real migration"; }
    return;
  }
  notify(`Real migration queued: ${queued.job_id}. Track its progress in Migration jobs.`);
  await safeRun(loadWorkspace);
  navigate("jobs");
}

function targetConnectionOptions(engine, selectedId = "") {
  const targets = state.connections.filter((item) => item.role === "target" && item.engine === engine && String(item.database || "").trim());
  const options = targets.map((item) => `<option value="${escapeHtml(item.connection_id)}"${item.connection_id === selectedId ? " selected" : ""}>${escapeHtml(item.name)} · ${escapeHtml(item.database)}${item.host ? ` · ${escapeHtml(item.host)}` : ""}</option>`).join("");
  return `<option value="">Choose a saved target connection…</option>${options}`;
}

function refreshPlanTargetOptions() {
  const engine = byId("plan-target-engine").value;
  byId("plan-target-connection").innerHTML = targetConnectionOptions(engine);
}

function openTargetConnectionDialog(engine) {
  navigate("connections");
  byId("connection-engine").value = engine;
  updateConnectionFields();
  byId("connection-role").value = "target";
  byId("connection-dialog").showModal();
}

function openPlanDialog(project) {
  byId("plan-project-id").value = project.project_id;
  const select = byId("plan-target-engine");
  const engines = project.direction === "RELATIONAL_TO_DOCUMENT" ? ["mongodb"] : ["mysql", "postgresql"];
  select.innerHTML = engines.map((engine) => `<option value="${engine}">${engine}</option>`).join("");
  refreshPlanTargetOptions();
  byId("plan-dialog").showModal();
}

async function submitPlan(event) {
  event.preventDefault();
  const project = state.selectedProject;
  if (!project) return;
  const targetConnectionId = byId("plan-target-connection").value;
  state.planTargetConnectionId = targetConnectionId;
  const button = event.submitter;
  button.disabled = true;
  const plan = await safeRun(() => post("/schema/generate", {
    project_id: project.project_id,
    target_engine: byId("plan-target-engine").value,
    if_revision: project.revision,
    options: { etl: { batch_size: Number(byId("plan-batch-size").value), on_drift: "widen", on_orphan: "DLQ", max_error_rate: 0.01 } },
    acknowledge_advisories: [],
  }));
  button.disabled = false;
  if (!plan) return;
  state.plan = plan;
  byId("plan-dialog").close();
  byId("plan-result").innerHTML = `<div class="demo-info"><span class="demo-icon">✓</span><div><strong>Plan generated · ${escapeHtml(plan.plan_id)}</strong><small>${escapeHtml(plan.plan_hash.slice(0, 28))} · target engine: ${escapeHtml(plan.target_engine)}</small></div><span class="demo-badge">READY</span></div>
    <p class="section-note">Choose the actual destination below. The plan has not written data; starting a real migration will write to the selected database.</p>
    <div class="plan-destination-row"><label>Migration destination<select id="plan-result-target-connection">${targetConnectionOptions(plan.target_engine, targetConnectionId)}</select></label><button id="plan-add-target" class="button button-secondary">Add target</button><button id="run-generated-plan" class="button button-primary">Start real migration</button></div>
    <details style="margin-top:12px"><summary class="text-button">Generated artifacts (${plan.artifacts?.length || 0})</summary><ul>${(plan.artifacts || []).map((item) => `<li class="monospace">${escapeHtml(item.name)}</li>`).join("")}</ul></details>`;
  byId("plan-result-target-connection").addEventListener("change", (event) => { state.planTargetConnectionId = event.target.value; });
  byId("plan-add-target").addEventListener("click", () => openTargetConnectionDialog(plan.target_engine));
  byId("run-generated-plan").addEventListener("click", () => startRealMigration(plan, byId("plan-result-target-connection").value));
  notify("Migration plan generated and saved.");
}

async function loadDatabaseBrowser() {
  const selectedId = state.selectedDatabase?.browser_id || readSavedLocation().databaseId;
  const result = await get("/database-browser");
  state.databases = result.items || [];
  setHtml("database-browser-list", databaseTable(state.databases));
  const unavailable = result.unavailable || [];
  setText("database-browser-notice", unavailable.length ? `${unavailable.length} saved connection(s) have expired credentials and are hidden until renewed.` : "Same engine, endpoint, and database are listed once even if registered for both source and target roles.");
  document.querySelectorAll("[data-browse-database]").forEach((button) => button.addEventListener("click", () => {
    const database = state.databases.find((item) => item.browser_id === button.dataset.browseDatabase);
    if (database) safeRun(() => openBrowserDatabase(database));
  }));
  if (selectedId) {
    const selected = state.databases.find((item) => item.browser_id === selectedId);
    if (selected) await openBrowserDatabase(selected);
  }
}

async function openBrowserDatabase(database) {
  state.selectedDatabase = database;
  saveLocation({ view: "database-browser", databaseId: database.browser_id });
  state.databaseObjects = [];
  state.currentObject = null;
  state.sampleOffset = 0;
  setText("database-browser-title", `${database.database} · ${database.engine}`);
  setHtml("database-browser-sample", "");
  setHtml("database-browser-objects", `<div class="empty-state">Loading database objects…</div>`);
  const result = await get(`/database-browser/${encodeURIComponent(database.connection_id)}/objects`);
  state.databaseObjects = result.items || [];
  setHtml("database-browser-objects", databaseObjectList(state.databaseObjects));
  document.querySelectorAll("[data-browse-object]").forEach((button) => button.addEventListener("click", () => safeRun(() => loadDatabaseSample(button.dataset.browseObject, 0, false))));
  const savedObject = readSavedLocation().objectName;
  if (savedObject && state.databaseObjects.some((item) => item.name === savedObject)) {
    await loadDatabaseSample(savedObject, 0, false);
  }
}

async function loadDatabaseSample(objectName, offset = 0, append = false) {
  const database = state.selectedDatabase;
  if (!database) return;
  state.currentObject = objectName;
  state.sampleOffset = offset;
  saveLocation({ view: "database-browser", databaseId: database.browser_id, objectName });
  const query = new URLSearchParams({ object_name: objectName, limit: "25", offset: String(offset) });
  const result = await get(`/database-browser/${encodeURIComponent(database.connection_id)}/sample?${query}`);
  const existing = append ? state.databaseSampleRows || [] : [];
  state.databaseSampleRows = [...existing, ...(result.items || [])];
  const rows = state.databaseSampleRows;
  setHtml("database-browser-sample", `<section class="browser-sample"><div class="browser-sample-heading"><strong>${escapeHtml(objectName)}</strong><small>${rows.length} row(s) shown · read-only preview</small></div>${rows.length ? rows.map((item, index) => `<pre><span class="browser-row-number">${index + 1}</span>${escapeHtml(JSON.stringify(item, null, 2))}</pre>`).join("") : `<div class="empty-state compact-empty">This object has no rows.</div>`}${result.has_more ? `<button id="database-browser-next" class="button button-secondary">Load next 25</button>` : ""}</section>`);
  byId("database-browser-next")?.addEventListener("click", () => safeRun(() => loadDatabaseSample(objectName, result.next_offset, true)));
}

function openSettings() {
  const settings = getSettings();
  byId("api-url").value = settings.baseUrl;
  byId("api-token").value = settings.token;
  byId("settings-dialog").showModal();
}

function saveSettingsFromForm(event) {
  event.preventDefault();
  saveSettings({ baseUrl: byId("api-url").value, token: byId("api-token").value });
  byId("settings-dialog").close();
  notify("API settings saved in this browser.");
  checkHealth();
  safeRun(loadWorkspace);
}

async function checkHealth() {
  const { baseUrl } = getSettings();
  try {
    const response = await fetch(`${baseUrl.replace(/\/api$/, "")}/healthz`);
    updateApiStatus(response.ok);
    if (!response.ok) throw new Error("API health check failed");
  } catch { updateApiStatus(false); }
}

function closeDialogOnButton(buttonId, dialogId) {
  byId(buttonId).addEventListener("click", () => byId(dialogId).close());
}

function wireEvents() {
  document.querySelectorAll("[data-view]").forEach((button) => button.addEventListener("click", () => navigate(button.dataset.view)));
  window.addEventListener("hashchange", () => {
    const requestedView = window.location.hash.slice(1);
    if (pageNames[requestedView]) navigate(requestedView, { syncHash: false });
  });
  document.querySelectorAll("[data-go]").forEach((button) => button.addEventListener("click", () => navigate(button.dataset.go)));
  ["refresh-overview", "projects-refresh", "jobs-refresh"].forEach((id) => byId(id).addEventListener("click", () => safeRun(loadWorkspace)));
  byId("connection-refresh").addEventListener("click", () => safeRun(() => loadWorkspace({ checkConnectionHealth: true })));
  byId("database-browser-refresh").addEventListener("click", () => safeRun(loadDatabaseBrowser));
  ["connect-demo", "connect-demo-secondary"].forEach((id) => byId(id).addEventListener("click", connectSqliteDemo));
  byId("new-connection-open").addEventListener("click", () => byId("connection-dialog").showModal());
  byId("analyze-open").addEventListener("click", openAnalyzeDialog);
  byId("settings-open").addEventListener("click", openSettings);
  byId("connection-engine").addEventListener("change", updateConnectionFields);
  byId("connection-form").addEventListener("submit", submitConnection);
  byId("analyze-form").addEventListener("submit", submitAnalysis);
  byId("plan-target-engine").addEventListener("change", refreshPlanTargetOptions);
  byId("plan-form").addEventListener("submit", submitPlan);
  byId("settings-save").addEventListener("click", saveSettingsFromForm);
  closeDialogOnButton("settings-cancel", "settings-dialog");
  closeDialogOnButton("connection-cancel", "connection-dialog");
  closeDialogOnButton("analyze-cancel", "analyze-dialog");
  closeDialogOnButton("plan-cancel", "plan-dialog");
  updateConnectionFields();
}

wireEvents();
const savedLocation = readSavedLocation();
const hashView = window.location.hash.slice(1);
const initialView = pageNames[hashView] ? hashView : (pageNames[savedLocation.view] ? savedLocation.view : "overview");
if (initialView !== "overview") navigate(initialView, { syncHash: false });
else saveLocation({ view: "overview" });
if (initialView === "projects" && savedLocation.projectId) safeRun(() => openProject(savedLocation.projectId));
checkHealth();
safeRun(loadWorkspace);
