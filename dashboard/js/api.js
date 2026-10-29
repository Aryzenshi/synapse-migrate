/** HTTP boundary for the dashboard. Keeps auth headers and API errors consistent. */
const DEFAULT_API_URL = `${window.location.origin}/api`;

export function getSettings() {
  return {
    baseUrl: (localStorage.getItem("synapse.apiUrl") || DEFAULT_API_URL).replace(/\/$/, ""),
    token: localStorage.getItem("synapse.apiToken") || "dev-token",
  };
}

export function saveSettings({ baseUrl, token }) {
  localStorage.setItem("synapse.apiUrl", baseUrl.trim().replace(/\/$/, ""));
  localStorage.setItem("synapse.apiToken", token.trim());
}

/** Send one authenticated JSON request; convert problem responses into readable Error objects. */
export async function request(method, path, body) {
  const { baseUrl, token } = getSettings();
  let response;
  try {
    response = await fetch(`${baseUrl}${path}`, {
      method,
      headers: { Authorization: `Bearer ${token}`, ...(body ? { "Content-Type": "application/json" } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
  } catch {
    throw new Error(`Cannot reach the API at ${baseUrl}. Check that the service is running.`);
  }
  if (!response.ok) {
    let detail = `Request failed (${response.status})`;
    try {
      const problem = await response.json();
      detail = problem.detail || problem.message || problem.title || detail;
    } catch { /* A response without JSON still has a useful HTTP status. */ }
    throw new Error(detail);
  }
  if (response.status === 204) return null;
  return response.json();
}

export const get = (path) => request("GET", path);
export const post = (path, body) => request("POST", path, body);
export async function remove(path) {
  await request("DELETE", path);
  return { deleted: true };
}
