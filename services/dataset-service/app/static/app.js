const statusEl = document.getElementById("status");
const summaryEl = document.getElementById("summary");
const filesEl = document.getElementById("files");
const classesEl = document.getElementById("classes");
const qualityEl = document.getElementById("quality");
const schemaEl = document.getElementById("schema");
const runBtn = document.getElementById("run");

const fmt = (value) => new Intl.NumberFormat().format(value ?? 0);

function card(label, value) {
  return `
    <div class="card">
      <div class="card-label">${label}</div>
      <div class="card-value">${value}</div>
    </div>
  `;
}

function renderFiles(files) {
  filesEl.innerHTML = files.map(file => `
    <div class="file-block">
      <div class="file-title">${file.file}</div>
      <table>
        <tbody>
          <tr><th>Rows</th><td>${fmt(file.rows)}</td></tr>
          <tr><th>Columns</th><td>${fmt(file.columns)}</td></tr>
          <tr><th>Label column</th><td>${file.label_column ?? "Not detected"}</td></tr>
          <tr><th>Memory</th><td>${file.memory_mb} MB</td></tr>
        </tbody>
      </table>
    </div>
  `).join("");
}

function renderClasses(files) {
  classesEl.innerHTML = files.map(file => {
    const rows = Object.entries(file.class_distribution || {});

    if (!rows.length) {
      return `
        <div class="file-block">
          <div class="file-title">${file.file}</div>
          <p class="small">No class column detected.</p>
        </div>
      `;
    }

    return `
      <div class="file-block">
        <div class="file-title">${file.file}</div>
        <table>
          <thead>
            <tr><th>Class</th><th>Rows</th><th>Share</th></tr>
          </thead>
          <tbody>
            ${rows.map(([name, count]) => `
              <tr>
                <td>${name}</td>
                <td>${fmt(count)}</td>
                <td>${((count / file.rows) * 100).toFixed(2)}%</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    `;
  }).join("");
}

function renderQuality(files) {
  qualityEl.innerHTML = files.map(file => {
    const missing = Object.entries(file.missing_by_column || {});

    return `
      <div class="file-block">
        <div class="file-title">${file.file}</div>
        <table>
          <tbody>
            <tr>
              <th>Missing cells</th>
              <td class="${file.missing_cells === 0 ? "ok" : "warn"}">${fmt(file.missing_cells)}</td>
            </tr>
            <tr>
              <th>Duplicate rows</th>
              <td class="${file.duplicate_rows === 0 ? "ok" : "warn"}">${fmt(file.duplicate_rows)}</td>
            </tr>
            <tr>
              <th>Detected classes</th>
              <td>${file.classes ?? "N/A"}</td>
            </tr>
          </tbody>
        </table>
        ${
          missing.length
            ? `<p class="small">Columns with missing values: ${missing.map(([k,v]) => `${k} (${fmt(v)})`).join(", ")}</p>`
            : `<p class="small ok">No missing values detected.</p>`
        }
      </div>
    `;
  }).join("");
}

function renderSchema(files) {
  schemaEl.innerHTML = files.map(file => `
    <div class="file-block">
      <div class="file-title">${file.file}</div>
      <p class="small">${file.column_names.map(name => `<code>${name}</code>`).join(" ")}</p>
    </div>
  `).join("");
}

async function loadProfile() {
  statusEl.className = "status";
  statusEl.textContent = "Running dataset evaluation…";

  summaryEl.innerHTML = "";
  filesEl.innerHTML = "";
  classesEl.innerHTML = "";
  qualityEl.innerHTML = "";
  schemaEl.innerHTML = "";

  try {
    const response = await fetch("/api/profile");
    const body = await response.json();

    if (!response.ok) {
      throw new Error(body.detail || "Evaluation failed");
    }

    const totalColumns = body.files.reduce((sum, f) => sum + f.columns, 0);
    const totalMissing = body.files.reduce((sum, f) => sum + f.missing_cells, 0);
    const totalDuplicates = body.files.reduce((sum, f) => sum + f.duplicate_rows, 0);

    summaryEl.innerHTML =
      card("Files", fmt(body.total_files)) +
      card("Total Rows", fmt(body.total_rows)) +
      card("Columns", fmt(totalColumns)) +
      card("Missing Cells", fmt(totalMissing)) +
      card("Duplicate Rows", fmt(totalDuplicates));

    renderFiles(body.files);
    renderClasses(body.files);
    renderQuality(body.files);
    renderSchema(body.files);

    statusEl.textContent = "Evaluation completed successfully.";
  } catch (err) {
    statusEl.className = "status error";
    statusEl.innerHTML = `
      <strong>Dataset not ready.</strong><br>
      ${err.message}<br><br>
      Copy the UNSW-NB15 CSV files into
      <code>shared/data/raw/</code>
      and run again.
    `;
  }
}

runBtn.addEventListener("click", loadProfile);
loadProfile();
