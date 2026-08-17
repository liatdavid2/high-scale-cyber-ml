
const $ = id => document.getElementById(id);
const pct = v => `${(Number(v || 0) * 100).toFixed(2)}%`;
const metric = v => (v === null || v === undefined ? "N/A" : Number(v).toFixed(4));
const pvalue = v => Number(v || 0).toFixed(6);

async function refreshServing(){
  try{
    const r = await fetch("/api/serving");
    const d = await r.json();
    $("modelLoaded").textContent = d.model_loaded ? "YES" : "NO";
    $("p50").textContent = `${d.p50_ms || 0} ms`;
    $("p95").textContent = `${d.p95_ms || 0} ms`;
    $("p99").textContent = `${d.p99_ms || 0} ms`;
    $("throughput").textContent = `${d.throughput_rps || 0} rps`;
    $("errorRate").textContent = pct(d.error_rate);
  }catch(e){}
}

function renderDrift(d){
  $("driftPanel").classList.remove("hidden");
  $("baselineName").textContent = d.baseline_dataset;
  $("currentName").textContent = d.current_dataset;
  $("rowsCompared").textContent = Number(d.rows_compared).toLocaleString();
  $("overallDrift").textContent = d.overall_drift;
  $("overallDrift").className = d.overall_drift;
  $("highFeatures").textContent = d.high_features;
  $("mediumFeatures").textContent = d.medium_features;
  $("lowFeatures").textContent = d.low_features;

  $("driftResults").innerHTML = d.results.map(x => `
    <tr>
      <td>${x.feature}</td>
      <td>${Number(x.psi).toFixed(4)}</td>
      <td>${Number(x.ks_statistic).toFixed(4)}</td>
      <td>${pvalue(x.ks_pvalue)}</td>
      <td class="${x.drift_level}">${x.drift_level}</td>
    </tr>
  `).join("");
}

function renderQuality(q){
  if(!q || !q.available){
    $("qualityPanel").classList.add("hidden");
    return;
  }

  $("qualityPanel").classList.remove("hidden");
  $("prAuc").textContent = metric(q.pr_auc);
  $("rocAuc").textContent = metric(q.roc_auc);
  $("f1").textContent = metric(q.f1);
  $("recall").textContent = metric(q.recall);
  $("precision").textContent = metric(q.precision);
  $("qualityP95").textContent = `${q.p95_inference_ms} ms`;
  $("qualityLabel").textContent = q.label_column;
  $("qualityRows").textContent = Number(q.rows_evaluated).toLocaleString();
  $("fpr").textContent = metric(q.fpr);
  $("fnr").textContent = metric(q.fnr);
  $("confusion").textContent = `TN=${q.tn}, FP=${q.fp}, FN=${q.fn}, TP=${q.tp}`;
  $("qualityErrors").textContent = q.api_errors;
}

$("evaluate").onclick = async () => {
  const file = $("csvFile").files[0];
  if(!file){
    $("uploadStatus").textContent = "Please choose a CSV file first.";
    return;
  }

  $("evaluate").disabled = true;
  $("evaluate").textContent = "Evaluating...";
  $("uploadStatus").textContent = "Uploading CSV, validating schema and running monitoring evaluations...";

  const form = new FormData();
  form.append("file", file);

  try{
    const r = await fetch("/api/evaluate-csv", {
      method: "POST",
      body: form
    });
    const d = await r.json();
    if(!r.ok) throw new Error(d.detail || "Evaluation failed");

    $("fileInfo").classList.remove("hidden");
    $("fileName").textContent = d.file.name;
    $("fileRows").textContent = Number(d.file.rows).toLocaleString();
    $("fileColumns").textContent = d.file.columns;
    $("schemaValid").textContent = d.file.schema_valid ? "VALID" : "INVALID";
    $("schemaValid").className = d.file.schema_valid ? "LOW" : "HIGH";
    $("labelFound").textContent = d.file.label_found ? `FOUND (${d.file.label_column})` : "NOT FOUND";

    renderDrift(d.drift);
    renderQuality(d.quality);

    $("uploadStatus").textContent = d.file.label_found
      ? "Completed: drift + labeled model-quality evaluation."
      : "Completed: drift evaluation. No label column was found, so model-quality metrics were skipped.";
  }catch(e){
    $("uploadStatus").textContent = `ERROR: ${e.message}`;
    $("fileInfo").classList.add("hidden");
    $("driftPanel").classList.add("hidden");
    $("qualityPanel").classList.add("hidden");
  }finally{
    $("evaluate").disabled = false;
    $("evaluate").textContent = "Evaluate Current CSV";
  }
};

refreshServing();
setInterval(refreshServing, 3000);
