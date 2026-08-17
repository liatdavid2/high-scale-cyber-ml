
const $ = id => document.getElementById(id);
const pct = v => `${(Number(v) * 100).toFixed(2)}%`;
const num = v => Number(v).toFixed(4);
const signed = v => {
  const n = Number(v);
  return `${n >= 0 ? "+" : ""}${n.toFixed(4)}`;
};

async function refreshServing(){
  try{
    const r = await fetch("/api/serving");
    const d = await r.json();
    $("modelLoaded").textContent = d.model_loaded ? "YES" : "NO";
    $("p50").textContent = `${d.p50_ms || 0} ms`;
    $("p95").textContent = `${d.p95_ms || 0} ms`;
    $("p99").textContent = `${d.p99_ms || 0} ms`;
    $("throughput").textContent = `${d.throughput_rps || 0} rps`;
    $("errorRate").textContent = pct(d.error_rate || 0);
    $("modelUri").textContent = d.model_uri || "-";
    $("requests").textContent = d.requests || 0;
    $("success").textContent = d.success || 0;
  }catch(e){}
}

$("runDrift").onclick = async () => {
  $("runDrift").disabled = true;
  $("runDrift").textContent = "Running...";
  $("driftStatus").textContent = "Reading training/testing CSVs and calculating PSI + KS...";

  try{
    const rows = Number($("driftRows").value);
    const r = await fetch("/api/drift", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({sample_size: rows})
    });
    const d = await r.json();
    if(!r.ok) throw new Error(d.detail || "Drift evaluation failed");

    $("driftSummary").classList.remove("hidden");
    $("overallDrift").textContent = d.overall_drift;
    $("overallDrift").className = d.overall_drift;
    $("baselineRows").textContent = d.baseline_rows.toLocaleString();
    $("currentRows").textContent = d.current_rows.toLocaleString();
    $("highFeatures").textContent = d.high_features;
    $("mediumFeatures").textContent = d.medium_features;
    $("lowFeatures").textContent = d.low_features;

    $("driftResults").innerHTML = d.results.map(x => `
      <tr>
        <td>${x.feature}</td>
        <td>${x.psi}</td>
        <td>${x.ks_statistic}</td>
        <td>${x.ks_pvalue}</td>
        <td class="${x.drift_level}">${x.drift_level}</td>
      </tr>
    `).join("");

    $("driftStatus").textContent =
      `Compared ${d.baseline_rows.toLocaleString()} rows from ${d.baseline_dataset} with ${d.current_rows.toLocaleString()} rows from ${d.current_dataset}.`;
  }catch(e){
    $("driftStatus").textContent = `ERROR: ${e.message}`;
  }finally{
    $("runDrift").disabled = false;
    $("runDrift").textContent = "Run Drift Evaluation";
  }
};

$("runQuality").onclick = async () => {
  $("runQuality").disabled = true;
  $("runQuality").textContent = "Running...";
  $("qualityStatus").textContent = "Sending labeled CSV samples through the deployed Inference API...";

  try{
    const rows = Number($("qualityRows").value);
    const r = await fetch("/api/quality", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({baseline_rows: rows, current_rows: rows})
    });
    const d = await r.json();
    if(!r.ok) throw new Error(d.detail || "Quality evaluation failed");

    $("qualityPanel").classList.remove("hidden");

    const labels = {
      pr_auc: "PR-AUC",
      roc_auc: "ROC-AUC",
      f1: "F1",
      recall: "Recall",
      precision: "Precision",
      fpr: "False Positive Rate",
      fnr: "False Negative Rate"
    };

    $("qualityResults").innerHTML = Object.keys(labels).map(k => {
      const badWhenHigher = k === "fpr" || k === "fnr";
      const delta = Number(d.change[k]);
      const cls = delta === 0 ? "" : ((badWhenHigher ? delta < 0 : delta > 0) ? "GOOD" : "BAD");
      return `
        <tr>
          <td>${labels[k]}</td>
          <td>${num(d.baseline[k])}</td>
          <td>${num(d.current[k])}</td>
          <td class="${cls}">${signed(delta)}</td>
        </tr>
      `;
    }).join("");

    $("qPr").textContent = num(d.current.pr_auc);
    $("qRoc").textContent = num(d.current.roc_auc);
    $("qF1").textContent = num(d.current.f1);
    $("qRecall").textContent = num(d.current.recall);
    $("qFpr").textContent = num(d.current.fpr);
    $("qFnr").textContent = num(d.current.fnr);
    $("confusion").textContent = `TN=${d.current.tn}, FP=${d.current.fp}, FN=${d.current.fn}, TP=${d.current.tp}`;
    $("qualityErrors").textContent = d.current_api_errors;
    $("qualityP95").textContent = `${d.current_p95_ms} ms`;

    $("qualityStatus").textContent =
      `Reference: ${d.baseline_rows.toLocaleString()} labeled training rows. Current: ${d.current_rows.toLocaleString()} labeled testing rows.`;
  }catch(e){
    $("qualityStatus").textContent = `ERROR: ${e.message}`;
  }finally{
    $("runQuality").disabled = false;
    $("runQuality").textContent = "Run Labeled Quality Check";
  }
};

refreshServing();
setInterval(refreshServing, 3000);
