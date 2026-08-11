
const $ = id => document.getElementById(id);
const fmt = v => new Intl.NumberFormat().format(v ?? 0);

function msg(text, error=false){
  $("message").textContent = text;
  $("message").className = error ? "message error" : "message";
}

function renderMetrics(m){
  $("eventsRate").textContent = fmt(m.events_per_sec);
  $("featuresRate").textContent = fmt(m.features_per_sec);
  $("redisRate").textContent = fmt(m.redis_writes_per_sec);
  $("p95").textContent = fmt(m.latency_p95_ms);
  $("p99").textContent = fmt(m.latency_p99_ms);
  $("errors").textContent = fmt(m.errors);

  $("consumerStatus").textContent = m.consumer_status;
  $("kafkaStatus").textContent = m.kafka_status;
  $("redisStatus").textContent = m.redis_status;
  $("topic").textContent = m.topic;
  $("group").textContent = m.group_id;
  $("freshness").textContent = `${fmt(m.feature_freshness_ms)} ms`;
  $("uptime").textContent = `${fmt(m.uptime_seconds)}s`;
}

function renderBenchmark(b){
  const p = b.progress;
  $("progressText").textContent = `${p.message} (${p.current}/${p.total})`;

  const pct = p.total ? Math.round((p.current / p.total) * 100) : 0;
  $("progressBar").style.width = `${pct}%`;

  const rows = b.results || [];
  $("benchmarkResults").innerHTML = rows.length
    ? rows.map(r => `
      <tr>
        <td>${fmt(r.input_rate)}</td>
        <td>${fmt(r.features_per_sec)}</td>
        <td>${fmt(r.redis_writes_per_sec)}</td>
        <td>${fmt(r.p50_ms)} ms</td>
        <td>${fmt(r.p95_ms)} ms</td>
        <td>${fmt(r.p99_ms)} ms</td>
        <td>${fmt(r.freshness_ms)} ms</td>
        <td>${fmt(r.errors)}</td>
        <td class="${r.result}">${r.result}</td>
      </tr>
    `).join("")
    : '<tr><td colspan="9">No benchmark results yet.</td></tr>';

  if(b.recommended){
    $("recommendation").classList.remove("hidden");
    $("recRate").textContent = fmt(b.recommended.input_rate);
    $("recFeatures").textContent = fmt(b.recommended.features_per_sec);
    $("recP95").textContent = `${fmt(b.recommended.p95_ms)} ms`;
    $("recFreshness").textContent = `${fmt(b.recommended.freshness_ms)} ms`;
    $("recResult").textContent = b.recommended.result;
  }else{
    $("recommendation").classList.add("hidden");
  }

  if(b.running){
    $("runState").textContent = "BENCHMARK";
    $("runState").className = "state running";
  }
}

async function refresh(){
  try{
    const [mres, bres] = await Promise.all([
      fetch("/api/metrics"),
      fetch("/api/benchmark")
    ]);

    const m = await mres.json();
    const b = await bres.json();

    renderMetrics(m);
    renderBenchmark(b);

    if(!m.running && !b.running){
      $("runState").textContent = "IDLE";
      $("runState").className = "state stopped";
    }else if(m.running && !b.running){
      $("runState").textContent = "RUNNING";
      $("runState").className = "state running";
    }

    if(m.last_error && !b.running){
      msg(m.last_error, true);
    }
  }catch(e){}
}

$("runBenchmark").onclick = async () => {
  const r = await fetch("/api/benchmark/start", {
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({
      dataset:$("benchDataset").value,
      duration_seconds:Number($("duration").value)
    })
  });

  const b = await r.json();
  if(!r.ok) alert(b.detail || "Could not start benchmark");
  refresh();
};

$("stopBenchmark").onclick = async () => {
  await fetch("/api/stop", {method:"POST"});
  refresh();
};

$("resetBenchmark").onclick = async () => {
  if((await fetch("/api/metrics")).ok){
    const r = await fetch("/api/reset", {method:"POST"});
    const b = await r.json();
    if(!r.ok) alert(b.detail || "Could not reset");
  }
  refresh();
};

$("start").onclick = async () => {
  const r = await fetch("/api/start", {method:"POST"});
  const b = await r.json();
  if(!r.ok) msg(b.detail || "Could not start", true);
  else msg("Feature processing started.");
  refresh();
};

$("stop").onclick = async () => {
  await fetch("/api/stop", {method:"POST"});
  msg("Feature processing stopped.");
  refresh();
};

$("reset").onclick = async () => {
  const r = await fetch("/api/reset", {method:"POST"});
  const b = await r.json();
  if(!r.ok) msg(b.detail || "Could not reset", true);
  else msg("Metrics reset.");
  refresh();
};

refresh();
setInterval(refresh, 1000);
