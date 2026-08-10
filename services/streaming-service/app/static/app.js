const fmt = value => new Intl.NumberFormat().format(value ?? 0);

const $ = id => document.getElementById(id);

const elements = {
  state: $("runState"),
  message: $("message"),
  dataset: $("dataset"),
  rate: $("rate"),
  targetRate: $("targetRate"),
  producedRate: $("producedRate"),
  processedRate: $("processedRate"),
  lag: $("lag"),
  lag2: $("lag2"),
  errors: $("errors"),
  errorRate: $("errorRate"),
  producerStatus: $("producerStatus"),
  consumerStatus: $("consumerStatus"),
  kafkaStatus: $("kafkaStatus"),
  datasetStatus: $("datasetStatus"),
  datasetRows: $("datasetRows"),
  topic: $("topic"),
  uptime: $("uptime"),
  totalProduced: $("totalProduced"),
  totalProcessed: $("totalProcessed"),
  producerErrors: $("producerErrors"),
  consumerErrors: $("consumerErrors"),
};

function setMessage(text, error = false) {
  elements.message.textContent = text;
  elements.message.className = error ? "message error" : "message";
}

function render(m) {
  elements.state.textContent = m.running ? "RUNNING" : "STOPPED";
  elements.state.className = m.running ? "state running" : "state stopped";

  elements.targetRate.textContent = fmt(m.target_rate);
  elements.producedRate.textContent = fmt(m.produced_per_sec);
  elements.processedRate.textContent = fmt(m.processed_per_sec);
  elements.lag.textContent = fmt(m.lag);
  elements.lag2.textContent = fmt(m.lag);
  elements.errors.textContent = fmt(m.errors);
  elements.errorRate.textContent = `${m.error_rate_pct}%`;

  elements.producerStatus.textContent = m.producer_status;
  elements.consumerStatus.textContent = m.consumer_status;
  elements.kafkaStatus.textContent = m.kafka_status;
  elements.datasetStatus.textContent = m.dataset;
  elements.datasetRows.textContent = fmt(m.dataset_rows);
  elements.topic.textContent = m.topic;
  elements.uptime.textContent = `${fmt(m.uptime_seconds)}s`;

  elements.totalProduced.textContent = fmt(m.total_produced);
  elements.totalProcessed.textContent = fmt(m.total_processed);
  elements.producerErrors.textContent = fmt(m.producer_errors);
  elements.consumerErrors.textContent = fmt(m.consumer_errors);

  if (m.last_error) {
    setMessage(m.last_error, true);
  }
}

async function metrics() {
  try {
    const res = await fetch("/api/metrics");
    const body = await res.json();
    if (!res.ok) throw new Error(body.detail || "Could not load metrics");
    render(body);
  } catch (err) {
    setMessage(err.message, true);
  }
}

$("start").addEventListener("click", async () => {
  setMessage("Starting streaming...");
  try {
    const res = await fetch("/api/start", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        dataset: elements.dataset.value,
        target_rate: Number(elements.rate.value),
      }),
    });
    const body = await res.json();
    if (!res.ok) throw new Error(body.detail || "Could not start");
    setMessage(`Streaming started at target ${fmt(body.target_rate)} events/sec.`);
    await metrics();
  } catch (err) {
    setMessage(err.message, true);
  }
});

$("stop").addEventListener("click", async () => {
  setMessage("Stopping...");
  try {
    const res = await fetch("/api/stop", {method: "POST"});
    const body = await res.json();
    if (!res.ok) throw new Error(body.detail || "Could not stop");
    setMessage("Streaming stopped.");
    await metrics();
  } catch (err) {
    setMessage(err.message, true);
  }
});

$("reset").addEventListener("click", async () => {
  try {
    const res = await fetch("/api/reset", {method: "POST"});
    const body = await res.json();
    if (!res.ok) throw new Error(body.detail || "Could not reset");
    setMessage("Metrics reset.");
    await metrics();
  } catch (err) {
    setMessage(err.message, true);
  }
});

metrics();
setInterval(metrics, 1000);
