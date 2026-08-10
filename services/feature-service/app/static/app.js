const $=id=>document.getElementById(id);
const fmt=v=>new Intl.NumberFormat().format(v??0);

function message(text,error=false){
  $("message").textContent=text;
  $("message").className=error?"message error":"message";
}

function render(m){
  $("runState").textContent=m.running?"RUNNING":"STOPPED";
  $("runState").className=m.running?"state running":"state stopped";

  $("eventsRate").textContent=fmt(m.events_per_sec);
  $("featuresRate").textContent=fmt(m.features_per_sec);
  $("redisRate").textContent=fmt(m.redis_writes_per_sec);
  $("p95").textContent=fmt(m.latency_p95_ms);
  $("p99").textContent=fmt(m.latency_p99_ms);
  $("errors").textContent=fmt(m.errors);

  $("consumerStatus").textContent=m.consumer_status;
  $("kafkaStatus").textContent=m.kafka_status;
  $("redisStatus").textContent=m.redis_status;
  $("topic").textContent=m.topic;
  $("group").textContent=m.group_id;
  $("freshness").textContent=`${fmt(m.feature_freshness_ms)} ms`;
  $("uptime").textContent=`${fmt(m.uptime_seconds)}s`;

  $("totalEvents").textContent=fmt(m.total_events);
  $("totalFeatures").textContent=fmt(m.total_features);
  $("redisWrites").textContent=fmt(m.redis_writes);
  $("p50Table").textContent=`${fmt(m.latency_p50_ms)} ms`;
  $("p95Table").textContent=`${fmt(m.latency_p95_ms)} ms`;
  $("p99Table").textContent=`${fmt(m.latency_p99_ms)} ms`;

  if(m.last_error) message(m.last_error,true);
}

async function refresh(){
  try{
    const r=await fetch("/api/metrics");
    render(await r.json());
  }catch(e){}
}

$("start").onclick=async()=>{
  const r=await fetch("/api/start",{method:"POST"});
  const b=await r.json();
  if(!r.ok) message(b.detail||"Could not start",true);
  else message("Feature processing started. Generate traffic from Stage 2.");
  refresh();
};

$("stop").onclick=async()=>{
  const r=await fetch("/api/stop",{method:"POST"});
  const b=await r.json();
  if(!r.ok) message(b.detail||"Could not stop",true);
  else message("Feature processing stopped.");
  refresh();
};

$("reset").onclick=async()=>{
  const r=await fetch("/api/reset",{method:"POST"});
  const b=await r.json();
  if(!r.ok) message(b.detail||"Could not reset",true);
  else message("Metrics reset.");
  refresh();
};

refresh();
setInterval(refresh,1000);
