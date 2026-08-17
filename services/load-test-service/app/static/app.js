const $=id=>document.getElementById(id);
const fmt=v=>new Intl.NumberFormat(undefined,{maximumFractionDigits:2}).format(v??0);

function render(d){
  const p=d.progress||{};
  $("progressText").textContent=`${p.message||""} (${p.current||0}/${p.total||0})`;
  $("progressBar").style.width=`${p.total?Math.round((p.current/p.total)*100):0}%`;

  $("state").textContent=d.running?"RUNNING":"IDLE";
  $("state").className=d.running?"state running":"state";

  const rows=d.results||[];
  $("results").innerHTML=rows.length?rows.map(r=>`
    <tr>
      <td>${fmt(r.target_rate)}</td>
      <td>${fmt(r.completed_per_sec)}</td>
      <td>${fmt(r.feature_p95_ms)} ms</td>
      <td>${fmt(r.feature_freshness_ms)} ms</td>
      <td>${fmt(r.inference_p95_ms)} ms</td>
      <td>${fmt(r.e2e_p50_ms)} ms</td>
      <td>${fmt(r.e2e_p95_ms)} ms</td>
      <td>${fmt(r.e2e_p99_ms)} ms</td>
      <td>${fmt(r.probe_errors)}</td>
      <td>${r.bottleneck}</td>
      <td class="${r.result}">${r.result}</td>
    </tr>
  `).join(""):'<tr><td colspan="11">No load-test results yet.</td></tr>';

  if(d.recommended){
    const r=d.recommended;
    $("recommendation").classList.remove("hidden");
    $("recRate").textContent=`${fmt(r.target_rate)} /sec`;
    $("recCompleted").textContent=fmt(r.completed_per_sec);
    $("recP95").textContent=`${fmt(r.e2e_p95_ms)} ms`;
    $("recFreshness").textContent=`${fmt(r.feature_freshness_ms)} ms`;
    $("recBottleneck").textContent=r.bottleneck==="None"?"No bottleneck at recommended rate":r.bottleneck;
  }else{
    $("recommendation").classList.add("hidden");
  }

  if(d.last_error){
    $("progressText").textContent=`ERROR: ${d.last_error}`;
  }
}

async function refresh(){
  try{
    const r=await fetch("/api/status");
    render(await r.json());
  }catch(e){}
}

$("run").onclick=async()=>{
  const r=await fetch("/api/start",{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({
      dataset:$("dataset").value,
      duration_seconds:Number($("duration").value)
    })
  });
  const d=await r.json();
  if(!r.ok) alert(d.detail||"Could not start load test");
  refresh();
};

$("stop").onclick=async()=>{
  await fetch("/api/stop",{method:"POST"});
  refresh();
};

$("reset").onclick=async()=>{
  const r=await fetch("/api/reset",{method:"POST"});
  const d=await r.json();
  if(!r.ok) alert(d.detail||"Could not reset");
  refresh();
};

refresh();
setInterval(refresh,1000);
