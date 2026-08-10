const $=id=>document.getElementById(id);
const fmt=v=>new Intl.NumberFormat().format(v??0);

function renderMetrics(m){
  $("targetRate").textContent=fmt(m.target_rate);
  $("producedRate").textContent=fmt(m.produced_per_sec);
  $("processedRate").textContent=fmt(m.processed_per_sec);
  $("lag").textContent=fmt(m.lag);
  $("consumerCount").textContent=m.consumer_count;
  $("errors").textContent=fmt(m.errors);
}

function renderBenchmark(b){
  const p=b.progress;
  $("runState").textContent=b.running?p.status:"IDLE";
  $("runState").className=b.running?"state running":"state stopped";
  $("progressText").textContent=`${p.message} (${p.current}/${p.total})`;
  $("progressBar").style.width=`${p.total?Math.round((p.current/p.total)*100):0}%`;

  const body=$("resultsBody");
  if(!b.results.length){
    body.innerHTML='<tr><td colspan="8">No benchmark results yet.</td></tr>';
  }else{
    body.innerHTML=b.results.map(r=>`
      <tr>
        <td>${r.consumers}</td>
        <td>${fmt(r.target_rate)}</td>
        <td>${fmt(r.produced_per_sec)}</td>
        <td>${fmt(r.processed_per_sec)}</td>
        <td>${fmt(r.lag)}</td>
        <td>${fmt(r.lag_growth_per_sec)}</td>
        <td>${fmt(r.errors)}</td>
        <td class="result-${r.result}">${r.result}</td>
      </tr>`).join("");
  }

  if(b.recommended){
    $("recommendation").classList.remove("hidden");
    $("recConsumers").textContent=b.recommended.consumers;
    $("recTarget").textContent=fmt(b.recommended.target_rate);
    $("recProcessed").textContent=fmt(b.recommended.processed_per_sec);
    $("recLagGrowth").textContent=fmt(b.recommended.lag_growth_per_sec);
    $("recResult").textContent=b.recommended.result;
  }else{
    $("recommendation").classList.add("hidden");
  }
}

async function refresh(){
  try{
    const [mres,bres]=await Promise.all([fetch("/api/metrics"),fetch("/api/benchmark")]);
    renderMetrics(await mres.json());
    renderBenchmark(await bres.json());
  }catch(e){}
}

$("runBenchmark").onclick=async()=>{
  const res=await fetch("/api/benchmark/start",{
    method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({
      dataset:$("benchDataset").value,
      duration_seconds:Number($("duration").value)
    })
  });
  const body=await res.json();
  if(!res.ok) alert(body.detail||"Could not start benchmark");
  refresh();
};

$("stopBenchmark").onclick=async()=>{
  await fetch("/api/stop",{method:"POST"});
  refresh();
};

$("reset").onclick=async()=>{
  const res=await fetch("/api/reset",{method:"POST"});
  const body=await res.json();
  if(!res.ok) alert(body.detail||"Could not reset");
  refresh();
};

$("manualStart").onclick=async()=>{
  const res=await fetch("/api/start",{
    method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({
      dataset:$("benchDataset").value,
      target_rate:Number($("rate").value),
      consumers:Number($("consumers").value)
    })
  });
  const body=await res.json();
  if(!res.ok) alert(body.detail||"Could not start");
  refresh();
};

$("manualStop").onclick=async()=>{
  await fetch("/api/stop",{method:"POST"});
  refresh();
};

refresh();
setInterval(refresh,1000);
