const $=id=>document.getElementById(id);
const fmt=v=>new Intl.NumberFormat().format(v??0);

function render(data){
  const p=data.progress;
  $("runState").textContent=data.running?p.phase:"IDLE";
  $("runState").className=data.running?"state running":"state";

  $("progressText").textContent=`${p.message} (${p.current}/${p.total})`;
  const pct=p.total?Math.round((p.current/p.total)*100):0;
  $("progressBar").style.width=`${pct}%`;

  const rows=data.results||[];
  $("results").innerHTML=rows.length?rows.map(r=>`
    <tr>
      <td>${r.consumers}</td>
      <td>${fmt(r.target_rate)}</td>
      <td>${fmt(r.produced_per_sec)}</td>
      <td>${fmt(r.processed_per_sec)}</td>
      <td>${fmt(r.lag)}</td>
      <td>${fmt(r.lag_growth_per_sec)}</td>
      <td>${fmt(r.errors)}</td>
      <td class="${r.result}">${r.result}</td>
    </tr>`).join(""):'<tr><td colspan="8">No results yet.</td></tr>';

  if(data.detected_limit){
    $("limitBox").classList.remove("hidden");
    $("limitText").textContent=data.detected_limit.message;
  }else{
    $("limitBox").classList.add("hidden");
  }

  if(data.recommended){
    $("recommendation").classList.remove("hidden");
    $("recConsumers").textContent=data.recommended.consumers;
    $("recTarget").textContent=fmt(data.recommended.target_rate);
    $("recProcessed").textContent=fmt(data.recommended.processed_per_sec);
    $("recLag").textContent=fmt(data.recommended.lag_growth_per_sec);
    $("recResult").textContent=data.recommended.result;
  }else{
    $("recommendation").classList.add("hidden");
  }
}

async function refresh(){
  try{
    const r=await fetch("/api/benchmark");
    render(await r.json());
  }catch(e){}
}

$("run").onclick=async()=>{
  const r=await fetch("/api/benchmark/start",{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({
      dataset:$("dataset").value,
      duration_seconds:Number($("duration").value)
    })
  });
  const b=await r.json();
  if(!r.ok) alert(b.detail||"Could not start");
  refresh();
};

$("stop").onclick=async()=>{await fetch("/api/stop",{method:"POST"});refresh()};
$("reset").onclick=async()=>{
  const r=await fetch("/api/reset",{method:"POST"});
  const b=await r.json();
  if(!r.ok) alert(b.detail||"Could not reset");
  refresh();
};

refresh();
setInterval(refresh,1000);
