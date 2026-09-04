"use strict";(()=>{var n={hello:null,snapshot:null,triggers:[],series:{ticks:[],fires:[],live:[],venues:{}}},D=120;function b(e,t){e.push(t),e.length>D&&e.shift()}function $(e){n.series.ticks=[...e.t],n.series.fires=[...e.f],n.series.live=[...e.l],n.series.venues=Object.fromEntries(Object.entries(e.v).map(([t,r])=>[t,[...r]]))}var k={};function R(e,t){let r=n.series.venues[e]??=[],a=k[e]===void 0?0:t-k[e];k[e]=t,b(r,a)}function E(e){b(n.series.ticks,e.ticks_per_sec),b(n.series.fires,e.triggers_per_sec),b(n.series.live,e.engine.live);for(let[t,r]of Object.entries(e.venue_ticks))R(t,r)}function S(e){for(let t of e)n.triggers.unshift(t);n.triggers.length>10&&(n.triggers.length=10)}var c=e=>e>=1e9?`${(e/1e9).toFixed(1)}B`:e>=1e6?`${(e/1e6).toFixed(1)}M`:e>=1e4?`${(e/1e3).toFixed(1)}k`:e.toLocaleString();function T(e){let t=Math.floor(e/3600),r=Math.floor(e%3600/60),a=Math.floor(e%60);return t>0?`${t}h ${r}m`:r>0?`${r}m ${a}s`:`${a}s`}function H(e){return`<span class="dot ${e?"on":"off"}"></span>`}function M(e){if(e.length<2)return"";let t=100,r=28,a=Math.max(...e,1),p=e.map((g,u)=>`${u/(e.length-1)*t},${r-g/a*r}`);return`<svg viewBox="0 0 ${t} ${r}" preserveAspectRatio="none" role="img" aria-label="ticks per second, last two minutes">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="${p.join(" ")}"/>
  </svg>`}function x(e,t=100,r=16){if(e.length<2)return"";let a=Math.max(...e,1),p=Math.min(...e,0),g=a-p||1,u=e.map((v,f)=>{let l=f/(e.length-1)*t,d=r-(v-p)/g*r;return`${l.toFixed(1)},${d.toFixed(1)}`});return`<svg viewBox="0 0 ${t} ${r}" preserveAspectRatio="none" class="trend">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points="${u.join(" ")}"/>
  </svg>`}function _(e){if(e.length<2)return"\u2014";let t=e[e.length-1]-e[e.length-2];return t>0?"\u25B2":t<0?"\u25BC":"\u2014"}function q(e){return e.innerHTML=`
  <header class="topbar">
    <span class="brand">chrono<span class="brand-dot">\u25B8</span></span>
    <span class="status mono">
      <span id="dot-feed"></span> feed
      <span id="dot-nats"></span> nats
      <span class="sep"></span>
      <span class="mono" id="published">\u2014</span> pub
      \xB7 <span class="mono" id="pubdropped">\u2014</span> drop
    </span>
  </header>
  <main class="grid">
    <section class="card area-pulse"><h2>pulse</h2>
      <div class="big mono" id="tickrate">\u2014</div>
      <div class="sub">ticks/s</div>
      <div id="spark"></div>
      <div class="duo"><span class="mono" id="ticks">\u2014</span> ticks
        \xB7 <span class="mono" id="ticksdropped">\u2014</span> dropped</div>
    </section>
    <section class="card area-fires fires"><h2>fires</h2>
      <div class="big mono" id="fired">\u2014</div>
      <div class="sub">triggers fired \xB7 <span id="firesarrow">\u2014</span></div>
      <div id="firesspark"></div>
    </section>
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile"><div class="big mono" id="st-active">\u2014</div><div class="sub">active</div></div>
        <div class="tile"><div class="big mono" id="st-triggered">\u2014</div><div class="sub">triggered</div></div>
        <div class="tile"><div class="big mono" id="st-cancelled">\u2014</div><div class="sub">cancelled</div></div>
      </div>
      <div class="livetrend"><span class="lbl">live alerts</span>
        <span class="mono" id="live">\u2014</span><span id="livetrendline"></span></div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div id="engine" class="rows"></div></section>
    <section class="card area-stream rail"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger\u2026</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`,{inquiry:document.getElementById("inquiry")}}function L(){let e=n.snapshot,t=(l,d)=>{let i=document.getElementById(l);i&&(i.textContent=d)},r=(l,d)=>{let i=document.getElementById(l);i&&(i.innerHTML=H(d))};if(r("dot-feed",e?e.feed_ever_connected&&e.feed_last_seen_ms_ago<1e4:!1),r("dot-nats",!!e?.nats_connected),!e)return;t("published",c(e.triggers_published)),t("pubdropped",c(e.triggers_publish_dropped)),t("tickrate",c(e.ticks_per_sec)),t("ticks",c(e.ticks)),t("ticksdropped",c(e.ticks_dropped));let a=document.getElementById("spark");a&&(a.innerHTML=M(n.series.ticks)),t("fired",c(e.triggers_fired)),t("firesarrow",_(n.series.fires));let p=document.getElementById("firesspark");p&&(p.innerHTML=x(n.series.fires)),t("st-active",String(e.alerts_by_state.active??0)),t("st-triggered",String(e.alerts_by_state.triggered??0)),t("st-cancelled",String(e.alerts_by_state.cancelled??0)),t("live",c(e.engine.live));let g=document.getElementById("livetrendline");g&&(g.innerHTML=x(n.series.live));let u=document.getElementById("venues");if(u){let l=Object.entries(e.venue_ticks).sort((i,m)=>m[1]-i[1]),d=l[0]?.[1]||1;u.innerHTML=l.map(([i,m])=>{let o=n.series.venues[i]??[];return`<div class="row">
          <span class="vname">${i} <span class="trendmark">${_(o)}</span></span>
          <span class="vmeta">${x(o.slice(-40))}</span>
          <span class="bar"><i style="width:${100*m/d}%"></i></span>
          <span class="mono">${c(m)}</span>
        </div>`}).join("")||'<div class="empty">no ticks yet</div>'}let v=document.getElementById("engine");v&&(v.innerHTML=`
      <div class="row"><span>ring drops</span><span class="mono">${c(e.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${n.hello?c(n.hello.symbol_count):"\u2014"}</span></div>
      <div class="row"><span>uptime</span><span class="mono">${T(e.uptime_sec)}</span></div>`);let f=document.getElementById("stream");if(f){let l=n.triggers[0],d=l?`${n.triggers.length}:${l.alert_id}:${l.fired_at_unix_nanos}`:"";d!==f.dataset.key&&(f.dataset.key=d,f.innerHTML=n.triggers.length===0?'<div class="empty">waiting for the first trigger\u2026</div>':n.triggers.map(i=>`<div class="trg">
                <span class="badge ${i.direction==="ABOVE"?"up":"down"}">${i.direction}</span>
                <span class="sym">${i.symbol}</span>
                <span class="mono">${i.fired_price}</span>
                <span class="meta">${i.venue}/${i.tier}</span>
                <span class="mono time">${new Date(i.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span>
              </div>`).join(""))}}async function B(e){let t=new URLSearchParams(Object.entries(e).filter(([,a])=>a!=="")),r=await fetch(`/api/alerts?${t.toString()}`);if(!r.ok)throw new Error(`alerts: HTTP ${r.status}`);return await r.json()}var w=25,F=["active","triggered","cancelled"],h=null;function A(e){h?.(e)}function z(e){let t=0,r=null;e.innerHTML=`
    <h2>alert inquiry</h2>
    <form class="filters" id="f">
      <input name="symbol" placeholder="symbol (e.g. BTCUSDT)">
      <select name="state"><option value="">state: any</option>
        ${F.map(o=>`<option>${o}</option>`).join("")}</select>
      <select name="venue"><option value="">venue: any</option></select>
      <select name="tier"><option value="">tier: any</option></select>
      <select name="direction"><option value="">direction: any</option>
        <option>ABOVE</option><option>BELOW</option></select>
      <button type="submit">query</button>
    </form>
    <table class="tbl"><thead><tr>
      <th>symbol</th><th>dims</th><th>type</th><th>dir</th><th>target</th>
      <th>state</th><th>created</th><th></th>
    </tr></thead><tbody id="rows"></tbody></table>
    <div class="pager"><button id="prev">\u2190 prev</button>
      <span id="pg" class="mono"></span>
      <button id="next">next \u2192</button></div>`;let a=e.querySelector("form"),p=e.querySelector("#rows"),g=e.querySelector("#pg"),u=e.querySelector("#prev"),v=e.querySelector("#next"),f=e.querySelector('select[name="venue"]'),l=e.querySelector('select[name="tier"]'),d=(o,s)=>{let y=o.value;o.innerHTML=`<option value="">${o.name}: any</option>`+s.map(j=>`<option>${j}</option>`).join(""),o.value=y};h=o=>{d(f,o.venues),d(l,o.tiers),h=null},n.hello&&h(n.hello);let i=()=>{u.disabled=t===0,v.disabled=!(r&&t+r.items.length<r.total)};async function m(){p.innerHTML='<tr><td colspan="8" class="empty">querying\u2026</td></tr>';let o={limit:String(w),offset:String(t)};new FormData(a).forEach((s,y)=>o[y]=String(s));try{r=await B(o)}catch(s){r=null,p.innerHTML=`<tr><td colspan="8" class="empty err">query failed \u2014 ${s instanceof Error?s.message:String(s)}</td></tr>`,g.textContent="",i();return}p.innerHTML=r.items.length===0?'<tr><td colspan="8" class="empty">no alerts match</td></tr>':r.items.map(s=>`<tr>
              <td>${s.symbol}</td><td class="meta">${s.venue}/${s.tier}</td>
              <td class="meta">${s.price_type}</td>
              <td><span class="badge ${s.direction==="ABOVE"?"up":"down"}">${s.direction}</span></td>
              <td class="mono">${s.target_price}</td>
              <td><span class="chip state-${s.state}">${s.state}</span></td>
              <td class="mono time">${new Date(s.created_at_unix_nanos/1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${s.id.slice(0,8)}</td>
            </tr>`).join(""),g.textContent=r.items.length===0?`0 of ${r.total}`:`${t+1}\u2013${t+r.items.length} of ${r.total}`,i()}a.addEventListener("submit",o=>{o.preventDefault(),t=0,m()}),u.addEventListener("click",()=>{t>0&&(t=Math.max(0,t-w),m())}),v.addEventListener("click",()=>{r&&t+r.items.length<r.total&&(t+=w,m())}),i(),m()}function I(){z(q(document.getElementById("app")).inquiry);let e=new EventSource("/api/stream");e.onmessage=t=>{let r=JSON.parse(t.data);switch(r.type){case"hello":n.hello=r,$(n.hello.history),A(n.hello);break;case"snapshot":n.snapshot=r.snapshot,E(n.snapshot);break;case"triggers":S(r.triggers);break}L()}}I();})();
