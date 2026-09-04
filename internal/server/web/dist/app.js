"use strict";(()=>{var s={hello:null,snapshot:null,triggers:[]},f=[];function k(e){s.triggers.unshift(e),s.triggers.length>200&&s.triggers.pop()}var d=e=>e>=1e9?`${(e/1e9).toFixed(1)}B`:e>=1e6?`${(e/1e6).toFixed(1)}M`:e>=1e4?`${(e/1e3).toFixed(1)}k`:e.toLocaleString();function _(e){let r=Math.floor(e/3600),t=Math.floor(e%3600/60),l=Math.floor(e%60);return r>0?`${r}h ${t}m`:t>0?`${t}m ${l}s`:`${l}s`}function w(e){return`<span class="dot ${e?"on":"off"}"></span>`}function $(e){if(e.length<2)return"";let r=100,t=28,l=Math.max(...e,1),g=e.map((m,c)=>`${c/(e.length-1)*r},${t-m/l*t}`);return`<svg viewBox="0 0 ${r} ${t}" preserveAspectRatio="none" role="img" aria-label="ticks per second, last two minutes">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="${g.join(" ")}"/>
  </svg>`}function E(e){return e.innerHTML=`
  <header class="topbar">
    <span class="brand">chrono<span class="brand-dot">\u25B8</span></span>
    <span class="status mono">
      <span id="dot-feed"></span> feed
      <span id="dot-nats"></span> nats
      <span class="sep"></span> up <span id="uptime">\u2014</span>
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
    <section class="card area-fires red"><h2>fires</h2>
      <div class="big mono" id="fired">\u2014</div>
      <div class="sub">triggers fired</div>
      <div class="duo"><span class="mono" id="published">\u2014</span> published
        \xB7 <span class="mono" id="pubdropped">\u2014</span> dropped</div>
    </section>
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile"><div class="big mono" id="st-active">\u2014</div><div class="sub">active</div></div>
        <div class="tile"><div class="big mono" id="st-triggered">\u2014</div><div class="sub">triggered</div></div>
        <div class="tile"><div class="big mono" id="st-cancelled">\u2014</div><div class="sub">cancelled</div></div>
      </div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div class="rows" id="engine"></div></section>
    <section class="card area-stream teal"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger\u2026</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`,{inquiry:document.getElementById("inquiry")}}function S(){let e=s.snapshot,r=(o,p)=>{let n=document.getElementById(o);n&&(n.textContent=p)},t=(o,p)=>{let n=document.getElementById(o);n&&(n.innerHTML=w(p))};if(t("dot-feed",e?e.feed_ever_connected&&e.feed_last_seen_ms_ago<1e4:!1),t("dot-nats",!!e?.nats_connected),!e)return;r("uptime",_(e.uptime_sec)),r("tickrate",d(e.ticks_per_sec)),r("ticks",d(e.ticks)),r("ticksdropped",d(e.ticks_dropped)),r("fired",d(e.triggers_fired)),r("published",d(e.triggers_published)),r("pubdropped",d(e.triggers_publish_dropped)),r("st-active",String(e.alerts_by_state.active??0)),r("st-triggered",String(e.alerts_by_state.triggered??0)),r("st-cancelled",String(e.alerts_by_state.cancelled??0));let l=document.getElementById("spark");l&&(l.innerHTML=$(f));let g=document.getElementById("venues");if(g){let o=Object.entries(e.venue_ticks).sort((n,u)=>u[1]-n[1]),p=o[0]?.[1]||1;g.innerHTML=o.map(([n,u])=>`<div class="row"><span>${n}</span>
        <span class="bar"><i style="width:${100*u/p}%"></i></span>
        <span class="mono">${d(u)}</span></div>`).join("")||'<div class="empty">no ticks yet</div>'}let m=document.getElementById("engine");m&&(m.innerHTML=`<div class="row"><span>live alerts</span><span class="mono">${d(e.engine.live)}</span></div>
      <div class="row"><span>ring drops</span><span class="mono">${d(e.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${s.hello?d(s.hello.symbol_count):"\u2014"}</span></div>`);let c=document.getElementById("stream");if(c){let o=s.triggers[0],p=o?`${s.triggers.length}:${o.alert_id}:${o.fired_at_unix_nanos}`:"";p!==c.dataset.key&&(c.dataset.key=p,c.innerHTML=s.triggers.length===0?'<div class="empty">waiting for the first trigger\u2026</div>':s.triggers.slice(0,12).map(n=>`<div class="trg">
                <span class="badge ${n.direction==="ABOVE"?"up":"down"}">${n.direction}</span>
                <span class="sym">${n.symbol}</span>
                <span class="mono">${n.fired_price}</span>
                <span class="meta">${n.venue}/${n.tier}</span>
                <span class="mono time">${new Date(n.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span>
              </div>`).join(""))}}async function T(e){let r=new URLSearchParams(Object.entries(e).filter(([,l])=>l!=="")),t=await fetch(`/api/alerts?${r.toString()}`);if(!t.ok)throw new Error(`alerts: HTTP ${t.status}`);return await t.json()}var y=25,L=["active","triggered","cancelled"],v=null;function q(e){v?.(e)}function M(e){let r=0,t=null;e.innerHTML=`
    <h2>alert inquiry</h2>
    <form class="filters" id="f">
      <input name="symbol" placeholder="symbol (e.g. BTCUSDT)">
      <select name="state"><option value="">state: any</option>
        ${L.map(a=>`<option>${a}</option>`).join("")}</select>
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
      <button id="next">next \u2192</button></div>`;let l=e.querySelector("form"),g=e.querySelector("#rows"),m=e.querySelector("#pg"),c=e.querySelector("#prev"),o=e.querySelector("#next"),p=e.querySelector('select[name="venue"]'),n=e.querySelector('select[name="tier"]'),u=(a,i)=>{let x=a.value;a.innerHTML=`<option value="">${a.name}: any</option>`+i.map(H=>`<option>${H}</option>`).join(""),a.value=x};v=a=>{u(p,a.venues),u(n,a.tiers),v=null},s.hello&&v(s.hello);let h=()=>{c.disabled=r===0,o.disabled=!(t&&r+t.items.length<t.total)};async function b(){g.innerHTML='<tr><td colspan="8" class="empty">querying\u2026</td></tr>';let a={limit:String(y),offset:String(r)};new FormData(l).forEach((i,x)=>a[x]=String(i));try{t=await T(a)}catch(i){t=null,g.innerHTML=`<tr><td colspan="8" class="empty err">query failed \u2014 ${i instanceof Error?i.message:String(i)}</td></tr>`,m.textContent="",h();return}g.innerHTML=t.items.length===0?'<tr><td colspan="8" class="empty">no alerts match</td></tr>':t.items.map(i=>`<tr>
              <td>${i.symbol}</td><td class="meta">${i.venue}/${i.tier}</td>
              <td class="meta">${i.price_type}</td>
              <td><span class="badge ${i.direction==="ABOVE"?"up":"down"}">${i.direction}</span></td>
              <td class="mono">${i.target_price}</td>
              <td><span class="chip state-${i.state}">${i.state}</span></td>
              <td class="mono time">${new Date(i.created_at_unix_nanos/1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${i.id.slice(0,8)}</td>
            </tr>`).join(""),m.textContent=t.items.length===0?`0 of ${t.total}`:`${r+1}\u2013${r+t.items.length} of ${t.total}`,h()}l.addEventListener("submit",a=>{a.preventDefault(),r=0,b()}),c.addEventListener("click",()=>{r>0&&(r=Math.max(0,r-y),b())}),o.addEventListener("click",()=>{t&&r+t.items.length<t.total&&(r+=y,b())}),h(),b()}function B(){M(E(document.getElementById("app")).inquiry);let e=new EventSource("/api/stream");e.onmessage=r=>{let t=JSON.parse(r.data);switch(t.type){case"hello":s.hello=t,q(s.hello);break;case"snapshot":s.snapshot=t.snapshot,f.push(s.snapshot.ticks_per_sec),f.length>120&&f.shift();break;case"trigger":k(t.trigger);break}S()}}B();})();
