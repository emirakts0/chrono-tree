"use strict";(()=>{var s={hello:null,snapshot:null,triggers:[],series:{ticks:[],fires:[],live:[],cpu:[],rss:[]}},Y=120;function k(e,t){e.push(t),e.length>Y&&e.shift()}function z(e){s.series.ticks=[...e.t],s.series.fires=[...e.f],s.series.live=[...e.l],s.series.cpu=[...e.c],s.series.rss=[...e.m]}function q(e){k(s.series.ticks,e.ticks_per_sec),k(s.series.fires,e.triggers_per_sec),k(s.series.live,e.engine.live),e.sys&&(k(s.series.cpu,e.sys.host_cpu_percent),k(s.series.rss,e.sys.rss_bytes))}function S(e){for(let t of e)s.triggers.unshift(t);s.triggers.length>10&&(s.triggers.length=10)}var v=e=>e>=1e9?`${(e/1e9).toFixed(1)}B`:e>=1e6?`${(e/1e6).toFixed(1)}M`:e>=1e4?`${(e/1e3).toFixed(1)}k`:e.toLocaleString(),E=e=>e>=1099511627776?`${(e/1099511627776).toFixed(1)} TB`:e>=1073741824?`${(e/1073741824).toFixed(1)} GB`:e>=1048576?`${(e/1048576).toFixed(1)} MB`:`${(e/1024).toFixed(0)} KB`;function B(e){let t=Math.floor(e/3600),i=Math.floor(e%3600/60),r=Math.floor(e%60);return t>0?`${t}h ${i}m`:i>0?`${i}m ${r}s`:`${r}s`}function D(e){return`<span class="dot ${e?"on":"off"}"></span>`}function A(e){if(e.length<2)return"";let t=100,i=28,r=Math.max(...e,1),o=e.map((a,m)=>`${m/(e.length-1)*t},${i-a/r*i}`);return`<svg viewBox="0 0 ${t} ${i}" preserveAspectRatio="none" role="img" aria-label="ticks per second, last two minutes">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="${o.join(" ")}"/>
  </svg>`}function w(e,t=100,i=16){if(e.length<2)return"";let r=Math.max(...e,1),o=Math.min(...e,0),a=r-o||1,m=e.map((b,f)=>{let l=f/(e.length-1)*t,p=i-(b-o)/a*i;return`${l.toFixed(1)},${p.toFixed(1)}`});return`<svg viewBox="0 0 ${t} ${i}" preserveAspectRatio="none" class="trend">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points="${m.join(" ")}"/>
  </svg>`}var M={active:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="3.5" fill="none" stroke="currentColor" stroke-width="2"/><circle class="pulse" cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" stroke-width="1"/></svg>',triggered:'<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M9.5 1 3 9.2h3.6L6 15l6.3-8.2H8.7L9.5 1z" fill="currentColor"/></svg>',cancelled:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5"/><line x1="3.4" y1="12.6" x2="12.6" y2="3.4" stroke="currentColor" stroke-width="1.5"/></svg>'};function I(e){e.innerHTML=`
  <header class="topbar">
    <span class="brand">chrono-tree<span class="brand-dot">\u25B8</span></span>
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
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile t-active"><span class="glyph">${M.active}</span><div class="big mono" id="st-active">\u2014</div><div class="sub">active</div><div class="tilespark" id="livetrendline"></div></div>
        <div class="tile t-triggered"><span class="glyph">${M.triggered}</span><div class="big mono" id="st-triggered">\u2014</div><div class="sub">triggered</div><div class="tilespark" id="firesspark"></div></div>
        <div class="tile t-cancelled"><span class="glyph">${M.cancelled}</span><div class="big mono" id="st-cancelled">\u2014</div><div class="sub">cancelled</div></div>
      </div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div id="engine" class="rows"></div></section>
    <section class="card area-stream rail"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger\u2026</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`;let t=document.createElement("div");t.className="tip",document.body.appendChild(t);let i=document.getElementById("stream");return i.addEventListener("mouseover",r=>{let o=r.target.closest(".trg");if(!o?.dataset.tip)return;t.innerHTML=o.dataset.tip,t.style.opacity="1";let a=o.getBoundingClientRect();t.style.top=`${Math.round(a.top+a.height/2)}px`,t.style.transform="translateY(-50%)",a.right+330<window.innerWidth?(t.style.left=`${Math.round(a.right+10)}px`,t.style.transform="translateY(-50%)"):(t.style.left=`${Math.round(a.left-10)}px`,t.style.transform="translate(-100%, -50%)")}),i.addEventListener("mouseout",r=>{r.target.closest?.(".trg")&&(t.style.opacity="0")}),{inquiry:document.getElementById("inquiry")}}function C(){let e=s.snapshot,t=(l,p)=>{let n=document.getElementById(l);n&&(n.textContent=p)},i=(l,p)=>{let n=document.getElementById(l);n&&(n.innerHTML=D(p))};if(i("dot-feed",e?e.feed_ever_connected&&e.feed_last_seen_ms_ago<1e4:!1),i("dot-nats",!!e?.nats_connected),!e)return;t("published",v(e.triggers_published)),t("pubdropped",v(e.triggers_publish_dropped)),t("tickrate",v(e.ticks_per_sec)),t("ticks",v(e.ticks)),t("ticksdropped",v(e.ticks_dropped));let r=document.getElementById("spark");r&&(r.innerHTML=A(s.series.ticks)),t("st-active",String(e.alerts_by_state.active??0)),t("st-triggered",String(e.alerts_by_state.triggered??0)),t("st-cancelled",String(e.alerts_by_state.cancelled??0));let o=document.getElementById("firesspark");o&&(o.innerHTML=w(s.series.fires));let a=document.getElementById("livetrendline");a&&(a.innerHTML=w(s.series.live));let m=document.getElementById("venues");if(m){let l=Object.entries(e.venue_ticks).sort((p,n)=>n[1]-p[1]);m.innerHTML=l.map(([p,n])=>`<div class="row">
          <span class="vname">${p}</span>
          <span class="mono">${v(n)}</span>
        </div>`).join("")||'<div class="empty">no ticks yet</div>'}let b=document.getElementById("engine");if(b){let l=e.sys,p=n=>`${Math.round(n)}%`;b.innerHTML=`
      <div class="row"><span>ring drops</span><span class="mono">${v(e.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${s.hello?v(s.hello.symbol_count):"\u2014"}</span></div>
      <div class="row"><span>uptime</span><span class="mono">${B(e.uptime_sec)}</span></div>
      <div class="hr"></div>
      <div class="row"><span>cpu<span class="meta"> \xB7 ${p(l?.proc_cpu_percent??0)} proc</span></span><span class="mono">${p(l?.host_cpu_percent??0)}<span class="vmeta">${w(s.series.cpu.slice(-40))}</span></span></div>
      <div class="row"><span>cores</span><span class="mono">${l?.cpu_cores??"\u2014"}</span></div>
      <div class="row"><span>ram<span class="meta"> \xB7 engine heap ${E(l?.heap_bytes??0)}</span></span><span class="mono">${E(l?.rss_bytes??0)}<span class="vmeta">${w(s.series.rss.slice(-40))}</span></span></div>
      <div class="row"><span>alert store</span><span class="mono">${(l?.db_bytes??0)>0?E(l.db_bytes):"\u2014"}</span></div>`}let f=document.getElementById("stream");if(f){let l=s.triggers[0],p=l?`${s.triggers.length}:${l.alert_id}:${l.fired_at_unix_nanos}`:"";p!==f.dataset.key&&(f.dataset.key=p,f.innerHTML=s.triggers.length===0?'<div class="empty">waiting for the first trigger\u2026</div>':s.triggers.map(n=>`<div class="trg" data-tip="<b>${n.symbol} \xB7 ${n.direction}</b><div class=tip-row><span class=mono>${n.fired_price}</span> (target <span class=mono>${n.target_price}</span>)</div><div class=tip-row>${n.venue}/${n.tier} \xB7 <span class=mono>${new Date(n.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span></div>">
                <span class="badge ${n.direction==="ABOVE"?"up":"down"}">${n.direction}</span>
                <span class="sym">${n.symbol}</span>
                <span class="mono">${n.fired_price}</span>
                <span class="meta">${n.venue}/${n.tier}</span>
                <span class="mono time">${new Date(n.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span>
              </div>`).join(""))}}async function O(e){let t=new URLSearchParams(Object.entries(e).filter(([,r])=>r!=="")),i=await fetch(`/api/alerts?${t.toString()}`);if(!i.ok)throw new Error(`alerts: HTTP ${i.status}`);return await i.json()}function y(e,t,i){let r=document.createElement("input");r.type="hidden",r.name=t;let o=document.createElement("button");o.type="button",o.className="dd",o.setAttribute("aria-haspopup","listbox"),o.setAttribute("aria-expanded","false");let a=document.createElement("ul");a.className="dd-list",a.role="listbox",a.hidden=!0;let m="",b=()=>{o.innerHTML=`<span>${i.find(n=>n.value===m)?.label??""}</span><i class="chev">\u25BE</i>`;for(let n of a.children)n.classList.toggle("sel",n.dataset.v===m)};i.forEach(n=>{let u=document.createElement("li");u.role="option",u.dataset.v=n.value,u.tabIndex=-1,u.textContent=n.label,u.addEventListener("click",()=>{m=n.value,r.value=m,b(),l()}),a.appendChild(u)});let f=()=>{a.hidden=!1,o.setAttribute("aria-expanded","true"),a.querySelector(`[data-v="${m}"]`)?.focus()},l=()=>{a.hidden=!0,o.setAttribute("aria-expanded","false")},p=()=>a.hidden?f():l();return o.addEventListener("click",p),a.addEventListener("keydown",n=>{let u=[...a.children],g=u.indexOf(document.activeElement);n.key==="Escape"?(l(),o.focus()):n.key==="ArrowDown"&&g<u.length-1?u[g+1].focus():n.key==="ArrowUp"&&g>0?u[g-1].focus():n.key==="Enter"&&g>=0&&(u[g].click(),n.preventDefault())}),document.addEventListener("click",n=>{e.contains(n.target)||l()}),e.append(r,o,a),b(),r}function T(e,t){let i=e.querySelector("input"),r=i.value;e.innerHTML="",y(e,i.name,t);let o=e.querySelector("input");o.value=r}var F=[25,50,100,250,500],U=["active","triggered","cancelled"],$=null;function j(e){$?.(e)}function R(e){let t=0,i=F[0],r=null;e.innerHTML=`
    <h2>alert inquiry</h2>
    <form class="filters" id="f">
      <input name="symbol" placeholder="symbol (e.g. BTCUSDT)">
      <span class="ddhost" data-dd="state"></span>
      <span class="ddhost" data-dd="venue"></span>
      <span class="ddhost" data-dd="tier"></span>
      <span class="ddhost" data-dd="direction"></span>
      <button type="submit"><svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.5" fill="none" stroke="currentColor" stroke-width="1.8"/><line x1="10.6" y1="10.6" x2="14" y2="14" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>query</button>
    </form>
    <div class="tblwrap"><table class="tbl"><thead><tr>
      <th>symbol</th><th>dims</th><th>type</th><th>dir</th><th>target</th>
      <th>state</th><th>created</th><th></th>
    </tr></thead><tbody id="rows"></tbody></table></div>
    <div class="pager">
      <label class="pgsize">rows/page
        <select id="pgsize">${F.map(c=>`<option value="${c}"${c===i?" selected":""}>${c}</option>`).join("")}</select>
      </label>
      <button id="first" title="first page">\xAB</button>
      <button id="prev">\u2190 prev</button>
      <span id="pg" class="mono"></span>
      <button id="next">next \u2192</button>
      <button id="last" title="last page">\xBB</button>
    </div>`;let o=e.querySelector("form"),a=e.querySelector("#rows"),m=e.querySelector(".tblwrap"),b=e.querySelector("#pg"),f=e.querySelector("#first"),l=e.querySelector("#prev"),p=e.querySelector("#next"),n=e.querySelector("#last"),u=e.querySelector("#pgsize"),g=c=>e.querySelector(`[data-dd="${c}"]`),h=(c,d)=>[{value:"",label:d},...c.map(_=>({value:_,label:_}))];y(g("state"),"state",h(U,"state: any")),y(g("venue"),"venue",h([],"venue: any")),y(g("tier"),"tier",h([],"tier: any")),y(g("direction"),"direction",h(["ABOVE","BELOW"],"direction: any")),$=c=>{T(g("venue"),h(c.venues,"venue: any")),T(g("tier"),h(c.tiers,"tier: any")),$=null},s.hello&&$(s.hello);let N=()=>r?Math.max(0,Math.floor((r.total-1)/i)*i):0,L=()=>{let c=t===0,d=!(r&&t+r.items.length<r.total);f.disabled=l.disabled=c,p.disabled=n.disabled=d},x=c=>{t=c,m.scrollTop=0,H()};async function H(){a.innerHTML='<tr><td colspan="8" class="empty">querying\u2026</td></tr>';let c={limit:String(i),offset:String(t)};new FormData(o).forEach((d,_)=>c[_]=String(d));try{r=await O(c)}catch(d){r=null,a.innerHTML=`<tr><td colspan="8" class="empty err">query failed \u2014 ${d instanceof Error?d.message:String(d)}</td></tr>`,b.textContent="",L();return}a.innerHTML=r.items.length===0?'<tr><td colspan="8" class="empty">no alerts match</td></tr>':r.items.map(d=>`<tr>
              <td>${d.symbol}</td><td class="meta">${d.venue}/${d.tier}</td>
              <td class="meta">${d.price_type}</td>
              <td><span class="badge ${d.direction==="ABOVE"?"up":"down"}">${d.direction}</span></td>
              <td class="mono">${d.target_price}</td>
              <td><span class="chip state-${d.state}">${d.state}</span></td>
              <td class="mono time">${new Date(d.created_at_unix_nanos/1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${d.id.slice(0,8)}</td>
            </tr>`).join(""),b.textContent=r.items.length===0?`0 of ${r.total}`:`${t+1}\u2013${t+r.items.length} of ${r.total}`,L()}o.addEventListener("submit",c=>{c.preventDefault(),x(0)}),f.addEventListener("click",()=>{t>0&&x(0)}),l.addEventListener("click",()=>{t>0&&x(Math.max(0,t-i))}),p.addEventListener("click",()=>{r&&t+r.items.length<r.total&&x(t+i)}),n.addEventListener("click",()=>{r&&t+r.items.length<r.total&&x(N())}),u.addEventListener("change",()=>{i=Number(u.value),x(0)}),L(),H()}function P(){if(matchMedia("(prefers-reduced-motion: reduce)").matches)return;let e=document.createElement("div");e.className="intro",e.innerHTML='<span class="intro-mark">chrono-tree<span class="brand-dot">\u25B8</span></span>',document.body.classList.add("introing"),document.body.appendChild(e),setTimeout(()=>{document.body.classList.remove("introing"),e.remove()},1500)}function V(){P(),R(I(document.getElementById("app")).inquiry);let e=new EventSource("/api/stream");e.onmessage=t=>{let i=JSON.parse(t.data);switch(i.type){case"hello":s.hello=i,z(s.hello.history),j(s.hello);break;case"snapshot":s.snapshot=i.snapshot,q(s.snapshot);break;case"triggers":S(i.triggers);break}C()}}V();})();
