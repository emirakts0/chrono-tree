"use strict";(()=>{var s={hello:null,snapshot:null,triggers:[],series:{ticks:[],fires:[],live:[],cpu:[],rss:[],venues:{}}},G=120;function k(e,t){e.push(t),e.length>G&&e.shift()}function S(e){for(let t in $)delete $[t];s.series.ticks=[...e.t],s.series.fires=[...e.f],s.series.live=[...e.l],s.series.cpu=[...e.c],s.series.rss=[...e.m],s.series.venues=Object.fromEntries(Object.entries(e.v).map(([t,n])=>[t,[...n]]))}var $={};function K(e,t){let n=s.series.venues[e]??=[],i=$[e]===void 0?0:t-$[e];$[e]=t,k(n,i)}function q(e){k(s.series.ticks,e.ticks_per_sec),k(s.series.fires,e.triggers_per_sec),k(s.series.live,e.engine.live),e.sys&&(k(s.series.cpu,e.sys.host_cpu_percent),k(s.series.rss,e.sys.rss_bytes));for(let[t,n]of Object.entries(e.venue_ticks))K(t,n)}function B(e){for(let t of e)s.triggers.unshift(t);s.triggers.length>10&&(s.triggers.length=10)}var v=e=>e>=1e9?`${(e/1e9).toFixed(1)}B`:e>=1e6?`${(e/1e6).toFixed(1)}M`:e>=1e4?`${(e/1e3).toFixed(1)}k`:e.toLocaleString(),h=e=>e>=1099511627776?`${(e/1099511627776).toFixed(1)} TB`:e>=1073741824?`${(e/1073741824).toFixed(1)} GB`:e>=1048576?`${(e/1048576).toFixed(1)} MB`:`${(e/1024).toFixed(0)} KB`;function D(e){let t=Math.floor(e/3600),n=Math.floor(e%3600/60),i=Math.floor(e%60);return t>0?`${t}h ${n}m`:n>0?`${n}m ${i}s`:`${i}s`}function A(e){return`<span class="dot ${e?"on":"off"}"></span>`}function I(e){if(e.length<2)return"";let t=100,n=28,i=Math.max(...e,1),l=e.map((o,m)=>`${m/(e.length-1)*t},${n-o/i*n}`);return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" role="img" aria-label="ticks per second, last two minutes">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="${l.join(" ")}"/>
  </svg>`}function w(e,t=100,n=16){if(e.length<2)return"";let i=Math.max(...e,1),l=Math.min(...e,0),o=i-l||1,m=e.map((b,f)=>{let a=f/(e.length-1)*t,d=n-(b-l)/o*n;return`${a.toFixed(1)},${d.toFixed(1)}`});return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" class="trend">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points="${m.join(" ")}"/>
  </svg>`}function O(e){if(e.length<2)return"\u2014";let t=e[e.length-1]-e[e.length-2];return t>0?"\u25B2":t<0?"\u25BC":"\u2014"}function C(e){if(e.length<2)return"flat";let t=e[e.length-1]-e[e.length-2];return t>0?"up":t<0?"down":"flat"}var M={active:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="3.5" fill="none" stroke="currentColor" stroke-width="2"/><circle class="pulse" cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" stroke-width="1"/></svg>',triggered:'<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M9.5 1 3 9.2h3.6L6 15l6.3-8.2H8.7L9.5 1z" fill="currentColor"/></svg>',cancelled:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5"/><line x1="3.4" y1="12.6" x2="12.6" y2="3.4" stroke="currentColor" stroke-width="1.5"/></svg>'};function j(e){e.innerHTML=`
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
    <section class="card area-engine"><h2>engine \xB7 host</h2><div id="engine" class="rows"></div></section>
    <section class="card area-stream rail"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger\u2026</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`;let t=document.createElement("div");t.className="tip",document.body.appendChild(t);let n=document.getElementById("stream");return n.addEventListener("mouseover",i=>{let l=i.target.closest(".trg");if(!l?.dataset.tip)return;t.innerHTML=l.dataset.tip,t.style.opacity="1";let o=l.getBoundingClientRect();t.style.top=`${Math.round(o.top+o.height/2)}px`,t.style.transform="translateY(-50%)",o.right+330<window.innerWidth?(t.style.left=`${Math.round(o.right+10)}px`,t.style.transform="translateY(-50%)"):(t.style.left=`${Math.round(o.left-10)}px`,t.style.transform="translate(-100%, -50%)")}),n.addEventListener("mouseout",i=>{i.target.closest?.(".trg")&&(t.style.opacity="0")}),{inquiry:document.getElementById("inquiry")}}function F(){let e=s.snapshot,t=(a,d)=>{let r=document.getElementById(a);r&&(r.textContent=d)},n=(a,d)=>{let r=document.getElementById(a);r&&(r.innerHTML=A(d))};if(n("dot-feed",e?e.feed_ever_connected&&e.feed_last_seen_ms_ago<1e4:!1),n("dot-nats",!!e?.nats_connected),!e)return;t("published",v(e.triggers_published)),t("pubdropped",v(e.triggers_publish_dropped)),t("tickrate",v(e.ticks_per_sec)),t("ticks",v(e.ticks)),t("ticksdropped",v(e.ticks_dropped));let i=document.getElementById("spark");i&&(i.innerHTML=I(s.series.ticks)),t("st-active",String(e.alerts_by_state.active??0)),t("st-triggered",String(e.alerts_by_state.triggered??0)),t("st-cancelled",String(e.alerts_by_state.cancelled??0));let l=document.getElementById("firesspark");l&&(l.innerHTML=w(s.series.fires));let o=document.getElementById("livetrendline");o&&(o.innerHTML=w(s.series.live));let m=document.getElementById("venues");if(m){let a=Object.entries(e.venue_ticks).sort((d,r)=>r[1]-d[1]);m.innerHTML=a.map(([d,r])=>{let c=s.series.venues[d]??[];return`<div class="row">
          <span class="vname">${d} <span class="trendmark ${C(c)}">${O(c)}</span></span>
          <span class="vmeta">${w(c.slice(-40))}</span>
          <span class="mono">${v(r)}</span>
        </div>`}).join("")||'<div class="empty">no ticks yet</div>'}let b=document.getElementById("engine");if(b){let a=e.sys,d=r=>`${Math.round(r)}%`;b.innerHTML=`
      <div class="row"><span>ring drops</span><span class="mono">${v(e.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${s.hello?v(s.hello.symbol_count):"\u2014"}</span></div>
      <div class="row"><span>uptime</span><span class="mono">${D(e.uptime_sec)}</span></div>
      <div class="hr"></div>
      <div class="row"><span>cpu<span class="meta"> \xB7 ${d(a?.proc_cpu_percent??0)} proc</span></span><span class="mono">${d(a?.host_cpu_percent??0)}<span class="vmeta">${w(s.series.cpu.slice(-40))}</span></span></div>
      <div class="row"><span>cores</span><span class="mono">${a?.cpu_cores??"\u2014"}</span></div>
      <div class="row"><span>rss<span class="meta"> \xB7 heap ${h(a?.heap_bytes??0)}</span></span><span class="mono">${h(a?.rss_bytes??0)}<span class="vmeta">${w(s.series.rss.slice(-40))}</span></span></div>
      <div class="row"><span>ram</span><span class="mono">${h(a?.host_mem_used_bytes??0)} / ${h(a?.host_mem_total_bytes??0)}</span></div>
      <div class="row"><span>alert store</span><span class="mono">${(a?.db_bytes??0)>0?h(a.db_bytes):"\u2014"}</span></div>
      <div class="row"><span>disk free</span><span class="mono">${h(a?.disk_free_bytes??0)}</span></div>`}let f=document.getElementById("stream");if(f){let a=s.triggers[0],d=a?`${s.triggers.length}:${a.alert_id}:${a.fired_at_unix_nanos}`:"";d!==f.dataset.key&&(f.dataset.key=d,f.innerHTML=s.triggers.length===0?'<div class="empty">waiting for the first trigger\u2026</div>':s.triggers.map(r=>`<div class="trg" data-tip="<b>${r.symbol} \xB7 ${r.direction}</b><div class=tip-row><span class=mono>${r.fired_price}</span> (target <span class=mono>${r.target_price}</span>)</div><div class=tip-row>${r.venue}/${r.tier} \xB7 <span class=mono>${new Date(r.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span></div>">
                <span class="badge ${r.direction==="ABOVE"?"up":"down"}">${r.direction}</span>
                <span class="sym">${r.symbol}</span>
                <span class="mono">${r.fired_price}</span>
                <span class="meta">${r.venue}/${r.tier}</span>
                <span class="mono time">${new Date(r.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span>
              </div>`).join(""))}}async function R(e){let t=new URLSearchParams(Object.entries(e).filter(([,i])=>i!=="")),n=await fetch(`/api/alerts?${t.toString()}`);if(!n.ok)throw new Error(`alerts: HTTP ${n.status}`);return await n.json()}function _(e,t,n){let i=document.createElement("input");i.type="hidden",i.name=t;let l=document.createElement("button");l.type="button",l.className="dd",l.setAttribute("aria-haspopup","listbox"),l.setAttribute("aria-expanded","false");let o=document.createElement("ul");o.className="dd-list",o.role="listbox",o.hidden=!0;let m="",b=()=>{l.innerHTML=`<span>${n.find(r=>r.value===m)?.label??""}</span><i class="chev">\u25BE</i>`;for(let r of o.children)r.classList.toggle("sel",r.dataset.v===m)};n.forEach(r=>{let c=document.createElement("li");c.role="option",c.dataset.v=r.value,c.tabIndex=-1,c.textContent=r.label,c.addEventListener("click",()=>{m=r.value,i.value=m,b(),a()}),o.appendChild(c)});let f=()=>{o.hidden=!1,l.setAttribute("aria-expanded","true"),o.querySelector(`[data-v="${m}"]`)?.focus()},a=()=>{o.hidden=!0,l.setAttribute("aria-expanded","false")},d=()=>o.hidden?f():a();return l.addEventListener("click",d),o.addEventListener("keydown",r=>{let c=[...o.children],g=c.indexOf(document.activeElement);r.key==="Escape"?(a(),l.focus()):r.key==="ArrowDown"&&g<c.length-1?c[g+1].focus():r.key==="ArrowUp"&&g>0?c[g-1].focus():r.key==="Enter"&&g>=0&&(c[g].click(),r.preventDefault())}),document.addEventListener("click",r=>{e.contains(r.target)||a()}),e.append(i,l,o),b(),i}function H(e,t){let n=e.querySelector("input"),i=n.value;e.innerHTML="",_(e,n.name,t);let l=e.querySelector("input");l.value=i}var P=[25,50,100,250,500],W=["active","triggered","cancelled"],L=null;function N(e){L?.(e)}function V(e){let t=0,n=P[0],i=null;e.innerHTML=`
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
        <select id="pgsize">${P.map(u=>`<option value="${u}"${u===n?" selected":""}>${u}</option>`).join("")}</select>
      </label>
      <button id="first" title="first page">\xAB</button>
      <button id="prev">\u2190 prev</button>
      <span id="pg" class="mono"></span>
      <button id="next">next \u2192</button>
      <button id="last" title="last page">\xBB</button>
    </div>`;let l=e.querySelector("form"),o=e.querySelector("#rows"),m=e.querySelector(".tblwrap"),b=e.querySelector("#pg"),f=e.querySelector("#first"),a=e.querySelector("#prev"),d=e.querySelector("#next"),r=e.querySelector("#last"),c=e.querySelector("#pgsize"),g=u=>e.querySelector(`[data-dd="${u}"]`),x=(u,p)=>[{value:"",label:p},...u.map(E=>({value:E,label:E}))];_(g("state"),"state",x(W,"state: any")),_(g("venue"),"venue",x([],"venue: any")),_(g("tier"),"tier",x([],"tier: any")),_(g("direction"),"direction",x(["ABOVE","BELOW"],"direction: any")),L=u=>{H(g("venue"),x(u.venues,"venue: any")),H(g("tier"),x(u.tiers,"tier: any")),L=null},s.hello&&L(s.hello);let U=()=>i?Math.max(0,Math.floor((i.total-1)/n)*n):0,T=()=>{let u=t===0,p=!(i&&t+i.items.length<i.total);f.disabled=a.disabled=u,d.disabled=r.disabled=p},y=u=>{t=u,m.scrollTop=0,z()};async function z(){o.innerHTML='<tr><td colspan="8" class="empty">querying\u2026</td></tr>';let u={limit:String(n),offset:String(t)};new FormData(l).forEach((p,E)=>u[E]=String(p));try{i=await R(u)}catch(p){i=null,o.innerHTML=`<tr><td colspan="8" class="empty err">query failed \u2014 ${p instanceof Error?p.message:String(p)}</td></tr>`,b.textContent="",T();return}o.innerHTML=i.items.length===0?'<tr><td colspan="8" class="empty">no alerts match</td></tr>':i.items.map(p=>`<tr>
              <td>${p.symbol}</td><td class="meta">${p.venue}/${p.tier}</td>
              <td class="meta">${p.price_type}</td>
              <td><span class="badge ${p.direction==="ABOVE"?"up":"down"}">${p.direction}</span></td>
              <td class="mono">${p.target_price}</td>
              <td><span class="chip state-${p.state}">${p.state}</span></td>
              <td class="mono time">${new Date(p.created_at_unix_nanos/1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${p.id.slice(0,8)}</td>
            </tr>`).join(""),b.textContent=i.items.length===0?`0 of ${i.total}`:`${t+1}\u2013${t+i.items.length} of ${i.total}`,T()}l.addEventListener("submit",u=>{u.preventDefault(),y(0)}),f.addEventListener("click",()=>{t>0&&y(0)}),a.addEventListener("click",()=>{t>0&&y(Math.max(0,t-n))}),d.addEventListener("click",()=>{i&&t+i.items.length<i.total&&y(t+n)}),r.addEventListener("click",()=>{i&&t+i.items.length<i.total&&y(U())}),c.addEventListener("change",()=>{n=Number(c.value),y(0)}),T(),z()}function Y(){if(matchMedia("(prefers-reduced-motion: reduce)").matches)return;let e=document.createElement("div");e.className="intro",e.innerHTML='<span class="intro-mark">chrono-tree<span class="brand-dot">\u25B8</span></span>',document.body.classList.add("introing"),document.body.appendChild(e),setTimeout(()=>{document.body.classList.remove("introing"),e.remove()},1500)}function J(){Y(),V(j(document.getElementById("app")).inquiry);let e=new EventSource("/api/stream");e.onmessage=t=>{let n=JSON.parse(t.data);switch(n.type){case"hello":s.hello=n,S(s.hello.history),N(s.hello);break;case"snapshot":s.snapshot=n.snapshot,q(s.snapshot);break;case"triggers":B(n.triggers);break}F()}}J();})();
