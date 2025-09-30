function fmt(x){return x.toLocaleString('ru-RU',{minimumFractionDigits:2, maximumFractionDigits:2});}
function pct(a,b){if(!b) return 0; return (a/b*100-100);}
function cls(n){return n>=0?'good':'bad';}

function setPeriodCurrentMonth(){
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);
  const end = now;
  const f = (d)=> d.toISOString().slice(0,10);
  document.getElementById('start').value = f(start);
  document.getElementById('end').value = f(end);
}
function setPeriodPrevMonth(){
  const now = new Date();
  const first = new Date(now.getFullYear(), now.getMonth(), 1);
  const prevLast = new Date(first-1);
  const prevFirst = new Date(prevLast.getFullYear(), prevLast.getMonth(), 1);
  const f = (d)=> d.toISOString().slice(0,10);
  document.getElementById('start').value = f(prevFirst);
  document.getElementById('end').value = f(prevLast);
}
function setPeriodYTD(){
  const now = new Date();
  const start = new Date(now.getFullYear(), 0, 1);
  const end = now;
  const f = (d)=> d.toISOString().slice(0,10);
  document.getElementById('start').value = f(start);
  document.getElementById('end').value = f(end);
}
function setPeriodPrevYear(){
  const now = new Date();
  const start = new Date(now.getFullYear()-1, 0, 1);
  const end = new Date(now.getFullYear()-1, 11, 31);
  const f = (d)=> d.toISOString().slice(0,10);
  document.getElementById('start').value = f(start);
  document.getElementById('end').value = f(end);
}

async function loadChartsAndKPI(){
  const [rev, mar, inf] = await Promise.all([
    fetch('/api/revenue/daily?days=60',{credentials:'include'}).then(r=>r.json()),
    fetch('/api/margin/daily?days=60',{credentials:'include'}).then(r=>r.json()),
    fetch('/api/inflow/daily?days=60',{credentials:'include'}).then(r=>r.json()),
  ]);
  const revRows = rev.data||[], marRows = mar.data||[], infRows = inf.data||[];

  // KPI за 7 дней
  const from7 = (()=>{const d=new Date(); d.setDate(d.getDate()-6); return d.toISOString().slice(0,10);})();
  const kRows = marRows.filter(r=>r.date>=from7);
  const kRev = kRows.reduce((s,r)=>s+(r.revenue||0),0);
  const kGP  = kRows.reduce((s,r)=>s+(r.gross_profit||0),0);
  const kChecks = (revRows.filter(r=>r.date>=from7)).reduce((s,r)=>s+(r.receipts||0),0);
  const kMargin = kRev ? (kGP/kRev*100) : 0;
  const kAT = kChecks ? (kRev/kChecks) : 0;
  document.getElementById('kpi-rev').textContent = fmt(kRev) + ' ₽';
  document.getElementById('kpi-gp').textContent = fmt(kGP) + ' ₽';
  document.getElementById('kpi-margin').textContent = kMargin.toFixed(1) + ' %';
  document.getElementById('kpi-at').textContent = fmt(kAT) + ' ₽';

  // Revenue chart
  const byWh = {};
  for(const r of revRows){ (byWh[r.warehouse] ||= []).push(r); }
  const dates = [...new Set(revRows.map(r=>r.date))].sort();
  Plotly.newPlot('chart-revenue',
    Object.entries(byWh).map(([wh, arr])=>{
      const map = Object.fromEntries(arr.map(r=>[r.date, r.revenue]));
      return {x: dates, y: dates.map(d=> map[d] ?? 0), type:'scatter', mode:'lines+markers', name: wh};
    }),
    {paper_bgcolor:'#0b0c10', plot_bgcolor:'#0b0c10',
     xaxis:{gridcolor:'#222831', tickformat:'%Y-%m-%d'},
     yaxis:{gridcolor:'#222831', title:'₽'},
     margin:{t:10,r:10,b:40,l:60}}, {displayModeBar:false, responsive:true}
  );

  // Margin chart
  const byWhM = {};
  for(const r of marRows){ (byWhM[r.warehouse] ||= []).push(r); }
  const datesM = [...new Set(marRows.map(r=>r.date))].sort();
  const tracesM = [];
  for(const [wh, arr] of Object.entries(byWhM)){
    const mapGP = Object.fromEntries(arr.map(r=>[r.date, r.gross_profit]));
    const mapPct= Object.fromEntries(arr.map(r=>[r.date, r.margin_pct]));
    tracesM.push({x: datesM, y: datesM.map(d=> mapGP[d] ?? 0), type:'bar', name: wh + ' GP', opacity:0.75});
    tracesM.push({x: datesM, y: datesM.map(d=> mapPct[d] ?? 0), type:'scatter', mode:'lines', name: wh + ' %', yaxis:'y2'});
  }
  Plotly.newPlot('chart-margin', tracesM, {
    paper_bgcolor:'#0b0c10', plot_bgcolor:'#0b0c10',
    xaxis:{gridcolor:'#222831', tickformat:'%Y-%m-%d'},
    yaxis:{gridcolor:'#222831', title:'Валовая прибыль ₽'},
    yaxis2:{gridcolor:'#222831', title:'Маржа %', overlaying:'y', side:'right'},
    barmode:'group', margin:{t:10,r:60,b:40,l:60}
  }, {displayModeBar:false, responsive:true});

  // Inflow chart
  const byWhI = {};
  for(const r of infRows){ (byWhI[r.warehouse] ||= []).push(r); }
  const datesI = [...new Set(infRows.map(r=>r.date))].sort();
  Plotly.newPlot('chart-inflow',
    Object.entries(byWhI).map(([wh, arr])=>{
      const map = Object.fromEntries(arr.map(r=>[r.date, r.inflow]));
      return {x: datesI, y: datesI.map(d=> map[d] ?? 0), type:'bar', name: wh};
    }), {
      paper_bgcolor:'#0b0c10', plot_bgcolor:'#0b0c10',
      xaxis:{gridcolor:'#222831', tickformat:'%Y-%m-%d'},
      yaxis:{gridcolor:'#222831', title:'Оприходования ₽'},
      barmode:'group', margin:{t:10,r:10,b:40,l:60}
    }, {displayModeBar:false, responsive:true}
  );
}

async function loadWarehouses(){
  const res = await fetch('/api/warehouses',{credentials:'include'});
  const json = await res.json();
  const sel = document.getElementById('warehouse');
  for(const w of (json.data||[])){
    const opt = document.createElement('option');
    opt.value = w.id; opt.textContent = w.name;
    sel.appendChild(opt);
  }
}

async function loadComparison(){
  const start = document.getElementById('start').value;
  const end   = document.getElementById('end').value;
  const group = document.getElementById('group').value;
  const wh    = document.getElementById('warehouse').value;
  const url = new URL('/api/summary', location.origin);
  url.searchParams.set('start', start);
  url.searchParams.set('end', end);
  url.searchParams.set('group', group);
  if(wh) url.searchParams.set('warehouse_id', wh);
  const res = await fetch(url, {credentials:'include'});
  if(!res.ok){ document.getElementById('compare').textContent = 'Ошибка загрузки'; return; }
  const data = await res.json();
  const t = data.totals||{};
  const c = data.compare||{};
  const prev = c.previous||{}, yoy = c.previous_year||{};
  const kpi = (r)=>({
    rev: r.revenue||0,
    gp:  (r.revenue||0) - (r.cost||0),
    mar: (r.revenue ? ((r.revenue - (r.cost||0))/r.revenue*100) : 0),
    at:  (r.receipts ? (r.revenue/r.receipts) : 0),
    chk: (r.receipts||0),
  });
  const cur  = kpi(t);
  const p    = kpi(prev);
  const y    = kpi(yoy);

  document.getElementById('compare').textContent =
    `Период ${start} – ${end}${wh? ' • Склад ID '+wh : ''} • Группировка: ${group}`;

  const rows = [
    ['Выручка', cur.rev, p.rev, pct(cur.rev, p.rev), y.rev, pct(cur.rev, y.rev)],
    ['Валовая прибыль', cur.gp, p.gp, pct(cur.gp, p.gp), y.gp, pct(cur.gp, y.gp)],
    ['Маржа %', cur.mar, p.mar, cur.mar - p.mar, y.mar, cur.mar - y.mar],
    ['Средний чек', cur.at, p.at, pct(cur.at, p.at), y.at, pct(cur.at, y.at)],
    ['Количество чеков', cur.chk, p.chk, pct(cur.chk, p.chk), y.chk, pct(cur.chk, y.chk)],
  ];
  let html = '<table><thead><tr><th>Метрика</th><th>Текущий</th><th>Предыдущий</th><th>Δ к пред.</th><th>Год назад</th><th>Δ к прошл. году</th></tr></thead><tbody>';
  for(const r of rows){
    const [name, curv, pv, d1, yv, d2] = r;
    const d1c = cls(d1), d2c = cls(d2);
    const curFmt = name==='Маржа %' ? curv.toFixed(1)+' %' : (name==='Количество чеков'? (curv||0).toFixed(0) : fmt(curv) + (name==='Средний чек'?' ₽':' ₽'));
    const pFmt   = name==='Маржа %' ? (pv||0).toFixed(1)+' %' : (name==='Количество чеков'? (pv||0).toFixed(0) : fmt(pv||0) + (name==='Средний чек'?' ₽':' ₽'));
    const yFmt   = name==='Маржа %' ? (yv||0).toFixed(1)+' %' : (name==='Количество чеков'? (yv||0).toFixed(0) : fmt(yv||0) + (name==='Средний чек'?' ₽':' ₽'));
    const d1Fmt  = name==='Маржа %' ? (d1||0).toFixed(1)+' п.п.' : (name==='Количество чеков'? (isFinite(d1)? d1.toFixed(1)+' %':'—') : (isFinite(d1)? d1.toFixed(1)+' %':'—'));
    const d2Fmt  = name==='Маржа %' ? (d2||0).toFixed(1)+' п.п.' : (name==='Количество чеков'? (isFinite(d2)? d2.toFixed(1)+' %':'—') : (isFinite(d2)? d2.toFixed(1)+' %':'—'));
    html += `<tr><td>${name}</td><td>${curFmt}</td><td>${pFmt}</td><td class="${d1c}">${d1Fmt}</td><td>${yFmt}</td><td class="${d2c}">${d2Fmt}</td></tr>`;
  }
  html += '</tbody></table>';
  document.getElementById('compare-table').innerHTML = html;

  await Promise.all([loadTopWarehouses(start, end), loadTopProducts(start, end, wh)]);
  document.getElementById('tp-scope').textContent = wh ? ('склад ID '+wh) : 'по всем складам';
}

async function loadTopWarehouses(start, end){
  const res = await fetch(`/api/top/warehouses?start=${start}&end=${end}`, {credentials:'include'});
  const data = await res.json();
  const rows = data.data||[];
  let html = '<table><thead><tr><th>Склад</th><th>Выручка</th><th>GP</th><th>Маржа</th><th>Чеки</th><th>Ср. чек</th></tr></thead><tbody>';
  for(const r of rows){
    html += `<tr>
      <td>${r.warehouse}</td>
      <td>${fmt(r.revenue)} ₽</td>
      <td>${fmt(r.gross_profit)} ₽</td>
      <td>${(r.margin_pct||0).toFixed(1)} %</td>
      <td>${(r.checks||0).toFixed(0)}</td>
      <td>${fmt(r.avg_ticket||0)} ₽</td>
    </tr>`;
  }
  html += '</tbody></table>';
  document.getElementById('top-warehouses').innerHTML = html;
}

async function loadTopProducts(start, end, wh){
  const url = new URL('/api/top/products', location.origin);
  url.searchParams.set('start', start);
  url.searchParams.set('end', end);
  if(wh) url.searchParams.set('warehouse_id', wh);
  const res = await fetch(url, {credentials:'include'});
  if(!res.ok){
    document.getElementById('top-products').innerHTML = '<div class="muted">Ошибка загрузки</div>'; return;
  }
  const data = await res.json();
  const rows = data.data||[];
  let html = '<table><thead><tr><th>Товар</th><th>Выручка</th><th>GP</th><th>Маржа</th><th>Кол-во</th><th>Ср. цена</th></tr></thead><tbody>';
  for(const r of rows){
    html += `<tr>
      <td>${r.name}</td>
      <td>${fmt(r.revenue)} ₽</td>
      <td>${fmt(r.gross_profit)} ₽</td>
      <td>${(r.margin_pct||0).toFixed(1)} %</td>
      <td>${(r.qty||0).toFixed(0)}</td>
      <td>${fmt(r.avg_price||0)} ₽</td>
    </tr>`;
  }
  html += '</tbody></table>';
  document.getElementById('top-products').innerHTML = html;
}

function wireQuickButtons(){
  document.getElementById('btn-cur-month').addEventListener('click', ()=>{ setPeriodCurrentMonth(); loadComparison(); });
  document.getElementById('btn-prev-month').addEventListener('click', ()=>{ setPeriodPrevMonth(); loadComparison(); });
  document.getElementById('btn-ytd').addEventListener('click', ()=>{ setPeriodYTD(); loadComparison(); });
  document.getElementById('btn-prev-year').addEventListener('click', ()=>{ setPeriodPrevYear(); loadComparison(); });
}

async function boot(){
  setPeriodCurrentMonth();
  await loadWarehouses();
  await loadChartsAndKPI();
  wireQuickButtons();
  await loadComparison();
  document.getElementById('apply').addEventListener('click', loadComparison);
}
document.addEventListener('DOMContentLoaded', boot);
