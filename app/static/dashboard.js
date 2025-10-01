// Worker Analytics Dashboard — v9 (writeoff + inflow in metrics, multi-warehouse)

function fmt(x){return (x??0).toLocaleString('ru-RU',{minimumFractionDigits:2, maximumFractionDigits:2});}
function pctDelta(a,b){ if(!isFinite(a)||!isFinite(b)||b===0) return NaN; return (a/b*100-100); }
function cls(n){ return isFinite(n) ? (n>=0?'good':'bad') : ''; }
function toISO(d){ const z=new Date(d); z.setHours(0,0,0,0); return `${z.getFullYear()}-${String(z.getMonth()+1).padStart(2,'0')}-${String(z.getDate()).padStart(2,'0')}`; }
const parseISO=(s)=>{ const [y,m,dd]=s.split('-').map(Number); const d=new Date(y, m-1, dd); d.setHours(0,0,0,0); return d; };
function lastDayOfMonth(y,m){ return new Date(y, m+1, 0); }
function debounced(fn,ms=250){ let t; return (...a)=>{ clearTimeout(t); t=setTimeout(()=>fn(...a),ms); }; }
async function jget(url){ const r=await fetch(url,{credentials:'include'}); if(!r.ok) throw new Error(url+' -> '+r.status); return r.json(); }
function by(arr,key){ const m={}; for(const r of arr){ const k=r[key]; (m[k] ||= []).push(r); } return m; }

function setPeriodCurrentMonth(){
  const now=new Date();
  const start=new Date(now.getFullYear(), now.getMonth(), 1);
  const end=lastDayOfMonth(now.getFullYear(), now.getMonth());
  start.setHours(0,0,0,0); end.setHours(0,0,0,0);
  document.getElementById('start').value=toISO(start);
  document.getElementById('end').value=toISO(end);
}
function setPeriodPrevMonth(){
  const now=new Date();
  const prevFirst=new Date(now.getFullYear(), now.getMonth()-1, 1);
  const prevLast=lastDayOfMonth(prevFirst.getFullYear(), prevFirst.getMonth());
  prevFirst.setHours(0,0,0,0); prevLast.setHours(0,0,0,0);
  document.getElementById('start').value=toISO(prevFirst);
  document.getElementById('end').value=toISO(prevLast);
}
function setPeriodYTD(){ // до вчера
  const now=new Date();
  const start=new Date(now.getFullYear(),0,1);
  const end=new Date(now); end.setDate(end.getDate()-1);
  start.setHours(0,0,0,0); end.setHours(0,0,0,0);
  document.getElementById('start').value=toISO(start);
  document.getElementById('end').value=toISO(end);
}
function setPeriodPrevYear(){
  const y=(new Date()).getFullYear()-1;
  const start=new Date(y,0,1), end=new Date(y,11,31);
  start.setHours(0,0,0,0); end.setHours(0,0,0,0);
  document.getElementById('start').value=toISO(start);
  document.getElementById('end').value=toISO(end);
}

function rangeDays(startISO,endISO){
  const s=parseISO(startISO), e=parseISO(endISO);
  const days=[]; let d=new Date(s);
  while(d<=e){ days.push(toISO(d)); d.setDate(d.getDate()+1); }
  return days;
}
function prevPeriodRange(startISO,endISO){
  const len=rangeDays(startISO,endISO).length;
  const s=parseISO(startISO);
  const prevEnd=new Date(s); prevEnd.setDate(prevEnd.getDate()-1);
  const prevStart=new Date(prevEnd); prevStart.setDate(prevStart.getDate()-(len-1));
  return {start:toISO(prevStart),end:toISO(prevEnd)};
}
function prevYearRange(startISO,endISO){
  const s=parseISO(startISO), e=parseISO(endISO);
  const s2=new Date(s.getFullYear()-1,s.getMonth(),s.getDate());
  const e2=new Date(e.getFullYear()-1,e.getMonth(),e.getDate());
  s2.setHours(0,0,0,0); e2.setHours(0,0,0,0);
  return {start:toISO(s2),end:toISO(e2)};
}

window.__WAREHOUSES__=[];
function getSelectedWarehouseIds(){
  // Читаем все чекбоксы с name="wh[]"
  const boxes=document.querySelectorAll('input[name="wh[]"]:checked');
  const ids=[...boxes].map(b=>b.value);
  // если ни одного не выбрано — трактуем как «все»
  if(ids.length===0) return (window.__WAREHOUSES__||[]).map(w=>String(w.id));
  return ids;
}
function findWarehouseNameById(id){
  const w=(window.__WAREHOUSES__||[]).find(x=>String(x.id)===String(id));
  return w?.name;
}

async function loadWarehouses(){
  try{
    const res=await jget('/api/warehouses'); window.__WAREHOUSES__=res.data||[];
    // Рисуем панель мультивыбора, если её нет
    if(!document.getElementById('wh-multi')){
      const grid=document.querySelector('.grid'); if(!grid) return;
      const card=document.createElement('div'); card.className='card col-12'; card.id='wh-multi';
      let html='<div class="muted">Склады:</div><div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:8px">';
      for(const w of window.__WAREHOUSES__){
        html+=`<label style="user-select:none"><input type="checkbox" name="wh[]" value="${w.id}"> ${w.name}</label>`;
      }
      html+='</div>';
      card.innerHTML=html;
      grid.insertBefore(card, grid.firstChild);
      card.addEventListener('change', debounced(()=>loadComparison()));
    }
  }catch(e){ console.error('warehouses load failed', e); }
}

async function loadChartsAndKPI(){
  const [rev, mar, inf] = await Promise.all([
    jget('/api/revenue/daily?days=60'),
    jget('/api/margin/daily?days=60'),
    jget('/api/inflow/daily?days=60'),
  ]);
  const revRows = rev.data||[], marRows = mar.data||[], infRows = inf.data||[];

  const from7 = (()=>{const d=new Date(); d.setDate(d.getDate()-6); d.setHours(0,0,0,0); return toISO(d);})();
  const kRows = marRows.filter(r=>r.date>=from7);
  const kRev = kRows.reduce((s,r)=>s+(r.revenue||0),0);
  const kGP  = kRows.reduce((s,r)=>s+(r.gross_profit||0),0);
  const kChecks = (revRows.filter(r=>r.date>=from7)).reduce((s,r)=>s+(r.receipts||0),0);
  const kMargin = kRev ? (kGP/kRev*100) : 0;
  const kAT = kChecks ? (kRev/kChecks) : 0;
  const set=(id,txt)=>{ const el=document.getElementById(id); if(el) el.textContent=txt; };
  set('kpi-rev', fmt(kRev)+' ₽'); set('kpi-gp', fmt(kGP)+' ₽'); set('kpi-margin', kMargin.toFixed(1)+' %'); set('kpi-at', fmt(kAT)+' ₽');

  if(window.Plotly){
    const byWh = by(revRows,'warehouse'); const dates=[...new Set(revRows.map(r=>r.date))].sort();
    Plotly.newPlot('chart-revenue',
      Object.entries(byWh).map(([wh, arr])=>{
        const map=Object.fromEntries(arr.map(r=>[r.date,r.revenue]));
        return {x:dates,y:dates.map(d=>map[d]??0),type:'scatter',mode:'lines+markers',name:wh};
      }),
      {paper_bgcolor:'#0b0c10',plot_bgcolor:'#0b0c10',xaxis:{gridcolor:'#222831',tickformat:'%Y-%m-%d'},yaxis:{gridcolor:'#222831',title:'₽'},margin:{t:10,r:10,b:40,l:60}},
      {displayModeBar:false,responsive:true}
    );

    const byWhM = by(marRows,'warehouse'); const datesM=[...new Set(marRows.map(r=>r.date))].sort();
    const tracesM=[];
    for(const [wh, arr] of Object.entries(byWhM)){
      const gp=Object.fromEntries(arr.map(r=>[r.date,r.gross_profit]));
      const mp=Object.fromEntries(arr.map(r=>[r.date,r.margin_pct]));
      tracesM.push({x:datesM,y:datesM.map(d=>gp[d]??0),type:'bar',name:wh+' GP',opacity:0.75});
      tracesM.push({x:datesM,y:datesM.map(d=>mp[d]??0),type:'scatter',mode:'lines',name:wh+' %',yaxis:'y2'});
    }
    Plotly.newPlot('chart-margin', tracesM, {
      paper_bgcolor:'#0b0c10',plot_bgcolor:'#0b0c10',xaxis:{gridcolor:'#222831',tickformat:'%Y-%m-%d'},
      yaxis:{gridcolor:'#222831',title:'Валовая прибыль ₽'},
      yaxis2:{gridcolor:'#222831',title:'Маржа %',overlaying:'y',side:'right'},
      barmode:'group',margin:{t:10,r:60,b:40,l:60}
    }, {displayModeBar:false,responsive:true});

    const byWhI = by(infRows,'warehouse'); const datesI=[...new Set(infRows.map(r=>r.date))].sort();
    Plotly.newPlot('chart-inflow',
      Object.entries(byWhI).map(([wh, arr])=>{
        const map=Object.fromEntries(arr.map(r=>[r.date,r.inflow]));
        return {x:datesI,y:datesI.map(d=>map[d]??0),type:'bar',name:wh};
      }),
      {paper_bgcolor:'#0b0c10',plot_bgcolor:'#0b0c10',xaxis:{gridcolor:'#222831',tickformat:'%Y-%m-%d'},yaxis:{gridcolor:'#222831',title:'Оприходования ₽'},barmode:'group',margin:{t:10,r:10,b:40,l:60}},
      {displayModeBar:false,responsive:true}
    );
  }
}

// ---- SUMMARY helpers (multi-warehouse) ----
async function fetchSummary(start,end,group,whId){ // one warehouse
  const url=new URL('/api/summary',location.origin);
  url.searchParams.set('start',start); url.searchParams.set('end',end); url.searchParams.set('group',group||'day');
  if(whId) url.searchParams.set('warehouse_id',whId);
  return jget(url.toString());
}
function sumTotals(a,b){ // sums totals-like objects
  const out={}; for(const k of ['revenue','cost','discount','returns_cost','inflow_cost','receipts']){
    out[k]=(a?.[k]||0)+(b?.[k]||0);
  } return out;
}
function sumSeries(a,b){ // align by period and sum revenue
  const map=new Map();
  for(const r of (a||[])) map.set(r.period, {period:r.period, revenue:r.revenue||0});
  for(const r of (b||[])){
    const m = map.get(r.period) || {period:r.period, revenue:0};
    m.revenue += (r.revenue||0); map.set(r.period,m);
  }
  return [...map.values()].sort((x,y)=>x.period.localeCompare(y.period));
}
async function fetchSummaryTotalsMulti(start,end,group,whIds){
  if(!whIds || whIds.length===0) return fetchSummary(start,end,group,null);
  const arr=await Promise.all(whIds.map(id=>fetchSummary(start,end,group,id)));
  // reduce totals + compare
  const base = {totals:{}, compare:{previous:{}, previous_year:{}}};
  const out = arr.reduce((acc,cur)=>{
    acc.totals = sumTotals(acc.totals, cur.totals);
    acc.compare = {
      previous: sumTotals(acc.compare.previous, cur.compare?.previous),
      previous_year: sumTotals(acc.compare.previous_year, cur.compare?.previous_year),
    };
    // series суммировать для сравнительного графика
    acc.series = sumSeries(acc.series, cur.series);
    return acc;
  }, {...base, series:[]});
  return out;
}
async function fetchSummarySeriesMulti(start,end,group,whIds){
  const data=await fetchSummaryTotalsMulti(start,end,group,whIds);
  return (data.series||[]).map(r=>({period:r.period, revenue:r.revenue||0}));
}

// ---- writeoff sums (multi) from /api/writeoff/reasons ----
function isDefect(reason){
  if(!reason) return false;
  const s=String(reason).toLowerCase();
  return s.includes('брак');
}
function isInventory(reason){
  if(!reason) return false;
  const s=String(reason).toLowerCase();
  return s.includes('инвент') || s.includes('интвент') || s.includes('инвентар');
}
function sumWriteoffByBucket(rows, whIds, bucket){ // bucket: 'defect'|'inventory'
  const idsSet=new Set((whIds||[]).map(String));
  let sum=0;
  for(const r of rows){
    if(idsSet.size && !idsSet.has(String(r.warehouse_id))) continue;
    const reason=r.reason||'';
    if(bucket==='defect' && isDefect(reason)) sum+= (r.cost||0);
    else if(bucket==='inventory' && isInventory(reason)) sum+= (r.cost||0);
  }
  return sum;
}

async function loadCompareChart(){
  const start=document.getElementById('start')?.value;
  const end=document.getElementById('end')?.value;
  const group=document.getElementById('group')?.value || 'day';
  const whIds=getSelectedWarehouseIds();

  const nowSeries=await fetchSummarySeriesMulti(start,end,group,whIds);
  const xNow=nowSeries.map(r=>r.period); const yNow=nowSeries.map(r=>r.revenue);

  const traces=[{x:xNow,y:yNow,type:'scatter',mode:'lines+markers',name:'Текущий период'}];

  const doPrev=document.getElementById('cmp-prev-period')?.checked;
  const doYoY=document.getElementById('cmp-prev-year')?.checked;

  if(doPrev){
    const pp=prevPeriodRange(start,end);
    const prevSeries=await fetchSummarySeriesMulti(pp.start,pp.end,group,whIds);
    const yPrev=prevSeries.map(r=>r.revenue);
    traces.push({x:xNow,y:yPrev,type:'scatter',mode:'lines',name:'Пред. период',line:{dash:'dot'}});
  }
  if(doYoY){
    const yy=prevYearRange(start,end);
    const yoySeries=await fetchSummarySeriesMulti(yy.start,yy.end,group,whIds);
    const yYoy=yoySeries.map(r=>r.revenue);
    traces.push({x:xNow,y:yYoy,type:'scatter',mode:'lines',name:'Год назад',line:{dash:'dash'}});
  }

  if(window.Plotly){
    const yTitle = 'Выручка ₽' + (group==='day'?' (по дням)': group==='month'?' (по месяцам)':' (итоги по годам)');
    Plotly.newPlot('chart-compare', traces, {
      paper_bgcolor:'#0b0c10', plot_bgcolor:'#0b0c10',
      xaxis:{gridcolor:'#222831', title:(group==='day'?'Дни':'Периоды')},
      yaxis:{gridcolor:'#222831', title:yTitle},
      margin:{t:10,r:10,b:40,l:60}, legend:{orientation:'h'}
    }, {displayModeBar:false, responsive:true});
  }
}

async function loadTopWarehouses(start,end){
  try{
    const res=await jget(`/api/top/warehouses?start=${start}&end=${end}`);
    const rows=res.data||[];
    const el=document.getElementById('top-warehouses'); if(!el) return;
    let html='<table><thead><tr><th>Склад</th><th>Выручка</th><th>GP</th><th>Маржа</th><th>Чеки</th><th>Ср. чек</th></tr></thead><tbody>';
    for(const r of rows){ html+=`<tr><td>${r.warehouse}</td><td>${fmt(r.revenue)} ₽</td><td>${fmt(r.gross_profit)} ₽</td><td>${(r.margin_pct||0).toFixed(1)} %</td><td>${(r.checks||0).toFixed(0)}</td><td>${fmt(r.avg_ticket||0)} ₽</td></tr>`; }
    html+='</tbody></table>'; el.innerHTML=html;
  }catch(e){ console.warn('top warehouses failed', e); }
}
async function loadTopProducts(start,end,wh){
  try{
    // если выбран НЕ один склад — показываем по всем (API односекционный)
    const selected=getSelectedWarehouseIds();
    const whId = (selected.length===1) ? selected[0] : '';
    const url=new URL('/api/top/products',location.origin);
    url.searchParams.set('start',start); url.searchParams.set('end',end); if(whId) url.searchParams.set('warehouse_id',whId);
    const res=await jget(url.toString()); const rows=res.data||[];
    const el=document.getElementById('top-products'); if(!el) return;
    let html='<table><thead><tr><th>Товар</th><th>Выручка</th><th>GP</th><th>Маржа</th><th>Кол-во</th><th>Ср. цена</th></tr></thead><tbody>';
    for(const r of rows){ html+=`<tr><td>${r.name}</td><td>${fmt(r.revenue)} ₽</td><td>${fmt(r.gross_profit)} ₽</td><td>${(r.margin_pct||0).toFixed(1)} %</td><td>${(r.qty||0).toFixed(0)}</td><td>${fmt(r.avg_price||0)} ₽</td></tr>`; }
    html+='</tbody></table>'; el.innerHTML=html;
    const scope=document.getElementById('tp-scope'); if(scope) scope.textContent=(whId?('склад ID '+whId): (selected.length>1?'по выбранным складам':'по всем складам'));
  }catch(e){ console.warn('top products failed', e); }
}

// reasons table (current period)
function ensureWriteoffContainers(){
  const grid=document.querySelector('.grid'); if(!grid) return;
  if(!document.getElementById('chart-writeoff')){
    const card=document.createElement('div');
    card.className='card col-12';
    card.innerHTML='<div class="muted">Списания по дням (дефект/инвентаризация/прочее) — стэк + линия Total</div><div id="chart-writeoff" style="height:380px"></div>';
    grid.appendChild(card);
  }
  if(!document.getElementById('writeoff-reasons')){
    const card=document.createElement('div');
    card.className='card col-12';
    card.innerHTML='<div class="muted">Списания по причинам (сумма и % от выручки за период)</div><div id="writeoff-reasons"></div>';
    grid.appendChild(card);
  }
}

async function loadWriteoffBlock(){
  ensureWriteoffContainers();
  const start=document.getElementById('start')?.value;
  const end=document.getElementById('end')?.value;
  const selectedWhIds=getSelectedWarehouseIds();
  const selectedSet=new Set(selectedWhIds.map(String));

  // daily
  const dailyURL=new URL('/api/writeoff/daily',location.origin);
  dailyURL.searchParams.set('start',start); dailyURL.searchParams.set('end',end);
  const daily=(await jget(dailyURL.toString())).data||[];
  const dailyFiltered = daily.filter(r=> selectedSet.has(String(r.warehouse_id)));
  const days=[...new Set(dailyFiltered.map(r=>r.date))].sort();
  const mapByDate=Object.fromEntries(days.map(d=>[d,{defect:0,inventory:0,other:0,total:0}]));
  for(const r of dailyFiltered){
    const m=mapByDate[r.date];
    m.defect+=r.defect||0; m.inventory+=r.inventory||0; m.other+=r.other||0; m.total+=r.total||0;
  }
  if(window.Plotly){
    const x=days, yDef=x.map(d=>mapByDate[d].defect), yInv=x.map(d=>mapByDate[d].inventory), yOth=x.map(d=>mapByDate[d].other), yTot=x.map(d=>mapByDate[d].total);
    Plotly.newPlot('chart-writeoff',[
      {x,y:yDef,type:'bar',name:'Дефект'},
      {x,y:yInv,type:'bar',name:'Инвентаризация'},
      {x,y:yOth,type:'bar',name:'Прочее'},
      {x,y:yTot,type:'scatter',mode:'lines+markers',name:'Всего',yaxis:'y2'}
    ],{
      paper_bgcolor:'#0b0c10',plot_bgcolor:'#0b0c10',
      xaxis:{gridcolor:'#222831',tickformat:'%Y-%m-%d'},
      yaxis:{gridcolor:'#222831',title:'Списания ₽',rangemode:'tozero'},
      yaxis2:{gridcolor:'#222831',title:'Всего ₽',overlaying:'y',side:'right'},
      barmode:'stack',margin:{t:10,r:60,b:40,l:60}
    },{displayModeBar:false,responsive:true});
  }

  // reasons -> table
  const reasonURL=new URL('/api/writeoff/reasons',location.origin);
  reasonURL.searchParams.set('start',start); reasonURL.searchParams.set('end',end);
  const reasonRows=(await jget(reasonURL.toString())).data||[];
  const reasonFiltered=reasonRows.filter(r=> selectedSet.has(String(r.warehouse_id)));
  const totalInPeriod=reasonFiltered.reduce((s,r)=>s+(r.cost||0),0);

  let revenueTotal=0;
  try{
    const sum=await fetchSummaryTotalsMulti(start,end,'day',selectedWhIds);
    revenueTotal=sum?.totals?.revenue||0;
  }catch(_) {}

  const aggr={};
  for(const r of reasonFiltered){ const k=r.reason||'(без причины)'; aggr[k]=(aggr[k]||0)+(r.cost||0); }
  const rows=Object.entries(aggr).sort((a,b)=>b[1]-a[1]).map(([reason,cost])=>{
    const pctW = totalInPeriod? (cost/totalInPeriod*100):0;
    const pctR = revenueTotal? (cost/revenueTotal*100):0;
    return {reason, cost, pctW, pctR};
  });

  let html='<table><thead><tr><th>Причина</th><th>Сумма ₽</th><th>% в списаниях</th><th>% от выручки</th></tr></thead><tbody>';
  for(const r of rows){ html+=`<tr><td>${r.reason}</td><td>${fmt(r.cost)}</td><td>${r.pctW.toFixed(1)} %</td><td>${r.pctR.toFixed(2)} %</td></tr>`; }
  if(rows.length===0) html+='<tr><td colspan="4" class="muted">Нет данных</td></tr>';
  html+='</tbody></table>';
  const box=document.getElementById('writeoff-reasons'); if(box) box.innerHTML=html;
}

async function loadComparison(){
  const start=document.getElementById('start')?.value;
  const end=document.getElementById('end')?.value;
  const group=document.getElementById('group')?.value || 'day';
  const whIds=getSelectedWarehouseIds();

  // суммарная summary по выбранным складам
  const data=await fetchSummaryTotalsMulti(start,end,group,whIds);
  const t=data.totals||{}; const p=(data.compare||{}).previous||{}; const y=(data.compare||{}).previous_year||{};
  const pack=(r)=>({ rev:r.revenue||0, gp:(r.revenue||0)-(r.cost||0), mar:(r.revenue?(((r.revenue-(r.cost||0))/r.revenue*100)):0), at:(r.receipts?(r.revenue/r.receipts):0), chk:(r.receipts||0), inflow:r.inflow_cost||0 });
  const cur=pack(t), prev=pack(p), yoy=pack(y);

  // для writeoff строк метрики — считаем по причинам на текущий, пред. период и год назад
  const pp=prevPeriodRange(start,end), yy=prevYearRange(start,end);
  const [re_cur, re_prev, re_yoy] = await Promise.all([
    jget(`/api/writeoff/reasons?start=${start}&end=${end}`),
    jget(`/api/writeoff/reasons?start=${pp.start}&end=${pp.end}`),
    jget(`/api/writeoff/reasons?start=${yy.start}&end=${yy.end}`),
  ]);
  const rows_cur = re_cur.data||[], rows_prev=re_prev.data||[], rows_yoy=re_yoy.data||[];
  const def_cur = sumWriteoffByBucket(rows_cur, whIds, 'defect');
  const def_prev= sumWriteoffByBucket(rows_prev, whIds, 'defect');
  const def_yoy = sumWriteoffByBucket(rows_yoy, whIds, 'defect');
  const inv_cur = sumWriteoffByBucket(rows_cur, whIds, 'inventory');
  const inv_prev= sumWriteoffByBucket(rows_prev, whIds, 'inventory');
  const inv_yoy = sumWriteoffByBucket(rows_yoy, whIds, 'inventory');

  // динамические подписи лет
  const endY = parseISO(end).getFullYear();
  const prevEndY = parseISO(start).getFullYear();
  const yoyY = endY-1;

  document.getElementById('compare').textContent=`Период ${start} – ${end} • Склад(ы): ${getSelectedWarehouseIds().length||'все'} • Группировка: ${group}`;

  const rows=[
    ['Выручка',cur.rev,prev.rev,pctDelta(cur.rev,prev.rev),yoy.rev,pctDelta(cur.rev,yoy.rev)],
    ['Валовая прибыль',cur.gp,prev.gp,pctDelta(cur.gp,prev.gp),yoy.gp,pctDelta(cur.gp,yoy.gp)],
    ['Маржа %',cur.mar,prev.mar,cur.mar-prev.mar,yoy.mar,cur.mar-yoy.mar],
    ['Средний чек',cur.at,prev.at,pctDelta(cur.at,prev.at),yoy.at,pctDelta(cur.at,yoy.at)],
    ['Количество чеков',cur.chk,prev.chk,pctDelta(cur.chk,prev.chk),yoy.chk,pctDelta(cur.chk,yoy.chk)],
    // Новые строки:
    ['Оприходования',cur.inflow,prev.inflow,pctDelta(cur.inflow,prev.inflow),yoy.inflow,pctDelta(cur.inflow,yoy.inflow)],
    ['Брак',def_cur,def_prev,pctDelta(def_cur,def_prev),def_yoy,pctDelta(def_cur,def_yoy)],
    ['Брак % от выручки',cur.rev?def_cur/cur.rev*100:0, prev.rev?def_prev/prev.rev*100:0, (cur.rev?def_cur/cur.rev*100:0)-(prev.rev?def_prev/prev.rev*100:0), yoy.rev?def_yoy/yoy.rev*100:0, (cur.rev?def_cur/cur.rev*100:0)-(yoy.rev?def_yoy/yoy.rev*100:0)],
    ['Инвентаризация',inv_cur,inv_prev,pctDelta(inv_cur,inv_prev),inv_yoy,pctDelta(inv_cur,inv_yoy)],
    ['Инвентаризация % от выручки',cur.rev?inv_cur/cur.rev*100:0, prev.rev?inv_prev/prev.rev*100:0, (cur.rev?inv_cur/cur.rev*100:0)-(prev.rev?inv_prev/prev.rev*100:0), yoy.rev?inv_yoy/yoy.rev*100:0, (cur.rev?inv_cur/cur.rev*100:0)-(yoy.rev?inv_yoy/yoy.rev*100:0)],
  ];

  let html=`<table><thead><tr>
  <th>Метрика</th>
  <th>Текущий (${endY})</th>
  <th>Предыдущий (${prevEndY})</th>
  <th>Δ к пред.</th>
  <th>Год назад (${yoyY})</th>
  <th>Δ к прошл. году</th>
  </tr></thead><tbody>`;

  for(const r of rows){
    const [name,curv,pv,d1,yv,d2]=r;
    const isPctRow = name.includes('%');
    const curFmt = isPctRow ? (curv||0).toFixed(2)+' %'
                  : name==='Маржа %'? (curv||0).toFixed(1)+' %'
                  : (name==='Количество чеков'? (curv||0).toFixed(0) : fmt(curv||0)+' ₽');
    const pFmt   = isPctRow ? (pv||0).toFixed(2)+' %'
                  : name==='Маржа %'? (pv||0).toFixed(1)+' %'
                  : (name==='Количество чеков'? (pv||0).toFixed(0) : fmt(pv||0)+' ₽');
    const yFmt   = isPctRow ? (yv||0).toFixed(2)+' %'
                  : name==='Маржа %'? (yv||0).toFixed(1)+' %'
                  : (name==='Количество чеков'? (yv||0).toFixed(0) : fmt(yv||0)+' ₽');
    const d1Fmt  = isPctRow ? (isFinite(d1)? d1.toFixed(2)+' п.п.':'—')
                  : name==='Маржа %'? (isFinite(d1)? d1.toFixed(1)+' п.п.':'—')
                  : (isFinite(d1)? d1.toFixed(1)+' %':'—');
    const d2Fmt  = isPctRow ? (isFinite(d2)? d2.toFixed(2)+' п.п.':'—')
                  : name==='Маржа %'? (isFinite(d2)? d2.toFixed(1)+' п.п.':'—')
                  : (isFinite(d2)? d2.toFixed(1)+' %':'—');
    html+=`<tr><td>${name}</td><td>${curFmt}</td><td>${pFmt}</td><td class="${cls(d1)}">${d1Fmt}</td><td>${yFmt}</td><td class="${cls(d2)}">${d2Fmt}</td></tr>`;
  }
  html+='</tbody></table>';
  document.getElementById('compare-table').innerHTML=html;

  // ТОПы
  await Promise.all([loadTopWarehouses(start,end), loadTopProducts(start,end)]);
  // Сравнительный график
  await loadCompareChart();
  // Списания (график и причины)
  await loadWriteoffBlock();
}

function addChartToggles(){
  try{
    const grid=document.querySelector('.grid'); if(!grid) return;
    if(document.getElementById('chart-toggles')) return;
    const card=document.createElement('div');
    card.className='card col-12';
    card.id='chart-toggles';
    card.innerHTML = `
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
        <span class="muted">Показать графики:</span>
        <label><input type="checkbox" data-target="#chart-revenue" checked> Выручка</label>
        <label><input type="checkbox" data-target="#chart-margin" checked> Маржа/GP</label>
        <label><input type="checkbox" data-target="#chart-inflow" checked> Оприходования</label>
        <label><input type="checkbox" data-target="#chart-writeoff" checked> Списания</label>
      </div>`;
    grid.insertBefore(card, grid.firstChild);
    card.querySelectorAll('input[type=checkbox]').forEach(cb=>{
      cb.addEventListener('change',(e)=>{
        const sel=e.target.getAttribute('data-target');
        const el=document.querySelector(sel)?.closest('.card') || document.querySelector(sel);
        if(el) el.style.display=e.target.checked?'':'none';
      });
    });
  }catch(e){ console.debug('toggles failed', e); }
}

const loadComparisonDebounced = debounced(()=>loadComparison(), 250);

function wireQuickButtons(){
  const qs=(id)=>document.getElementById(id);
  qs('btn-cur-month')?.addEventListener('click', ()=>{ setPeriodCurrentMonth(); loadComparisonDebounced(); });
  qs('btn-prev-month')?.addEventListener('click', ()=>{ setPeriodPrevMonth(); loadComparisonDebounced(); });
  qs('btn-ytd')?.addEventListener('click', ()=>{ setPeriodYTD(); loadComparisonDebounced(); });
  qs('btn-prev-year')?.addEventListener('click', ()=>{ setPeriodPrevYear(); loadComparisonDebounced(); });
  qs('cmp-prev-period')?.addEventListener('change', loadCompareChart);
  qs('cmp-prev-year')?.addEventListener('change', loadCompareChart);
  qs('group')?.addEventListener('change', loadCompareChart);
  qs('apply')?.addEventListener('click', loadComparisonDebounced);
}

async function boot(){
  try{
    setPeriodCurrentMonth();
    await loadWarehouses();
    await loadChartsAndKPI();
    wireQuickButtons();
    addChartToggles();
    await loadComparison();
  }catch(e){ console.error('dashboard boot failed', e); }
}
document.addEventListener('DOMContentLoaded', boot);
