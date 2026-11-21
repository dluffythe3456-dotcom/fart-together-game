const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const html=fs.readFileSync(path.join(__dirname,'index.html'));
const srv=http.createServer((q,s)=>{s.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});s.end(html)});
const wss=new WebSocketServer({server:srv,maxPayload:8192});
const C=new Map();let nid=1,dirty=false;
wss.on('connection',ws=>{
 const id='p'+nid++;C.set(id,{ws,pr:{}});ws.send(JSON.stringify({t:'id',id}));dirty=true;
 ws.isAlive=true;
 ws.on('pong',()=>{ws.isAlive=true});
 ws.on('message',d=>{ws.isAlive=true;try{const m=JSON.parse(d);if(m.t==='p'&&m.p&&typeof m.p==='object'){const c=C.get(id);
  if(!c)return;
  for(const k in m.p){if(m.p[k]===null)delete c.pr[k];else c.pr[k]=m.p[k]}dirty=true}}catch(e){}});
 const drop=()=>{if(C.delete(id))dirty=true};
 ws.on('close',drop);
 ws.on('error',drop)});
// Heartbeat: remove players whose phone closed, slept or lost signal
setInterval(()=>{
 for(const [id,c] of C){
  if(c.ws.isAlive===false){c.ws.terminate();C.delete(id);dirty=true;continue}
  c.ws.isAlive=false;
  try{c.ws.ping()}catch(e){}
 }
},5000);
setInterval(()=>{if(!dirty)return;dirty=false;
 const msg=JSON.stringify({t:'peers',list:[...C].map(([id,c])=>({id,pr:c.pr}))});
 for(const c of C.values())if(c.ws.readyState===1)c.ws.send(msg)},33);
srv.listen(process.env.PORT||3000,()=>console.log('Toot Raft running on port '+(process.env.PORT||3000)));
