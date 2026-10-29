import React,{useState} from 'react';
import {createRoot} from 'react-dom/client';
import './style.css';
function App(){
  const [id,setId]=useState('fixture-r1:e0001');
  const [out,setOut]=useState('Only local fixture data can be displayed.');
  const [token,setToken]=useState('');
  async function trace(){
    try{
      const response=await fetch('/api/trace/'+encodeURIComponent(id),{headers:{Authorization:'Bearer '+token}});
      setOut(JSON.stringify(await response.json(),null,2));
    }catch{setOut('Backend unavailable. Do not interpret this page as a LIVE dashboard.');}
  }
  return <main><header><h1>SIC8 Fraud Operations</h1><strong>DEMO_FIXTURE · NOT LIVE</strong></header><p>Transaction trace (read-only). Real backend and R1 model integration not configured.</p><label>Event ID <input value={id} maxLength={128} onChange={e=>setId(e.target.value)} /></label><label>Local demo token <input type="password" value={token} onChange={e=>setToken(e.target.value)} /></label><button onClick={trace}>Inspect fixture event</button><pre>{out}</pre><section><h2>Genie</h2><p>Not configured. Governed Databricks views and entitlements require R4 verification.</p></section></main>
}
createRoot(document.getElementById('root')!).render(<React.StrictMode><App/></React.StrictMode>);
