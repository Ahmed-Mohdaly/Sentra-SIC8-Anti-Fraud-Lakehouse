from fastapi.testclient import TestClient
from apps.analyst_portal.api.main import app
from src.common.pipeline import run_local

def test_fixture_api_guarded(tmp_path, monkeypatch):
    root=tmp_path/'demo';run_local(root)
    monkeypatch.setenv('SIC8_DEMO_DATA_ROOT',str(root))
    monkeypatch.setenv('SIC8_DEMO_SQLITE',str(root/'gold_fixture.sqlite'))
    client=TestClient(app)
    assert client.get('/health').json()['live_data'] is False
    assert client.get('/metrics').status_code == 503
    monkeypatch.setenv('SIC8_API_DEMO_TOKEN','test-token-for-local-only')
    assert client.get('/metrics').status_code == 401
    hdr={'Authorization':'Bearer test-token-for-local-only'}
    assert client.get('/metrics',headers=hdr).json()['transactions'] == 13
    assert client.get('/trace/fixture-r1:e0001',headers=hdr).status_code == 200
    assert client.get('/trace/%%',headers=hdr).status_code in (400,404)
    assert client.post('/genie/chat',headers=hdr).status_code == 503
