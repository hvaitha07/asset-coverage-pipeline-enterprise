from src.common.utils import normalize_hostname
def test_normalize_hostname():
    assert normalize_hostname("host-01.corp.local") == "HOST-01"
    assert normalize_hostname("  ws_99  ") == "WS99"
