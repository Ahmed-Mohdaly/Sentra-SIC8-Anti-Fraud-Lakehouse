from src.common.pipeline import run_local

def test_denominators_are_explicit(tmp_path):
    report=run_local(tmp_path)
    assert report['truth_positive_events'] >= report['detected_truth_positive_events']
    assert report['terminal_attempts'] >= report['terminal_failures']
    assert report['unique_flagged_transactions']<=report['alerts']
    assert report['event_detection_rate'] is not None
