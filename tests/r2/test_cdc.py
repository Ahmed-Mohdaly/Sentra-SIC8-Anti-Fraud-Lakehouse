from src.r2_spark.cdc import CdcEvent, apply_cdc
import pytest

def test_cdc_order_and_delete():
    events=[CdcEvent('u1','d',30,None),CdcEvent('u1','c',10,{'n':'a'}),CdcEvent('u1','u',20,{'n':'b'}),CdcEvent('u2','r',25,{'n':'c'})]
    assert apply_cdc(events)=={'u2':{'n':'c'}}

def test_cdc_no_missing_after():
    with pytest.raises(ValueError):apply_cdc([CdcEvent('u','c',1,None)])
