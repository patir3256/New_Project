import pytest
from lib.utils import get_spark_session
from lib.datareader import read_customer,read_orders
from lib.datamanipulation import filter_closed_orders,count_order_joined,filter_status_generic
from lib.configreader import get_app_config
from conftest import spark


def test_read_customer_df(spark):
    customer_count = read_customer(spark,"LOCAL").count()
    assert customer_count == 12435

def test_read_orders(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

@pytest.mark.skip("no need to test")
def test_get_app_config():
    file_path = get_app_config("LOCAL")
    assert file_path['orders.file.path'] == 'data/orders.csv'

@pytest.mark.transformation()
def test_count_orders_state(spark):
    orders_df = read_orders(spark,"LOCAL")
    customers_df = read_customer(spark,"LOCAL")
    join_condition = orders_df.customer_id == customers_df.customer_id
    join_type = "inner"
    joined_df = orders_df.join(customers_df,join_condition,join_type)
    joined_count = count_order_joined(joined_df).count()
    assert joined_count == 44

@pytest.mark.latest
def test_check_closed_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_generic_count = filter_status_generic(orders_df,'CLOSED').count()
    assert filtered_generic_count == 7556

@pytest.mark.runonly
@pytest.mark.parametrize("status,count",
                         [("CLOSED",7556),
                          ("PENDING_PAYMENT",15030),
                          ("COMPLETE",22900)])
def test_check_count(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count1 = filter_status_generic(orders_df,status).count()
    assert filtered_count1 == count


