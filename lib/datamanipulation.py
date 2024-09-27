from pyspark.sql.functions import *

def filter_closed_orders(orders_df):
    return orders_df.filter("order_status == 'CLOSED'")

def join_orders_customers(orders_df,customers_df):
    join_condition = orders_df.customer_id == customers_df.customer_id
    join_type = "inner"
    return orders_df.join(customers_df,join_condition,join_type)

def count_order_joined(joined_df):
    return joined_df.groupBy('state').count()

def filter_status_generic(orders_df,status):
    return orders_df.filter(f"order_status == '{status}'")