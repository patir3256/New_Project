import sys
from lib import datamanipulation,datareader,utils
from pyspark.sql.functions import *
from lib.logger import log4j

if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)
    
    job_run_env = sys.argv[1]

    print("creating spark session")

    spark = utils.get_spark_session(job_run_env)

    print("created spark session")

    orders_df = datareader.read_orders(spark,job_run_env)
    orders_filtered = datamanipulation.filter_closed_orders(orders_df)
    customers_df = datareader.read_customer(spark,job_run_env)
    joined_df = datamanipulation.join_orders_customers(orders_df,customers_df)
    agg_df = datamanipulation.count_order_joined(joined_df)

    agg_df.show()

    print("End of Main")

