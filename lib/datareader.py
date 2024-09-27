from lib import configreader

#defining customer schema 
def customer_schema():
    customer_schema = """customer_id int,customer_fname string,customer_lname string,
    username string,password string,address string,city string,
    state string,pincode string"""
    return customer_schema

#defining customer dataframe
def read_customer(spark,env):
    conf = configreader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read.format("csv").\
                 option("header","true"). \
                 schema(customer_schema()). \
                 load(customers_file_path)

#defining orders schema
def orders_schema():
    order_schema = """order_id int,order_date string,
                    customer_id int,order_status string"""
    return order_schema

#defining order dataframe
def read_orders(spark,env):
    conf = configreader.get_app_config(env)
    orders_file_path = conf["orders.file.path"]
    return spark.read.format("csv"). \
                 option("header","true"). \
                 schema(orders_schema()). \
                 load(orders_file_path)