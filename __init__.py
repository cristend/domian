import os
import importlib
import csv
import datetime
import zipfile
import shutil
db = importlib.import_module('model')
ORDER = ['Order ID','Order Name','Order Status','Paid_at','Quantity','Lineitem SKU']
PRIORITY = ['SKU','acronym','Agent Lily','Agent Mendy','Agent Alair','Agent Alex','Agent Myeah']
STOCK = ['SKU', 'Agent Lily','Agent Mendy','Agent Alair','Agent Alex','Agent Myeah']
output = []
def readTableCols(filename, flag=True):
    table_cols = {}
    index = 0
    with open(filename) as f:
        reader = csv.reader(f)
        row = next(reader)
        if flag:
            for col in row:
                table_cols.update({col : index})
                index+=1 
        else:
            for col in row:
                if index==1:
                    table_cols.update({'acronym' : index})
                else:
                    table_cols.update({col : index})
                index+=1
    return table_cols

def loadOrder(filename):
    table_cols = readTableCols('csv/order.csv')
    with open(filename) as f:
        reader = csv.reader(f)
        next(reader)
        reader = list(reader)
        for row in reader:
            data = (row[table_cols[ORDER[0]]], row[table_cols[ORDER[1]]], row[table_cols[ORDER[2]]], row[table_cols[ORDER[3]]], row[table_cols[ORDER[4]]], row[table_cols[ORDER[5]]])
            db.insertOrder(conn,data)
            conn.commit()

def loadPriority(filename):
    table_cols = readTableCols('csv/supplier.csv',False)
    with open(filename) as f:
        reader = csv.reader(f)
        next(reader)
        reader = list(reader)
        for row in reader:
            data = (row[table_cols[PRIORITY[0]]], row[table_cols[PRIORITY[1]]], row[table_cols[PRIORITY[2]]], row[table_cols[PRIORITY[3]]], row[table_cols[PRIORITY[4]]], row[table_cols[PRIORITY[5]]], row[table_cols[PRIORITY[6]]])
            db.insertSupplier(conn,data)
            conn.commit()

def loadStock(filename):
    table_cols = readTableCols('csv/stock.csv')
    with open(filename) as f:
        reader = csv.reader(f)
        next(reader)
        reader = list(reader)
        for row in reader:
            data = (row[table_cols[STOCK[0]]], row[table_cols[STOCK[1]]], row[table_cols[STOCK[2]]], row[table_cols[STOCK[3]]], row[table_cols[STOCK[4]]], row[table_cols[STOCK[5]]])
            db.insertStock(conn,data)
            conn.commit()

conn =  db.connectDb('test.db')
sku_list = []
db.createTable(conn,True)
loadOrder('csv/order.csv')
loadPriority('csv/supplier.csv')
loadStock('csv/stock.csv')
#query order_list with sort paid at
sql = "SELECT * FROM order_list ORDER BY paid_at ASC"
data = db.sqlQuery(conn,sql).fetchall()
#from each sku decide supplier base on priority
for row in data:
    sku = row[5]
    #query prio list of sku
    sql = "SELECT acronym, lily, mendy, alair, alex, myeah FROM supplier WHERE sku == '{0}'".format(sku)
    prio = db.sqlQuery(conn,sql).fetchone()
    #query stock list of sku
    sql = "SELECT * FROM stock WHERE sku == '{0}'".format(sku)
    stocks = db.sqlQuery(conn,sql).fetchone()
    #Check status
    if row[2] == 'SHIPPING':
        continue
    #Check if sku have stock chose supplier base on prio
    if stocks is not None:
        table_cols = ['sku', 'lily', 'mendy', 'alair', 'alex', 'myeah']
        #supplier which have sku
        supplier_own_skus = []
        #supplier prio 
        supplier_prios = []
        #map between stock and prio, supplier have position in db table as value and prio_value as key
        map_sp = {}
        index = 0
        #Check which supplier have stock of product
        for stock in stocks:
            if isinstance(stock,int):
                #check if supplier still have stock
                if stock > 0:
                    supplier_own_skus.append(index)
            index+=1
        #atleast one supplier have stock
        if supplier_own_skus:
            for supplier_own_sku in supplier_own_skus:
                supplier_prios.append(prio[supplier_own_sku])
                key = prio[supplier_own_sku]
                value = supplier_own_sku
                map_sp[key] = value
            supplier_prios.sort()
            col_index = map_sp[supplier_prios[0]]
            stock_left =  stocks[col_index] - row[4]
            sql = "UPDATE stock SET {0} = {1} WHERE sku = '{2}'".format(table_cols[col_index], stock_left, sku)
            db.sqlQuery(conn,sql)
            date = datetime.datetime.now()
            if date.strftime('%H') < 12:
                shift = 'a'
            else: 
                shift = 'b'
            day = date.strftime('%d.%m')
            status = "P {0} {1} {2}".format(shift, day, table_cols[col_index])
            sql = "INSERT INTO output(order_id, order_name, paid_at, quantity, acronym, status, supplier ) VALUES('{0}','{1}','{2}','{3}','{4}','{5}','{6}')".format(row[0],row[1], row[3], row[4], prio[0], status, table_cols[col_index])
            db.sqlQuery(conn,sql)
        else:
            sql = "INSERT INTO output(order_id, order_name, paid_at, quantity, acronym, status) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', 'error')".format(row[0], row[1], row[3], row[4], prio[0])
            db.sqlQuery(conn,sql)
    else:
        sql = "INSERT INTO output(order_id, order_name, paid_at, quantity, acronym, status) VALUES('{0}', '{1}', '{2}', '{3}', 'none', 'error')".format(row[0], row[1], row[3], row[4])
        db.sqlQuery(conn,sql)

output_cols = ['Order ID','Order Name','Tracking Number','Notes','Paid_at','Quantity', 'Product Name', 'Status']
sql = "SELECT DISTINCT supplier FROM output WHERE status != 'error'"
supplier = db.sqlQuery(conn,sql).fetchall()
sql = "SELECT order_id, order_name, tracking_number, notes, paid_at, quantity, acronym, status FROM output WHERE status == 'error'"
errors = db.sqlQuery(conn,sql).fetchall()
with open('error_order.csv','wb') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
    writer.writerow(output_cols)
    for error in errors:
        error = list(error)
        writer.writerow(error)
for agent in supplier:
    try:
        os.mkdir(agent[0])
    except OSError:
        pass
for agent in supplier:
    sql = "SELECT order_id, order_name, tracking_number, notes, paid_at, quantity, acronym, status FROM output WHERE supplier=='{0}'".format(agent[0])
    outputs = db.sqlQuery(conn,sql).fetchall()
    num_of_pcs = 0
    date = datetime.datetime.now()
    prc_date = date.strftime('%d.%m')
    for output in outputs:
        num_of_pcs += output[5] 
        with open('{0}/{1}_{2}_{3}_{4}.csv'.format(agent[0], output[6], num_of_pcs, prc_date, agent[0]),'wb') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(output_cols)
            output = list(output)
            writer.writerow(output)
date = datetime.datetime.now()
date = date.strftime('%d_%m_%y')

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), os.path.join(path, '..')))

zipf = zipfile.ZipFile('{0}.zip'.format(date), 'w', zipfile.ZIP_DEFLATED)
for agent in supplier:
    zipdir(agent[0],zipf)
    shutil.rmtree('{0}'.format(agent[0]))
zipf.write('error_order.csv')
os.remove('error_order.csv')
zipf.close()

conn.close()
