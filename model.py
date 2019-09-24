import sqlite3

ORDER_LIST = ['order_id', 'order_name','order_status', 'paid_at', 'quantity', 'lineitem_sku']
SUPPLIER = ['sku', 'acronym', 'lily', 'mendy', 'alair', 'alex', 'myeah']
STOCK = ['sku', 'lily', 'mendy', 'alair', 'alex', 'myeah']

def connectDb(db_name):
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        conn.text_factory = str
        #conn.row_factory = lambda cursor, row: row[0]
    except Error as e:
        print(e)
    return conn

def createTable(conn,flag=False):
    c = conn.cursor()
    table = ['order_list','supplier','stock', 'output']
    if flag:
        for table_name in table:
            drop = "DROP TABLE IF EXISTS {0}".format(table_name)
            c.execute(drop)

    c.execute('''CREATE TABLE order_list
        (order_id STR PRIMARY KEY NOT NULL,
        order_name STR NOT NULL,
        order_status STR NOT NULL,
        paid_at TEXT NOT NULL,
        quantity INT NOT NULL,
        lineitem_sku STR NOT NULL);''')

    c.execute('''CREATE TABLE supplier
        (sku STR PRIMARY KEY NOT NULL,
        acronym STR NOT NULL,
        lily INT,
        mendy INT,
        alair INT,
        alex INT,
        myeah INT);''')

    c.execute('''CREATE TABLE stock
        (sku STR PRIMARY KEY NOT NULL,
        lily INT,
        mendy INT,
        alair INT,
        alex INT,
        myeah INT);''')

    c.execute('''CREATE TABLE output
        (order_id STR PRIMARY KEY NOT NULL,
        order_name STR NOT NULL,
        tracking_number INT,
        notes STR,
        paid_at TEXT NOT NULL,
        quantity INT NOT NULL,
        acronym STR NOT NULL,
        status STR NOT NULL,
        supplier STR );''')

class DbTable():
    db_col = []
    def __init__(self,db_col,db_name):
        for col in db_col:
            self.db_col.append(col)
        self.db_name = db_name
    def getCol(self):
        return self.db_col
    def getName(self):
        return self.db_name

def insertOrder(conn,data):
    c = conn.cursor()
    sql ='''INSERT INTO order_list(order_id, order_name, order_status, paid_at, quantity, lineitem_sku)
            VALUES(?,?,?,?,?,?)'''
    c.execute(sql,data)
    return c

def insertSupplier(conn,data):
    c = conn.cursor()
    sql ='''INSERT INTO supplier(sku, acronym, lily, mendy, alair, alex, myeah)
            VALUES(?,?,?,?,?,?,?)'''
    c.execute(sql,data)
    return c

def insertStock(conn,data):
    c = conn.cursor()
    sql ='''INSERT INTO stock(sku, lily, mendy, alair, alex, myeah)
            VALUES(?,?,?,?,?,?)'''
    c.execute(sql,data)
    return c

def sqlQuery(conn,sql):
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    return c
