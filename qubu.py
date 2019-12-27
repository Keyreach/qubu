import sqlite3

class Column():
    def __init__(self, column_type, primary_key=False, null=False, default=None):
        self.column_type = column_type
        self.primary_key = primary_key
        self.nullable = null
        self.default = default
    def render(self):
        result = self.column_type
        if not self.nullable:
            result += ' NOT NULL'
        if self.default is not None:
            result += ' DEFAULT ' + self.default
        if self.primary_key:
            result += ' PRIMARY KEY'
        return result

class Query():

    def __init__(self, table):
        self.table = table
        self.where_expr = None

    def where(self, **where):
        wh_fields = []
        wh_values = []
        for k, v in where.items():
            wh_fields.append('{} = ?'.format(k))
            wh_values.append(v)
        self.where_expr = (wh_fields, wh_values)
        return self

    def select(self, *fields):
        select_sql = ', '.join(fields)
        sql = 'SELECT {} FROM {}'.format(
            select_sql, self.table.name
        )
        if self.where_expr is not None: 
            where_sql = ' AND '.join(self.where_expr[0])
            sql += ' WHERE {}'.format(where_sql)
        return self.table.execute(sql, self.where_expr[1] if self.where_expr else ())

    def insert(self, **values):
        keys = []
        vals = []
        for k, v in values.items():
            keys.append(k)
            vals.append(v)
        fields_sql = ', '.join(keys)
        values_sql = ', '.join(['?'] * len(keys))
        sql = 'INSERT INTO {}({}) VALUES({})'.format(
            self.table.name, fields_sql, values_sql
        )
        return self.table.execute(sql, vals)

    def update(self, **values):
        keys = []
        vals = []
        for k, v in values.items():
            keys.append(k)
            vals.append(v)
        update_sql = ', '.join([ '{} = ?'.format(k) for k in keys])
        sql = 'UPDATE {} SET {}'.format(
            self.table.name, update_sql
        )
        if self.where_expr is not None: 
            where_sql = ' AND '.join(self.where_expr[0])
            sql += ' WHERE {}'.format(where_sql)
        return self.table.execute(sql, vals + (self.where_expr[1] if self.where_expr else ()))

    def delete(self):
        sql = 'DELETE FROM {}'.format(
            self.table.name
        )
        if self.where_expr is not None: 
            where_sql = ' AND '.join(self.where_expr[0])
            sql += ' WHERE {}'.format(where_sql)
        return self.table.execute(sql, self.where_expr[1] if self.where_expr else ())

class Table():

    def __init__(self, name, db):
        self.name = name
        self.db = db
        self.connection = None
        self.cursor = None

    def __call__(self):
        return Query(self)

    def __enter__(self):
        self.connection = sqlite3.connect(self.db.dsn)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, *exc):
        self.cursor.close()
        self.connection.commit()
        self.connection.close()
        self.connection = None
        return None

    def create(self, **fields):
        create_sql = ', '.join(['{} {}'.format(k, v.render()) for k, v in fields.items()])
        sql = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(
            self.name, create_sql
        )
        self.execute(sql)

    def execute(self, sql, values=None):
        if self.connection is None:
            raise Exception('No connection')
        print(sql, values)
        self.cursor.execute(sql, values if values is not None else ())
        return self.cursor

class Database():

    def __init__(self, dsn):
        self.dsn = dsn
        self.tables = {}

    def __getattr__(self, name):
        if not name in self.tables:
            self.tables[name] = Table(name, db=self)
        return self.tables[name]
