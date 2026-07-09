import unittest
from hybridjoin import DWLoader


class MockCursor:
    def __init__(self, db):
        self.db = db
        self.last_query = None
        self.last_args = None

    def execute(self, query, args=None):
        self.last_query = query.strip().lower()
        self.last_args = args
        # implement basic SELECT handling used by DWLoader
        if query.strip().lower().startswith('select customer_sk'):
            # args is tuple/list of customer_ids
            out = []
            for row in self.db['dim_customer']:
                if row['customer_id'] in args:
                    out.append((row['customer_sk'], row['customer_id']))
            self._last_select = out
        elif query.strip().lower().startswith('select product_sk'):
            out = []
            for row in self.db['dim_product']:
                if row['product_id'] in args:
                    out.append((row['product_sk'], row['product_id']))
            self._last_select = out
        elif query.strip().lower().startswith('select date_sk'):
            out = []
            for row in self.db['dim_date']:
                if row['date'] in args:
                    out.append((row['date_sk'], row['date']))
            self._last_select = out
        elif query.strip().lower().startswith('select store_sk'):
            out = []
            for row in self.db['dim_store']:
                if row['store_id'] in args:
                    out.append((row['store_sk'], row['store_id']))
            self._last_select = out
        else:
            self._last_select = []

    def fetchall(self):
        return getattr(self, '_last_select', [])

    def executemany(self, query, args_list):
        qs = query.strip().lower()
        if qs.startswith('insert into dim_customer'):
            for tpl in args_list:
                # tpl = (customer_id, gender, age_group, occupation, city_category, stay..., marital_status)
                new_sk = len(self.db['dim_customer']) + 1
                rec = {
                    'customer_sk': new_sk,
                    'customer_id': tpl[0],
                    'gender': tpl[1],
                    'age_group': tpl[2],
                    'occupation': tpl[3],
                    'city_category': tpl[4],
                    'stay_in_current_city_years': tpl[5],
                    'marital_status': tpl[6]
                }
                self.db['dim_customer'].append(rec)
        elif 'insert into dim_product' in qs:
            for tpl in args_list:
                new_sk = len(self.db['dim_product']) + 1
                rec = {
                    'product_sk': new_sk,
                    'product_id': tpl[0],
                    'product_category': tpl[1],
                    'price': tpl[2],
                    'store_id': tpl[3],
                    'supplier_id': tpl[4],
                    'store_name': tpl[5],
                    'supplier_name': tpl[6]
                }
                self.db['dim_product'].append(rec)
        elif qs.startswith('insert ignore into dim_date') or qs.startswith('insert into dim_date'):
            for tpl in args_list:
                # (date, year, month, day)
                if not any(r['date'] == tpl[0] for r in self.db['dim_date']):
                    new_sk = len(self.db['dim_date']) + 1
                    rec = {'date_sk': new_sk, 'date': tpl[0], 'year': tpl[1], 'month': tpl[2], 'day': tpl[3]}
                    self.db['dim_date'].append(rec)
        elif qs.startswith('insert into dim_store'):
            for tpl in args_list:
                new_sk = len(self.db['dim_store']) + 1
                rec = {
                    'store_sk': new_sk,
                    'store_id': tpl[0],
                    'store_name': tpl[1]
                }
                self.db['dim_store'].append(rec)
        elif qs.startswith('insert into fact_sales'):
            for tpl in args_list:
                self.db['fact_sales'].append(tpl)
        else:
            # for other queries just ignore for tests
            pass


class MockConn:
    def __init__(self, db):
        self._db = db
        self.cursor_obj = MockCursor(self._db)

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

    def rollback(self):
        pass


class TestDWLoaderFix(unittest.TestCase):
    def setUp(self):
        # in-memory "database"
        self.db = {'dim_customer': [], 'dim_product': [], 'dim_date': [], 'dim_store': [], 'fact_sales': []}

        # instantiate without calling DWLoader.__init__ (avoid real DB connect)
        self.loader = object.__new__(DWLoader)
        self.loader.conn = MockConn(self.db)
        self.loader.cursor = self.loader.conn.cursor_obj
        self.loader.batch_size = 1000
        self.loader.buffer = []
        import threading
        self.loader.lock = threading.Lock()
        self.loader.customer_cache = {}
        self.loader.product_cache = {}
        self.loader.date_cache = {}
        self.loader.store_cache = {}
        self.loader.customer_master = None
        self.loader.product_master = None
        self.loader._customer_detail_cache = {}
        self.loader._product_detail_cache = {}
        self.loader._store_lookup = {}

    def test_dim_populated_from_rows(self):
        rows = [
            {
                'orderID': '1',
                'Customer_ID': 'CUST_1',
                'Product_ID': 'PROD_1',
                'quantity': '2',
                'date': '2020-01-01',
                'Gender': 'F',
                'Age': '18-25',
                'Occupation': '5',
                'City_Category': 'A',
                'Stay_In_Current_City_Years': '2',
                'Marital_Status': '0',
                'Product_Category': 'Grocery',
                'price$': '10.5',
                'storeID': '33',
                'supplierID': '77',
                'storeName': 'TestStore',
                'supplierName': 'TestSupplier'
            }
        ]

        # directly flush this small batch to simulate DB write
        self.loader._flush_batch(rows)

        # dim_customer should have one row with attributes from rows[0]
        self.assertEqual(len(self.db['dim_customer']), 1)
        cust = self.db['dim_customer'][0]
        self.assertEqual(cust['customer_id'], 'CUST_1')
        self.assertEqual(cust['gender'], 'F')
        self.assertEqual(cust['age_group'], '18-25')
        self.assertEqual(cust['occupation'], '5')

        # dim_product should have one row with attributes
        self.assertEqual(len(self.db['dim_product']), 1)
        prod = self.db['dim_product'][0]
        self.assertEqual(prod['product_id'], 'PROD_1')
        self.assertEqual(prod['product_category'], 'Grocery')
        self.assertAlmostEqual(prod['price'], 10.5)

        # dim_date should have the date
        self.assertEqual(len(self.db['dim_date']), 1)
        self.assertEqual(self.db['dim_date'][0]['date'], '2020-01-01')

        # fact sales should have one row (order_id, customer_sk, product_sk, date_sk, qty, total_price)
        self.assertEqual(len(self.db['fact_sales']), 1)
        fact = self.db['fact_sales'][0]
        self.assertEqual(fact[0], '1')
        # SKs should refer to the inserted dims
        self.assertEqual(fact[1], self.db['dim_customer'][0]['customer_sk'])
        self.assertEqual(fact[2], self.db['dim_product'][0]['product_sk'])
        self.assertEqual(fact[3], self.db['dim_date'][0]['date_sk'])
        self.assertEqual(fact[4], 2)
        self.assertEqual(fact[5], 21.0)  # 2 * 10.5

        # dim_store should have been populated from product attributes
        self.assertEqual(len(self.db['dim_store']), 1)
        store = self.db['dim_store'][0]
        self.assertEqual(store['store_id'], '33')
        self.assertEqual(store['store_name'], 'TestStore')


if __name__ == '__main__':
    unittest.main()
