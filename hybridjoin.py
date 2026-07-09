#!/usr/bin/env python3

import argparse
import csv
import mysql.connector
import math
import threading
import time
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional

# ----------------------- Utility data structures -----------------------
class DoublyLinkedNode:
    def __init__(self, key, payload=None):
        self.key = key
        self.payload = payload
        self.prev = None
        self.next = None

class DoublyLinkedQueue:
    """FIFO queue with ability to remove arbitrary nodes."""
    def __init__(self):
        self.head: Optional[DoublyLinkedNode] = None
        self.tail: Optional[DoublyLinkedNode] = None
        self.size = 0

    def append(self, node: DoublyLinkedNode):
        if not self.head:
            self.head = self.tail = node
            node.prev = node.next = None
        else:
            node.prev = self.tail
            node.next = None
            self.tail.next = node
            self.tail = node
        self.size += 1

    def popleft(self) -> Optional[DoublyLinkedNode]:
        if not self.head:
            return None
        node = self.head
        self.head = node.next
        if self.head:
            self.head.prev = None
        else:
            self.tail = None
        node.next = node.prev = None
        self.size -= 1
        return node

    def remove(self, node: DoublyLinkedNode):
        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next
        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev
        node.prev = node.next = None
        self.size -= 1

# ----------------------- Hash Table (multi-map) -----------------------
class HybridHashTable:
    def __init__(self, hS: int = 10000):
        self.hS = hS
        self.slots: Dict[int, List[Dict[str,Any]]] = defaultdict(list)
        self.count = 0

    def _slot(self, key: str) -> int:
        return hash(key) % self.hS

    def insert(self, key: str, record: dict, queue_node: DoublyLinkedNode):
        s = self._slot(key)
        self.slots[s].append({"key": key, "record": record, "node": queue_node})
        self.count += 1

    def find_and_delete(self, key: str) -> List[dict]:
        s = self._slot(key)
        matched = []
        remaining = []
        for entry in self.slots.get(s, []):
            if entry["key"] == key:
                matched.append(entry)
            else:
                remaining.append(entry)
        if matched:
            if remaining:
                self.slots[s] = remaining
            else:
                if s in self.slots:
                    del self.slots[s]
            self.count -= len(matched)
        return matched

    def delete_by_node(self, node: DoublyLinkedNode):
        key = node.key
        s = self._slot(key)
        lst = self.slots.get(s, [])
        for i, entry in enumerate(lst):
            if entry["node"] is node:
                lst.pop(i)
                self.count -= 1
                break
        if not lst and s in self.slots:
            del self.slots[s]

# ----------------------- Stream Buffer -----------------------
class StreamBuffer:
    def __init__(self):
        self.buffer: List[dict] = []
        self.lock = threading.Lock()

    def put(self, tup: dict):
        with self.lock:
            self.buffer.append(tup)

    def get_batch(self, n: int) -> List[dict]:
        with self.lock:
            batch = self.buffer[:n]
            self.buffer = self.buffer[n:]
        return batch

    def size(self) -> int:
        with self.lock:
            return len(self.buffer)

# ----------------------- Disk Buffer (simulated master data) -----------------------
class MasterData:
    def __init__(self, csv_path: str, key_col: str):
        self.csv_path = csv_path
        self.key_col = key_col
        self.rows: List[dict] = []
        self.index_map: Dict[str, int] = {}
        self._load_index()

    def _load_index(self):
        try:
            with open(self.csv_path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for r in reader:
                    row_count += 1
                    self.rows.append(r)
                    key = r.get(self.key_col)
                    if key is not None and key not in self.index_map:
                        self.index_map[key] = len(self.rows) - 1
                print(f'Loaded {row_count} rows from {self.csv_path}, indexed {len(self.index_map)} unique keys for column "{self.key_col}"')
                if row_count == 0:
                    print(f'WARNING: No rows loaded from {self.csv_path}')
                if len(self.index_map) == 0:
                    print(f'WARNING: No keys indexed. Available columns: {list(reader.fieldnames) if reader.fieldnames else "None"}')
        except Exception as e:
            print(f'ERROR loading {self.csv_path}: {e}')
            import traceback
            traceback.print_exc()
            raise

    def find_index(self, match_key: str) -> Optional[int]:
        if match_key is None:
            return None
        return self.index_map.get(match_key)

    def load_partition(self, start_idx: int, vP: int) -> List[dict]:
        if start_idx is None:
            start_idx = 0
        if start_idx < 0:
            start_idx = 0
        end = min(start_idx + vP, len(self.rows))
        return self.rows[start_idx:end]

    def get_record(self, key: str) -> Optional[dict]:
        idx = self.find_index(key)
        if idx is None:
            return None
        return self.rows[idx]

# ----------------------- Helpers -----------------------
def _chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fix_date_format(d: str) -> str:
    """Convert various date formats to YYYY-MM-DD."""
    if not d:
        return ''
    d = str(d).strip()
    if not d or d.lower() in ['none', 'null', '']:
        return ''
    
    # Already in YYYY-MM-DD format
    if len(d) == 10 and d.count('-') == 2:
        try:
            parts = d.split('-')
            if len(parts) == 3 and len(parts[0]) == 4:
                # Validate it's a valid date
                int(parts[0])  # year
                int(parts[1])  # month
                int(parts[2])  # day
                return d
        except (ValueError, IndexError):
            pass
    
    # MM/DD/YYYY format
    if "/" in d:
        try:
            parts = d.split("/")
            if len(parts) == 3:
                mm, dd, yyyy = parts
                mm = mm.zfill(2)
                dd = dd.zfill(2)
                # Handle YYYY/MM/DD format too
                if len(yyyy) == 4 and len(mm) <= 2 and len(dd) <= 2:
                    return f"{yyyy}-{mm}-{dd}"
                elif len(mm) == 4:  # YYYY/MM/DD format
                    return f"{mm}-{dd.zfill(2)}-{yyyy.zfill(2)}"
                else:
                    return f"{yyyy}-{mm}-{dd}"
        except (ValueError, IndexError):
            pass
    
    # Try to parse as various formats
    try:
        from datetime import datetime
        # Try common formats
        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y', '%m-%d-%Y']:
            try:
                dt = datetime.strptime(d, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
    except Exception:
        pass
    
    # If all else fails, return empty string
    print(f"Warning: Could not parse date format: '{d}'")
    return ''

# ----------------------- DW Loader -----------------------
class DWLoader:

    def __init__(self, host, port, user, password, db, batch_size: int = 1000,
                 customer_master: Optional[MasterData] = None,
                 product_master: Optional[MasterData] = None):
        try:
            print(f'Connecting to database {db} on {host}:{port} as {user}...')
            self.conn = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=db,
                autocommit=False
            )
            self.cursor = self.conn.cursor()
            print('Database connection established successfully.')
        except Exception as e:
            print(f'ERROR: Failed to connect to database: {e}')
            raise
        self.batch_size = batch_size
        self.buffer: List[dict] = []
        self.lock = threading.Lock()

        self.customer_cache: Dict[str, int] = {}
        self.product_cache: Dict[str, int] = {}
        self.date_cache: Dict[str, int] = {}
        self.store_cache: Dict[str, int] = {}

        # Store master data references for direct lookups
        self.customer_master = customer_master
        self.product_master = product_master

        # Lightweight caches for master-data lookups
        self._customer_detail_cache: Dict[str, dict] = {}
        self._product_detail_cache: Dict[str, dict] = {}
        self._store_lookup: Dict[str, dict] = {}

        if self.product_master:
            for row in self.product_master.rows:
                store_id = row.get('storeID')
                if store_id and store_id not in self._store_lookup:
                    self._store_lookup[store_id] = {
                        'store_id': store_id,
                        'store_name': row.get('storeName')
                    }

    def insert_enriched(self, enriched_rows: List[dict]):
        if not enriched_rows:
            return
        with self.lock:
            self.buffer.extend(enriched_rows)
            if len(self.buffer) >= self.batch_size:
                to_flush = self.buffer[:self.batch_size]
                self.buffer = self.buffer[self.batch_size:]
                self._flush_batch(to_flush)

    def flush_all(self):
        with self.lock:
            remaining = len(self.buffer)
            if remaining > 0:
                print(f'Flushing final batch of {remaining} rows...')
            while self.buffer:
                to_flush = self.buffer[:self.batch_size]
                self.buffer = self.buffer[self.batch_size:]
                try:
                    self._flush_batch(to_flush)
                except Exception as e:
                    print(f'Error flushing final batch: {e}')
                    import traceback
                    traceback.print_exc()
                    break
            if remaining > 0:
                print(f'Finished flushing {remaining} remaining rows.')

    def _flush_batch(self, rows: List[dict]):
        if not rows:
            return
        try:
            aggregated: Dict[tuple, Dict[str, Any]] = {}
            skipped_rows = []
            for r in rows:
                order_id = (r.get('orderID') or r.get('order_id') or '').strip()
                cust_id = (r.get('Customer_ID') or r.get('customer_id') or '').strip()
                prod_id = (r.get('Product_ID') or r.get('product_id') or '').strip()
                order_date = fix_date_format((r.get('date') or r.get('Date') or '').strip())
                
                # Try to recover missing values
                if not order_id:
                    skipped_rows.append(f"Missing orderID: {r}")
                    continue
                if not cust_id:
                    skipped_rows.append(f"Missing Customer_ID: {r}")
                    continue
                if not prod_id:
                    skipped_rows.append(f"Missing Product_ID: {r}")
                    continue
                if not order_date:
                    # Try alternative date fields
                    alt_date = fix_date_format((r.get('order_date') or r.get('Order_Date') or r.get('Date') or '').strip())
                    if alt_date:
                        order_date = alt_date
                    else:
                        skipped_rows.append(f"Missing/invalid date: {r.get('date') or r.get('order_date')}")
                        continue
                
                qty = self._safe_int(r.get('quantity', 1), 1)
                price = self._determine_price(r, sample_rows=rows)
                key = (order_id, cust_id, prod_id, order_date)
                entry = aggregated.setdefault(key, {'qty': 0, 'total_price': 0.0})
                entry['qty'] += qty
                entry['total_price'] += price * qty
            
            if skipped_rows:
                print(f"Warning: Skipped {len(skipped_rows)} rows due to missing required fields. First few: {skipped_rows[:5]}")

            if not aggregated:
                return

            cust_ids = list({key[1] for key in aggregated.keys()})
            prod_ids = list({key[2] for key in aggregated.keys()})
            dates = list({key[3] for key in aggregated.keys()})

            # pass rows being flushed so resolvers can use exact payloads for fallback lookups
            self._resolve_customers(cust_ids, sample_rows=rows)
            self._resolve_products(prod_ids, sample_rows=rows)
            self._resolve_dates(dates)
            store_ids = []
            for pid in prod_ids:
                prod_record = self._lookup_product_source(pid, sample_rows=rows)
                if not prod_record:
                    continue
                store_id = prod_record.get('storeID')
                if store_id:
                    store_ids.append(store_id)
            if store_ids:
                self._resolve_stores(store_ids, sample_rows=rows)

            fact_batch = []
            for (order_id, cust_id, prod_id, order_date), metrics in aggregated.items():
                # Ensure date is normalized for cache lookup
                normalized_date = fix_date_format(str(order_date).strip()) if order_date else ''
                if not normalized_date:
                    print(f"ERROR: Invalid date after normalization: '{order_date}' for order_id={order_id}")
                    continue
                
                cust_sk = self.customer_cache.get(cust_id)
                prod_sk = self.product_cache.get(prod_id)
                # Try multiple cache keys in case of format differences
                date_sk = (self.date_cache.get(normalized_date) or 
                          self.date_cache.get(str(normalized_date)) or
                          self.date_cache.get(order_date))

                if cust_sk is None or prod_sk is None or date_sk is None:
                    # Resolve missing SKs
                    if cust_sk is None:
                        self._resolve_customers([cust_id])
                        cust_sk = self.customer_cache.get(cust_id)
                    if prod_sk is None:
                        self._resolve_products([prod_id])
                        prod_sk = self.product_cache.get(prod_id)
                    if date_sk is None:
                        # Resolve date and ensure it's in cache
                        self._resolve_dates([normalized_date])
                        date_sk = (self.date_cache.get(normalized_date) or 
                                  self.date_cache.get(str(normalized_date)) or
                                  self.date_cache.get(order_date))

                if cust_sk is None or prod_sk is None or date_sk is None:
                    # Last attempt to resolve - try individual lookups with retry
                    missing = []
                    if cust_sk is None:
                        missing.append(f"customer_sk for {cust_id}")
                        # Try one more time to find/create customer
                        try:
                            self._resolve_customers([cust_id], sample_rows=rows)
                            cust_sk = self.customer_cache.get(cust_id)
                        except Exception as e:
                            print(f"Failed to resolve customer {cust_id}: {e}")
                    if prod_sk is None:
                        missing.append(f"product_sk for {prod_id}")
                        try:
                            self._resolve_products([prod_id], sample_rows=rows)
                            prod_sk = self.product_cache.get(prod_id)
                        except Exception as e:
                            print(f"Failed to resolve product {prod_id}: {e}")
                    if date_sk is None:
                        missing.append(f"date_sk for {normalized_date} (original: {order_date})")
                        try:
                            # Try resolving again with normalized date
                            self._resolve_dates([normalized_date])
                            date_sk = (self.date_cache.get(normalized_date) or 
                                      self.date_cache.get(str(normalized_date)) or
                                      self.date_cache.get(order_date))
                            if date_sk is None:
                                # Direct database query as last resort
                                try:
                                    q = "SELECT date_sk FROM dim_date WHERE date = %s"
                                    self.cursor.execute(q, (normalized_date,))
                                    result = self.cursor.fetchone()
                                    if result:
                                        date_sk = result[0]
                                        self.date_cache[normalized_date] = date_sk
                                        self.date_cache[str(normalized_date)] = date_sk
                                        print(f"Resolved date {normalized_date} via direct query: date_sk={date_sk}")
                                except Exception as db_err:
                                    print(f"Direct database query failed for date {normalized_date}: {db_err}")
                        except Exception as e:
                            print(f"Failed to resolve date {normalized_date} (original: {order_date}): {e}")
                            import traceback
                            traceback.print_exc()
                    
                    if cust_sk is None or prod_sk is None or date_sk is None:
                        print(f"ERROR: Cannot resolve SKs for fact row - {', '.join(missing)}. Row: order_id={order_id}, cust_id={cust_id}, prod_id={prod_id}, date={order_date}")
                        continue

                # Final validation - ensure all SKs are valid integers
                if cust_sk is None or prod_sk is None or date_sk is None:
                    print(f"ERROR: Cannot insert fact row with NULL SKs: cust_sk={cust_sk}, prod_sk={prod_sk}, date_sk={date_sk} for order_id={order_id}")
                    continue
                
                # Ensure SKs are integers
                try:
                    cust_sk = int(cust_sk)
                    prod_sk = int(prod_sk)
                    date_sk = int(date_sk)
                except (ValueError, TypeError) as e:
                    print(f"ERROR: Invalid SK types: cust_sk={cust_sk} (type: {type(cust_sk)}), prod_sk={prod_sk} (type: {type(prod_sk)}), date_sk={date_sk} (type: {type(date_sk)})")
                    continue
                
                total_price = round(metrics['total_price'], 2)
                fact_batch.append((
                    order_id,
                    cust_sk,
                    prod_sk,
                    date_sk,
                    metrics['qty'],
                    total_price
                ))

            if fact_batch:
                # Validate all fact records before inserting
                valid_batch = []
                invalid_count = 0
                for fact_row in fact_batch:
                    order_id, cust_sk, prod_sk, date_sk, qty, price = fact_row
                    if cust_sk is None or prod_sk is None or date_sk is None:
                        invalid_count += 1
                        print(f"WARNING: Skipping fact row with NULL SK: order_id={order_id}, cust_sk={cust_sk}, prod_sk={prod_sk}, date_sk={date_sk}")
                        continue
                    valid_batch.append(fact_row)
                
                if invalid_count > 0:
                    print(f"WARNING: {invalid_count} fact records skipped due to NULL foreign keys")
                
                if valid_batch:
                    fact_sql = """
                        INSERT INTO fact_sales
                        (order_id, customer_sk, product_sk, date_sk, quantity, total_price)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                            quantity = quantity + VALUES(quantity),
                            total_price = total_price + VALUES(total_price)
                    """
                    try:
                        self.cursor.executemany(fact_sql, valid_batch)
                        print(f"Successfully inserted/updated {len(valid_batch)} fact records (skipped {invalid_count} invalid)")
                    except Exception as e:
                        print(f"Error inserting fact records: {e}")
                        # Try inserting one by one to identify problematic rows
                        success_count = 0
                        for i, fact_row in enumerate(valid_batch):
                            try:
                                self.cursor.execute(fact_sql, fact_row)
                                success_count += 1
                            except Exception as row_err:
                                print(f"Error inserting fact row {i}: {fact_row}, error: {row_err}")
                        print(f"Partial success: {success_count}/{len(valid_batch)} fact records inserted")
                        raise

            self.conn.commit()

        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            print("DWLoader _flush_batch error:", e)
            raise

    # ----------------------- Lookup helpers -----------------------
    def _lookup_customer_source(self, cust_id: Optional[str], sample_rows: Optional[List[dict]] = None) -> Optional[dict]:
        if not cust_id:
            return None
        if cust_id in self._customer_detail_cache:
            return self._customer_detail_cache[cust_id]
        if self.customer_master:
            record = self.customer_master.get_record(cust_id)
            if record:
                self._customer_detail_cache[cust_id] = record
                return record
        if sample_rows:
            for r in sample_rows:
                if r.get('Customer_ID') == cust_id:
                    self._customer_detail_cache[cust_id] = r
                    return r
        return None

    def _lookup_product_source(self, prod_id: Optional[str], sample_rows: Optional[List[dict]] = None) -> Optional[dict]:
        if not prod_id:
            return None
        if prod_id in self._product_detail_cache:
            return self._product_detail_cache[prod_id]
        if self.product_master:
            record = self.product_master.get_record(prod_id)
            if record:
                self._product_detail_cache[prod_id] = record
                return record
        if sample_rows:
            for r in sample_rows:
                if r.get('Product_ID') == prod_id:
                    self._product_detail_cache[prod_id] = r
                    return r
        return None

    @staticmethod
    def _safe_int(value, default:int = 0) -> int:
        try:
            if value is None:
                return default
            if isinstance(value, int):
                return value
            s = str(value).strip()
            if not s:
                return default
            return int(float(s))
        except Exception:
            return default

    @staticmethod
    def _safe_float(value, default: Optional[float] = 0.0) -> Optional[float]:
        try:
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return float(value)
            s = str(value).strip()
            if not s:
                return default
            return float(s)
        except Exception:
            return default

    def _determine_price(self, row: dict, sample_rows: Optional[List[dict]] = None) -> float:
        raw_price = row.get('price$') or row.get('price') or row.get('Price')
        price = self._safe_float(raw_price, None)
        if price is not None:
            return price
        prod_id = row.get('Product_ID')
        prod_record = self._lookup_product_source(prod_id, sample_rows=sample_rows)
        if prod_record:
            price = self._safe_float(prod_record.get('price$') or prod_record.get('price') or prod_record.get('Price'))
            if price is not None:
                return price
        return 0.0

    def _resolve_stores(self, store_ids: List[str], sample_rows: Optional[List[dict]] = None):
        to_check = [sid for sid in store_ids if sid and sid not in self.store_cache]
        if not to_check:
            return

        chunks = list(_chunked(to_check, 500))
        for chunk in chunks:
            q = "SELECT store_sk, store_id FROM dim_store WHERE store_id IN ({})".format(
                ",".join(["%s"] * len(chunk))
            )
            self.cursor.execute(q, chunk)
            for sk, sid in self.cursor.fetchall():
                self.store_cache[sid] = sk

        missing = [sid for sid in to_check if sid not in self.store_cache]
        if not missing:
            return

        inserts = []
        for sid in missing:
            store_data = self._store_lookup.get(str(sid).strip())
            if not store_data and sample_rows:
                for r in sample_rows:
                    if str(r.get('storeID', '')).strip() == str(sid).strip():
                        store_name = r.get('storeName') or r.get('store_name')
                        store_data = {'store_id': str(sid).strip(), 'store_name': store_name}
                        break
            if store_data:
                store_name = store_data.get('store_name') or None
                inserts.append((str(sid).strip(), store_name if store_name else None))
            else:
                # Create store with NULL name - store exists but name unknown
                inserts.append((str(sid).strip(), None))

        if inserts:
            insert_sql = """
                INSERT INTO dim_store (store_id, store_name)
                VALUES (%s,%s)
                ON DUPLICATE KEY UPDATE
                    store_name = VALUES(store_name)
            """
            try:
                self.cursor.executemany(insert_sql, inserts)
                self.conn.commit()
            except Exception as e:
                print(f"Error inserting stores: {e}")
                self.conn.rollback()
                raise

        for chunk in _chunked([i[0] for i in inserts], 500):
            q = "SELECT store_sk, store_id FROM dim_store WHERE store_id IN ({})".format(
                ",".join(["%s"] * len(chunk))
            )
            self.cursor.execute(q, chunk)
            for sk, sid in self.cursor.fetchall():
                self.store_cache[sid] = sk

    @staticmethod
    def _normalize_date_key(value: Any) -> str:
        if value is None:
            return ''
        if isinstance(value, str):
            return value
        try:
            return value.strftime("%Y-%m-%d")
        except Exception:
            return str(value)

    # ----------------------- CUSTOMER -----------------------
    def _resolve_customers(self, cust_ids: List[str], sample_rows: Optional[List[dict]] = None):
        to_check = [cid for cid in cust_ids if cid not in self.customer_cache]
        if not to_check:
            return

        chunks = list(_chunked(to_check, 500))
        for chunk in chunks:
            q = "SELECT customer_sk, customer_id FROM dim_customer WHERE customer_id IN ({})".format(
                ",".join(["%s"] * len(chunk))
            )
            self.cursor.execute(q, chunk)
            for sk, cid in self.cursor.fetchall():
                self.customer_cache[cid] = sk

        missing = [cid for cid in to_check if cid not in self.customer_cache]
        if missing:
            inserts = []
            for cid in missing:
                search_space = sample_rows if sample_rows is not None else self.buffer
                customer_data = self._lookup_customer_source(cid, sample_rows=search_space)

                if customer_data:
                    # Ensure no empty strings are passed as None for proper NULL handling
                    gender = customer_data.get('Gender') or None
                    age = customer_data.get('Age') or None
                    occupation = customer_data.get('Occupation') or None
                    city_cat = customer_data.get('City_Category') or None
                    stay_years = customer_data.get('Stay_In_Current_City_Years') or None
                    marital = customer_data.get('Marital_Status')
                    # Convert marital status to int if it's a string number
                    if marital is not None and isinstance(marital, str):
                        try:
                            marital = int(float(marital.strip()))
                        except (ValueError, AttributeError):
                            marital = None
                    
                    inserts.append((
                        cid,
                        gender if gender else None,
                        age if age else None,
                        occupation if occupation else None,
                        city_cat if city_cat else None,
                        stay_years if stay_years else None,
                        marital
                    ))
                else:
                    # Create record with NULLs - customer exists in transactional data but not master
                    inserts.append((cid, None, None, None, None, None, None))

            if inserts:
                insert_sql = """
                    INSERT INTO dim_customer
                    (customer_id, gender, age_group, occupation, city_category, stay_in_current_city_years, marital_status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        gender = COALESCE(VALUES(gender), gender),
                        age_group = COALESCE(VALUES(age_group), age_group),
                        occupation = COALESCE(VALUES(occupation), occupation),
                        city_category = COALESCE(VALUES(city_category), city_category),
                        stay_in_current_city_years = COALESCE(VALUES(stay_in_current_city_years), stay_in_current_city_years),
                        marital_status = COALESCE(VALUES(marital_status), marital_status)
                """
                try:
                    self.cursor.executemany(insert_sql, inserts)
                    self.conn.commit()
                except Exception as e:
                    print(f"Error inserting customers: {e}")
                    self.conn.rollback()
                    raise

            for chunk in _chunked([i[0] for i in inserts], 500):
                q = "SELECT customer_sk, customer_id FROM dim_customer WHERE customer_id IN ({})".format(
                    ",".join(["%s"] * len(chunk))
                )
                self.cursor.execute(q, chunk)
                for sk, cid in self.cursor.fetchall():
                    self.customer_cache[cid] = sk

    # ----------------------- PRODUCT -----------------------
    def _resolve_products(self, prod_ids: List[str], sample_rows: Optional[List[dict]] = None):
        to_check = [pid for pid in prod_ids if pid not in self.product_cache]
        if not to_check:
            return

        chunks = list(_chunked(to_check, 500))
        for chunk in chunks:
            q = "SELECT product_sk, product_id FROM dim_product WHERE product_id IN ({})".format(
                ",".join(["%s"] * len(chunk))
            )
            self.cursor.execute(q, chunk)
            for sk, pid in self.cursor.fetchall():
                self.product_cache[pid] = sk

        missing = [pid for pid in to_check if pid not in self.product_cache]
        if missing:
            inserts = []
            store_ids_needed = set()
            search_space = sample_rows if sample_rows is not None else self.buffer
            for pid in missing:
                product_data = self._lookup_product_source(pid, sample_rows=search_space)

                if product_data:
                    price_val = self._safe_float(product_data.get('price$') or product_data.get('price') or product_data.get('Price'))
                    if price_val is None:
                        price_val = 0.0
                    
                    store_id = product_data.get('storeID') or product_data.get('store_id') or None
                    if store_id:
                        store_ids_needed.add(str(store_id).strip())
                    
                    # Ensure empty strings become None
                    category = product_data.get('Product_Category') or None
                    supplier_id = product_data.get('supplierID') or product_data.get('supplier_id') or None
                    store_name = product_data.get('storeName') or product_data.get('store_name') or None
                    supplier_name = product_data.get('supplierName') or product_data.get('supplier_name') or None
                    
                    inserts.append((
                        pid,
                        category if category else None,
                        price_val,
                        store_id.strip() if store_id else None,
                        supplier_id.strip() if supplier_id else None,
                        store_name if store_name else None,
                        supplier_name if supplier_name else None
                    ))
                else:
                    # Create record with defaults - product exists in transactional data but not master
                    inserts.append((pid, None, 0.0, None, None, None, None))

            if inserts:
                insert_sql = """
                    INSERT INTO dim_product
                    (product_id, product_category, price, store_id, supplier_id, store_name, supplier_name)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        product_category = COALESCE(VALUES(product_category), product_category),
                        price = COALESCE(VALUES(price), price),
                        store_id = COALESCE(VALUES(store_id), store_id),
                        supplier_id = COALESCE(VALUES(supplier_id), supplier_id),
                        store_name = COALESCE(VALUES(store_name), store_name),
                        supplier_name = COALESCE(VALUES(supplier_name), supplier_name)
                """
                try:
                    self.cursor.executemany(insert_sql, inserts)
                    self.conn.commit()
                except Exception as e:
                    print(f"Error inserting products: {e}")
                    self.conn.rollback()
                    raise

            if store_ids_needed:
                self._resolve_stores(list(store_ids_needed), sample_rows=search_space)

            for chunk in _chunked([i[0] for i in inserts], 500):
                q = "SELECT product_sk, product_id FROM dim_product WHERE product_id IN ({})".format(
                    ",".join(["%s"] * len(chunk))
                )
                self.cursor.execute(q, chunk)
                for sk, pid in self.cursor.fetchall():
                    self.product_cache[pid] = sk

    # ----------------------- DATE -----------------------
    def _resolve_dates(self, dates: List[str]):
        if not dates:
            return
        
        # Normalize all dates first
        norm_dates = []
        for d in dates:
            if not d:
                continue
            fixed = fix_date_format(str(d).strip())
            if fixed and fixed not in norm_dates:  # Avoid duplicates
                norm_dates.append(fixed)
        
        if not norm_dates:
            print(f"Warning: No valid dates found in input list: {dates[:5]}")
            return
        
        # Check which dates are not in cache
        to_check = [d for d in norm_dates if d and d not in self.date_cache]
        if not to_check:
            return

        # First, query existing dates from database
        chunks = list(_chunked(to_check, 500))
        for chunk in chunks:
            try:
                q = "SELECT date_sk, date FROM dim_date WHERE date IN ({})".format(",".join(["%s"] * len(chunk)))
                self.cursor.execute(q, chunk)
                results = self.cursor.fetchall()
                for sk, d in results:
                    # Use date string as key (already normalized)
                    key = self._normalize_date_key(d)
                    self.date_cache[key] = sk
                    if d in to_check:
                        # Also cache by the original format in case of differences
                        self.date_cache[d] = sk
            except Exception as e:
                print(f"Error querying dates: {e}, chunk: {chunk[:3]}")
                import traceback
                traceback.print_exc()
                raise

        # Find dates that still need to be inserted
        missing = [d for d in to_check if d not in self.date_cache]
        if missing:
            inserts = []
            for d in missing:
                try:
                    parts = d.split('-')
                    if len(parts) == 3:
                        yyyy, mm, dd = parts
                        y = int(yyyy.strip())
                        m = int(mm.strip())
                        day = int(dd.strip())
                        # Validate range
                        if not (1 <= m <= 12) or not (1 <= day <= 31) or not (1900 <= y <= 2100):
                            print(f"Warning: Invalid date values for {d}: year={y}, month={m}, day={day}")
                            y = m = day = None
                    else:
                        y = m = day = None
                except (ValueError, IndexError) as e:
                    print(f"Error parsing date '{d}': {e}")
                    y = m = day = None
                
                if y is not None and m is not None and day is not None:
                    inserts.append((d, y, m, day))
                else:
                    print(f"Warning: Skipping invalid date: '{d}'")

            if inserts:
                try:
                    insert_sql = """
                        INSERT INTO dim_date(date, year, month, day) 
                        VALUES (%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                            year = COALESCE(VALUES(year), year),
                            month = COALESCE(VALUES(month), month),
                            day = COALESCE(VALUES(day), day)
                    """
                    self.cursor.executemany(insert_sql, inserts)
                    self.conn.commit()
                    print(f"Inserted/updated {len(inserts)} dates")
                except Exception as e:
                    print(f"Error inserting dates: {e}")
                    print(f"Problematic inserts (first 5): {inserts[:5]}")
                    self.conn.rollback()
                    raise

            # CRITICAL: Refresh cache after insert - ensure all inserted dates are in cache
            if inserts:
                inserted_dates = [i[0] for i in inserts]
                for chunk in list(_chunked(inserted_dates, 500)):
                    try:
                        q = "SELECT date_sk, date FROM dim_date WHERE date IN ({})".format(",".join(["%s"] * len(chunk)))
                        self.cursor.execute(q, chunk)
                        results = self.cursor.fetchall()
                        cached_dates = set()
                        for sk, d in results:
                            # Cache using normalized date key
                            key = self._normalize_date_key(d)
                            self.date_cache[key] = sk
                            # Also cache using the date string directly
                            date_str = str(d)
                            self.date_cache[date_str] = sk
                            cached_dates.add(date_str)
                        
                        # Check if any dates weren't cached
                        missing_in_cache = set(chunk) - cached_dates
                        if missing_in_cache:
                            print(f"Warning: {len(missing_in_cache)} dates not found in cache after insert: {list(missing_in_cache)[:5]}")
                    except Exception as e:
                        print(f"Error refreshing date cache: {e}")
                        import traceback
                        traceback.print_exc()
                        # Don't raise - we want to continue even if cache refresh fails

# ----------------------- Stream Reader Thread -----------------------
class StreamReader(threading.Thread):
    def __init__(self, filepath: str, stream_buffer: StreamBuffer,
                 batch_read: int = 1000, initial_pause: float = 0.0):
        super().__init__(daemon=True)
        self.filepath = filepath
        self.stream_buffer = stream_buffer
        self.batch_read = batch_read
        self.initial_pause = initial_pause
        self._stop = threading.Event()

    def run(self):
        time.sleep(self.initial_pause)
        try:
            row_count = 0
            with open(self.filepath, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for r in reader:
                    row_count += 1
                    # we could normalize here if needed
                    self.stream_buffer.put(r)
                    if self._stop.is_set():
                        break
                    if len(self.stream_buffer.buffer) % self.batch_read == 0:
                        time.sleep(0.01)
            print(f'StreamReader finished. Read {row_count} rows from {self.filepath}')
        except Exception as e:
            print(f'StreamReader error reading {self.filepath}: {e}')
            import traceback
            traceback.print_exc()

    def stop(self):
        self._stop.set()

# ----------------------- Joiner Thread implementing HYBRIDJOIN -----------------------
class Joiner(threading.Thread):
    def __init__(self, stream_buffer: StreamBuffer,
                 customer_master: MasterData,
                 product_master: MasterData,
                 dw_loader: DWLoader,
                 hS: int = 10000, vP: int = 500):
        super().__init__(daemon=True)
        self.stream_buffer = stream_buffer
        self.customer_master = customer_master
        self.product_master = product_master
        self.dw_loader = dw_loader
        self.hS = hS
        self.vP = vP
        self.hash_table = HybridHashTable(hS=self.hS)
        self.queue = DoublyLinkedQueue()
        self.w = hS
        self._stop = threading.Event()
        self.total_processed = 0
        self.all_joined_rows = []  # Store all joined rows to print at the end

    def run(self):
        print('Joiner started. hS=', self.hS, 'vP=', self.vP)
        iterations_without_progress = 0
        max_stale_iterations = 1000
        try:
            while not self._stop.is_set():
                # 1) Consume from stream into hash/queue
                if self.w > 0:
                    batch = self.stream_buffer.get_batch(self.w)
                    if batch:
                        for r in batch:
                            try:
                                cust_id = r.get('Customer_ID') or r.get('customer_id')
                                prod_id = r.get('Product_ID') or r.get('product_id')
                                if not cust_id or not prod_id:
                                    print(f"Warning: Missing Customer_ID or Product_ID in row: {list(r.keys())}")
                                    continue
                                # normalize and overwrite date in row
                                raw_date = (r.get('date') or '').strip()
                                r['date'] = fix_date_format(raw_date)
                                key = f"{cust_id}|{prod_id}"
                                node = DoublyLinkedNode(key, payload=r)
                                self.queue.append(node)
                                self.hash_table.insert(key, r, node)
                                self.w -= 1
                            except KeyError as e:
                                print(f"KeyError accessing row: {e}, available keys: {list(r.keys())}")
                                continue
                            except Exception as e:
                                print(f"Error processing batch row: {e}")
                                continue
                else:
                    if self.hash_table.count == 0 and self.stream_buffer.size() == 0:
                        time.sleep(0.02)

                # 2) If nothing in queue, wait
                oldest = self.queue.head
                if not oldest:
                    time.sleep(0.02)
                    continue

                cust_id, prod_id = oldest.key.split('|')

                cust_idx = self.customer_master.find_index(cust_id)
                if cust_idx is None:
                    cust_idx = 0
                cust_partition = self.customer_master.load_partition(cust_idx, self.vP)

                prod_idx = self.product_master.find_index(prod_id)
                if prod_idx is None:
                    prod_idx = 0
                prod_partition = self.product_master.load_partition(prod_idx, self.vP)

                enriched_results = []

                # join with customer partition
                for cust_row in cust_partition:
                    key = f"{cust_row.get('Customer_ID')}|{prod_id}"
                    matches = self.hash_table.find_and_delete(key)
                    if matches:
                        for m in matches:
                            joined = dict(m['record'])
                            joined.update({k: v for k, v in cust_row.items() if k != 'Customer_ID'})
                            enriched_results.append(joined)
                            try:
                                self.queue.remove(m['node'])
                            except Exception:
                                pass
                            self.w += 1

                # join with product partition
                for prod_row in prod_partition:
                    key = f"{cust_id}|{prod_row.get('Product_ID')}"
                    matches = self.hash_table.find_and_delete(key)
                    if matches:
                        for m in matches:
                            joined = dict(m['record'])
                            joined.update({k: v for k, v in prod_row.items() if k != 'Product_ID'})
                            enriched_results.append(joined)
                            try:
                                self.queue.remove(m['node'])
                            except Exception:
                                pass
                            self.w += 1

                if enriched_results:
                    self.total_processed += len(enriched_results)
                    iterations_without_progress = 0
                    print(f"Produced {len(enriched_results)} joined rows. total_processed={self.total_processed}")
                    # Store all joined rows to print at the end
                    self.all_joined_rows.extend(enriched_results)
                    try:
                        self.dw_loader.insert_enriched(enriched_results)
                    except Exception as e:
                        print("DW insert error", e)
                        import traceback
                        traceback.print_exc()
                else:
                    iterations_without_progress += 1
                    if iterations_without_progress > max_stale_iterations and self.hash_table.count == 0:
                        print(f"No progress for {iterations_without_progress} iterations, hash table empty. Exiting joiner.")
                        break

                time.sleep(0.001)
            
            # Print all joined rows after finishing the join process
            print("\n" + "=" * 80)
            print(f"HYBRID JOIN FINISHED - PRINTING ALL {len(self.all_joined_rows)} JOINED ROWS:")
            print("=" * 80)
            for idx, row in enumerate(self.all_joined_rows, 1):
                print(f"Row {idx}: {row}")
            print("=" * 80)
            print(f"Total rows after hybrid join: {len(self.all_joined_rows)}\n")
        except Exception as e:
            print(f'Joiner error: {e}')
            import traceback
            traceback.print_exc()
            # Print collected rows even if there was an error
            if self.all_joined_rows:
                print("\n" + "=" * 80)
                print(f"PRINTING {len(self.all_joined_rows)} JOINED ROWS COLLECTED BEFORE ERROR:")
                print("=" * 80)
                for idx, row in enumerate(self.all_joined_rows, 1):
                    print(f"Row {idx}: {row}")
                print("=" * 80)

    def stop(self):
        self._stop.set()

# ----------------------- CLI & Runner -----------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stream', required=True)
    parser.add_argument('--customer', required=True)
    parser.add_argument('--product', required=True)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default=3306, type=int)
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--db', required=True)
    parser.add_argument("--hS", type=int, default=1000)
    parser.add_argument("--vP", type=int, default=4)
    args = parser.parse_args()

    if not os.path.exists(args.stream):
        raise FileNotFoundError(args.stream)
    if not os.path.exists(args.customer):
        raise FileNotFoundError(args.customer)
    if not os.path.exists(args.product):
        raise FileNotFoundError(args.product)

    stream_buffer = StreamBuffer()
    try:
        customer_master = MasterData(args.customer, 'Customer_ID')
        product_master = MasterData(args.product, 'Product_ID')
    except Exception as e:
        print(f'ERROR: Failed to load master data files: {e}')
        return

    try:
        dw_loader = DWLoader(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            db=args.db,
            batch_size=1000,
            customer_master=customer_master,
            product_master=product_master
        )
    except Exception as e:
        print(f'ERROR: Failed to initialize database loader: {e}')
        return

    reader = StreamReader(args.stream, stream_buffer, batch_read=1000)
    joiner = Joiner(stream_buffer, customer_master, product_master, dw_loader,
                    hS=args.hS, vP=args.vP)

    print('Starting StreamReader and Joiner...')
    reader.start()
    joiner.start()

    try:
        max_wait_time = 300  # Maximum 5 minutes
        wait_count = 0
        while (reader.is_alive() or stream_buffer.size() > 0 or joiner.hash_table.count > 0) and wait_count < max_wait_time:
            time.sleep(0.5)
            wait_count += 1
            if wait_count % 10 == 0:  # Print status every 5 seconds
                print(f'Status: reader_alive={reader.is_alive()}, buffer_size={stream_buffer.size()}, hash_count={joiner.hash_table.count}, processed={joiner.total_processed}')
    except KeyboardInterrupt:
        print('Interrupted. Stopping...')
    except Exception as e:
        print(f'ERROR in main loop: {e}')
        import traceback
        traceback.print_exc()
    finally:
        print('Stopping threads and flushing data...')
        reader.stop()
        joiner.stop()
        try:
            dw_loader.flush_all()
        except Exception as e:
            print(f'ERROR during final flush: {e}')
            import traceback
            traceback.print_exc()
        reader.join(timeout=5)
        joiner.join(timeout=5)
        print(f'Finished. Total processed: {joiner.total_processed}')

        # Close database connection
        try:
            dw_loader.cursor.close()
            dw_loader.conn.close()
            print('Database connection closed.')
        except Exception as e:
            print(f'Warning: Error closing database connection: {e}')

if __name__ == '__main__':
    main()
