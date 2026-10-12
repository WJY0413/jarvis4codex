"""Synthetic SQLite regressions for the holder's review-item count only."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sqlite3
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

from adapters.codex_app_server import task_provisioning_adapter as adapter


class SchedulerHistoryCountTests(unittest.TestCase):
    def count(self, path, hold_id='h'):
        counter = getattr(adapter, '_count_scheduler_failed_items', None)
        self.assertIsNotNone(counter, 'holder needs a payload-bounded review-item counter')
        return counter(path, hold_id=hold_id)

    def append(self, path, turn, verification=None, hold_id='h', final_answer='answer'):
        adapter.append_terminal_turn_history(path, task_id='task', hold_id=hold_id,
            request_id='request', thread_id='thread', turn_id=str(turn), turn_number=int(turn),
            candidate_id=None, candidate_ids=[7, 9], status='completed',
            final_answer=final_answer, completed_at='2026-10-03T00:00:00Z',
            output_verification=verification)

    def old_count(self, path, hold_id='h'):
        return sum(item.get('status') == 'review'
            for row in adapter.read_turn_history(path, hold_id=hold_id)
            for checked in [row.get('output_verification', {})]
            for item in checked.get('items', [checked]))

    def legacy(self, path):
        with sqlite3.connect(path) as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE turn_history (hold_id TEXT,turn_id TEXT,task_id TEXT,'
                'request_id TEXT,thread_id TEXT,turn_number INTEGER,candidate_id INTEGER,'
                'status TEXT,final_answer TEXT,has_final_answer INTEGER,completed_at TEXT,'
                'recorded_at TEXT,PRIMARY KEY(hold_id,turn_id))')
            db.execute("INSERT INTO turn_history VALUES ('h','1','t','r','thread',1,7,'failed','old',1,'a','a')")
        db.close()

    def test_missing_empty_and_filtered_history_return_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'history.sqlite'
            self.assertEqual(self.count(path), 0)
            self.assertFalse(path.exists())
            self.append(path, 1, {'status': 'review'}, hold_id='other')
            self.assertEqual(self.count(path), self.old_count(path))
            with sqlite3.connect(path) as db:
                db.execute('DELETE FROM turn_history')
            db.close()
            self.assertEqual(self.count(path), 0)

    def test_single_batch_and_empty_items_preserve_review_semantics(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'history.sqlite'
            for turn, verification in enumerate((None, {}, {'status': 'review'},
                    {'status': 'verified'}, {'status': 'blocked'},
                    {'status': 'review', 'items': []},
                    {'status': 'verified', 'items': [{'status': 'review'}, {'status': 'verified'}, {'status': 'review'}]}), 1):
                self.append(path, turn, verification)
            self.append(path, 8, {'status': 'review'}, hold_id='other')
            self.assertEqual(self.count(path), 3)
            self.assertEqual(self.count(path), self.old_count(path))
            self.assertEqual(self.count(path, ' h '), self.old_count(path, ' h '))

    def test_legacy_schema_has_zero_reviews_without_migration(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'history.sqlite'
            self.legacy(path)
            before = path.read_bytes()
            self.assertEqual(self.count(path), self.old_count(path))
            self.assertEqual(self.count(path), 0)
            self.assertEqual(path.read_bytes(), before)
            with sqlite3.connect(path) as db:
                self.assertNotIn('output_verification_json', {row[1] for row in db.execute('PRAGMA table_info(turn_history)')})
            db.close()

    def test_invalid_verification_still_fails_instead_of_reporting_zero(self):
        for raw, error in (('{broken', json.JSONDecodeError), ('null', AttributeError),
                ('[]', AttributeError), ('{"items":null}', TypeError),
                ('{"items":[null]}', AttributeError), ('{"items":["review"]}', AttributeError)):
            with self.subTest(raw=raw), tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / 'history.sqlite'
                self.append(path, 1)
                with sqlite3.connect(path) as db:
                    db.execute('UPDATE turn_history SET output_verification_json=?', (raw,))
                db.close()
                with self.assertRaises(error):
                    self.old_count(path)
                with self.assertRaises(error):
                    self.count(path)

    def test_existing_database_without_table_or_with_partial_json_schema_fails(self):
        for shape in ('missing_table', 'candidate_only', 'verification_only'):
            with self.subTest(shape=shape), tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / 'history.sqlite'
                if shape == 'missing_table':
                    with sqlite3.connect(path) as db:
                        db.execute('CREATE TABLE unrelated (value TEXT)')
                    db.close()
                    with self.assertRaises(sqlite3.OperationalError):
                        self.old_count(path)
                else:
                    self.legacy(path)
                    column = 'candidate_ids_json' if shape == 'candidate_only' else 'output_verification_json'
                    with sqlite3.connect(path) as db:
                        db.execute('ALTER TABLE turn_history ADD COLUMN ' + column + ' TEXT')
                        db.execute('UPDATE turn_history SET ' + column + '=?',
                            ('[7]' if shape == 'candidate_only' else '{"status":"review"}',))
                    db.close()
                    if shape == 'candidate_only':
                        with self.assertRaises(sqlite3.OperationalError):
                            self.old_count(path)
                    else:
                        # Legacy reader ignores the lone verification column. The
                        # new counter conservatively rejects this incomplete schema.
                        self.assertEqual(self.old_count(path), 0)
                before = path.read_bytes()
                with self.assertRaises(sqlite3.OperationalError):
                    self.count(path)
                self.assertEqual(path.read_bytes(), before)

    def test_count_streams_one_needed_column_without_payload_or_fetchall(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'history.sqlite'
            verification = {'status': 'verified', 'items': [{'status': 'review'}, {'status': 'verified'}]}
            for turn in range(1, 41):
                self.append(path, turn, verification, final_answer='x' * 131072)
            tracemalloc.start()
            old_rows = adapter.read_turn_history(path, hold_id='h')
            _, old_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            old_bytes = sum(len(row['final_answer']) for row in old_rows)
            old_columns = len(old_rows[0])
            del old_rows
            connect, loads = sqlite3.connect, json.loads
            selected = []

            class Cursor:
                def __init__(self, cursor):
                    self.cursor = cursor
                def __iter__(self):
                    for row in self.cursor:
                        if self.cursor.description == (('output_verification_json', None, None, None, None, None, None),):
                            selected.append((len(row), len(row[0])))
                        yield row
                def fetchall(self):
                    raise AssertionError('history count must stream; fetchall forbidden')

            class Reader:
                def __init__(self, conn):
                    self.conn = conn
                def execute(self, sql, *args):
                    if sql.startswith('SELECT'):
                        test.assertNotIn('final_answer', sql)
                        test.assertNotIn('candidate_ids', sql)
                        test.assertNotIn('SELECT *', sql)
                    return Cursor(self.conn.execute(sql, *args))
                def close(self):
                    self.conn.close()

            def parse(raw):
                self.assertEqual(raw, json.dumps(verification, ensure_ascii=False))
                return loads(raw)
            test = self
            with patch.object(adapter.sqlite3, 'connect', side_effect=lambda *a, **kw: Reader(connect(*a, **kw))), \
                    patch.object(adapter.json, 'loads', side_effect=parse):
                tracemalloc.start()
                try:
                    self.assertEqual(self.count(path), 40)
                    _, new_peak = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
            self.assertEqual(len(selected), 40)
            self.assertTrue(all(columns == 1 for columns, _ in selected))
            new_bytes = sum(size for _, size in selected)
            self.assertLess(new_bytes, old_bytes // 100)
            print('SYNTHETIC_COUNT_QUERY ' + json.dumps({'rows': 40, 'old_columns': old_columns,
                'new_columns': 1, 'old_final_answer_bytes': old_bytes, 'new_verification_bytes': new_bytes,
                'old_python_peak_bytes': old_peak, 'new_python_peak_bytes': new_peak}))

    def test_count_snapshot_survives_concurrent_schema_migration_and_append(self):
        for legacy in (True, False):
            with self.subTest(legacy=legacy), tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / 'history.sqlite'
                if legacy:
                    self.legacy(path)
                else:
                    self.append(path, 1, {'status': 'review'})
                    with sqlite3.connect(path) as db:
                        db.execute('PRAGMA journal_mode=WAL')
                    db.close()
                connect = sqlite3.connect
                test = self
                injected = False
                class Reader:
                    def __init__(self, conn):
                        self.conn = conn
                    def execute(self, sql, *args):
                        nonlocal injected
                        cursor = self.conn.execute(sql, *args)
                        if sql == 'PRAGMA table_info(turn_history)' and not injected:
                            columns = list(cursor)
                            injected = True
                            test.append(path, 2, {'status': 'review'})
                            return columns
                        return cursor
                    def close(self):
                        self.conn.close()
                def reader(*args, **kwargs):
                    conn = connect(*args, **kwargs)
                    return Reader(conn) if kwargs.get('uri') else conn
                with patch.object(adapter.sqlite3, 'connect', side_effect=reader):
                    self.assertEqual(self.count(path), 0 if legacy else 1)
                self.assertTrue(injected)
                self.assertEqual(self.count(path), 1 if legacy else 2)

    def test_twenty_concurrent_writers_are_visible_and_duplicate_turn_is_not_recounted(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'history.sqlite'
            with ThreadPoolExecutor(max_workers=20) as pool:
                list(pool.map(lambda turn: self.append(path, turn, {'status': 'review'}), range(1, 21)))
            self.append(path, 1, {'status': 'review'})
            self.assertEqual(self.count(path), 20)
            self.assertEqual(self.count(path), self.old_count(path))

    def test_holder_review_count_does_not_call_full_payload_history_reader(self):
        from adapters.codex_app_server import jarvis_task_hold_host as holder
        from test_jarvis_task_hold_host import ReliabilityRegressionTest
        with patch.object(holder, 'read_turn_history', create=True,
                side_effect=AssertionError('holder count may not materialize full history')):
            case = ReliabilityRegressionTest(
                'test_review_is_durable_before_next_turn_and_does_not_leak_into_unknown_turn')
            result = unittest.TestResult()
            case.run(result)
        self.assertEqual(result.failures, [])
        self.assertEqual(result.errors, [])


if __name__ == '__main__':
    unittest.main()
