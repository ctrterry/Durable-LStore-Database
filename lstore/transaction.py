#  * Program Name: Database System -> Milestone3 Update
#  * Author: Next Genrartion
#  * Date: Mar 05/2025 (Final_Version_v04)
#  * Description:

from lstore.table import Table, Record
from lstore.index import Index
import threading
from time import sleep
from lstore.lock_manager import LockManager

'''
Transactions take place in this file where it handles concurrent execution using Strict 2PL and 
handles conflicts using retry methods. 
'''

# Global lock manager instance (could also be made a singleton)
lock_manager = LockManager()

# Global transaction id generator
txn_id_counter = 0
txn_id_lock = threading.Lock()
class Transaction:
    def __init__(self):
        global txn_id_counter
        with txn_id_lock:
            self.txn_id = txn_id_counter
            txn_id_counter += 1
        self.queries = []           # List of (query_function, table, args)
        self.acquired_locks = []    # List of (record_id, lock_type) held by this txn
        self.max_retries = 5

    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

    """
    Run completes many different tasks to ensure read and write operations are correct.

    Retrying: The transaction is retired up to 5 times after failed attempts. This mechanism helps 
    ensure that transactions are correctly handles

    Lock Managers: The transaction is sorted into which locking mechanism to follow. lock_manager.py utilizes
    shared and exclusive locks based of what each transaction needs. If we are uncertain of a transaction type
    we use an exlusive lock to ensure the transactions authenticity. This helps prevent overwriting uncommitied
    data, dirty reads, etc.

    Lock Releases: Locks are released before execution to make sure that there is no transaction stuck in deadlock.

    """
    def run(self):
        retries = 0
        while retries < self.max_retries:
            self._release_all_locks()  # Ensure no locks linger from previous attempts.
            success = True
            for query, table, args in self.queries:
                # For our purposes, we assume the first argument is the primary key.
                key = args[0]
                # Determine the required lock type based on the operation.
                # (Assumes: insert, update, delete require exclusive; select, select_version, sum, sum_version require shared.)
                if query.__name__ in ['insert', 'update', 'delete']:
                    if not lock_manager.acquire_exclusive(key, self.txn_id):
                        success = False
                        break
                    self.acquired_locks.append((key, 'X'))
                elif query.__name__ in ['select', 'select_version', 'sum', 'sum_version']:
                    if not lock_manager.acquire_shared(key, self.txn_id):
                        success = False
                        break
                    self.acquired_locks.append((key, 'S'))
                else:
                    # Default to exclusive if uncertain.
                    if not lock_manager.acquire_exclusive(key, self.txn_id):
                        success = False
                        break
                    self.acquired_locks.append((key, 'X'))
                # Execute the query operation.
                result = query(*args)
                if result is False:
                    success = False
                    break
            if success:
                self.commit()
                return True
            else:
                self.abort()
                retries += 1
                sleep(0.1)  # Backoff delay before retry.
        return False


    """
    We ensure to release all locks before aborting and commiting to follow 2PL. Allows other threads to
    be able to access these affected records
    """
    def abort(self):
        # For an aborted transaction, any base/tail records created can be marked as deleted.
        # (Implement additional rollback logic as needed.)
        self._release_all_locks()
        return False

    def commit(self):
        # On commit, release all locks.
        self._release_all_locks()
        return True

    def _release_all_locks(self):
        for record_id, _ in self.acquired_locks:
            lock_manager.release(record_id, self.txn_id)
        self.acquired_locks = []
