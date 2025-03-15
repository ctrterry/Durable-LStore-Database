
#  * Program Name: Database System -> Milestone3 Update
#  * Author: Next Genrartion
#  * Date: Mar 05/2025 (Final_Version_v04)
#  * Description:
import threading
"""
The lockmanager class handles two different types of locks as well as a lock release function.
It takes the transaction's record ID and also assigns the lock an ID to track the lock type.
S is denoted as a shared lock and X is dentoed as an exclusive lock. If a lock is already associated
to the record_id we return the previous lock

Shared locks are able to be held and upgraded if a exlusive lock calls a transaction that has an exlusively 
held shared lock. However the opposite is not true. 

Release: The lock id, is found and removed along with the lock
"""
class LockManager:
    def __init__(self):
        # Using map that record_id -> (lock_type, set of txn_ids)
        self.locks = {}
        self.mutex = threading.Lock()

    # Give to the S 
    def acquire_shared(self, record_id, txn_id):
        with self.mutex:
            if record_id not in self.locks:
                self.locks[record_id] = ('S', {txn_id})
                return True
            lock_type, holders = self.locks[record_id]
            if lock_type == 'S':
                holders.add(txn_id)
                return True
            # If held exclusively by another txn, cannot acquire shared lock.
            if lock_type == 'X' and txn_id in holders:
                return True
            return False
    # Give to the X 
    def acquire_exclusive(self, record_id, txn_id):
        with self.mutex:
            if record_id not in self.locks:
                self.locks[record_id] = ('X', {txn_id})
                return True
            lock_type, holders = self.locks[record_id]
            # Already held exclusively by the same txn.
            if lock_type == 'X' and txn_id in holders:
                return True
            # Allow upgrade if the txn already holds the only shared lock.
            if lock_type == 'S' and holders == {txn_id}:
                self.locks[record_id] = ('X', {txn_id})
                return True
            return False
    # Release the lock
    def release(self, record_id, txn_id):
        with self.mutex:
            if record_id in self.locks:
                lock_type, holders = self.locks[record_id]
                if txn_id in holders:
                    holders.remove(txn_id)
                    if not holders:
                        del self.locks[record_id]
