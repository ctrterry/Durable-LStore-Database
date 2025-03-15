#  * Program Name: Database System -> Milestone3 Update
#  * Author: Next Genrartion
#  * Date: Mar 05/2025 (Final_Version_v04)
#  * Description:
from lstore.table import Table, Record
from lstore.index import Index
import threading

''' 
The Transaction Worker class that focuses on using python's threading module in order to concurrently executing 
database transactions. We use this as a helper class in order for the transaction class to be able to call in 
order to handle multithreading
'''

class TransactionWorker:
    def __init__(self, transactions=None):
        if transactions is None:
            transactions = []
        self.transactions = transactions
        self.stats = []
        self.result = 0
        self.worker_thread = None

    # adds an additional transaction
    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        # Since, need to create a dedicated worker thread that runs all assigned transactions concurrently.
        self.worker_thread = threading.Thread(target=self.__run)
        self.worker_thread.start()

    # waits for the worker thread to finish before appending the result
    def join(self):
        if self.worker_thread:
            self.worker_thread.join()

    '''
    Purpose: Spawns threads based on the needed transactions in transacption.py and then waits for threads
    to finsih before appending the results and tallying the number of successful transactions to return.

    '''

    def __run(self):
        threads = []
        results = [None] * len(self.transactions)

        def run_transaction(i, transaction):
            results[i] = transaction.run()

        # spawn a thread for each transaction.
        for i, transaction in enumerate(self.transactions):
            t = threading.Thread(target=run_transaction, args=(i, transaction))
            threads.append(t)
            t.start()

        # wait for all transaction threads and to finish.
        for t in threads:
            t.join()

        self.stats = results
        # Trying to count the number of transactions that successfully committed.
        self.result = sum(1 for r in results if r)
