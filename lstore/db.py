
#  * Program Name: Database System -> Milestone2 
#  * Author: Next Genrartion
#  * Date: Feb 23/2025 (Final_Version_v04)
#  * Description:

import os
from lstore.table import Table
from lstore.disk import DiskManager
from lstore.bufferPool import BufferPool

class Database:
    def __init__(self):
        self.tables = {}
        self.disk_manager = None
        self.buffer_pool = None

    def open(self, path):
        self.disk_manager = DiskManager(path)
        TEMP_CAPACITY = 100         # Inital the buffer pool capacity is 100, can be adjust as later
        self.buffer_pool = BufferPool(capacity=TEMP_CAPACITY, disk_manager=self.disk_manager)
        for filename in os.listdir(path):
            if filename.endswith("_metadata.mpack"):
                table_name = filename.replace("_metadata.mpack", "")
                table_metadata = self.disk_manager.get_table_of_metadata_from_disk(table_name)
                if table_metadata:
                    table = Table(
                        table_metadata["name"],
                        table_metadata["num_columns"],
                        table_metadata["key_index"]
                    )
                    table.next_rid = table_metadata["next_rid"]
                    table.page_directory = table_metadata["page_directory"]
                    # Set disk manager and buffer pool for the table
                    table.key_directory = table_metadata.get("key_directory", {})
                    if not table.key_directory:
                        new_key_directory = {}
                        for rid, record in table.page_directory.items():
                            key_val = record['key']
                            if key_val not in new_key_directory or rid < new_key_directory[key_val]:
                                new_key_directory[key_val] = rid
                        table.key_directory = new_key_directory
                    # Otherwise, Trying to save the data
                    table.disk_manager = self.disk_manager
                    table.buffer_pool = self.buffer_pool
                    self.tables[table_name] = table


    def close(self):
        for table in self.tables.values():
            # When called the close, need merge the tail to the base record
            # This cause the error -> Need asking professor (Tianren chen)
            # table.merge() # -> logical error 

            # Save the metadata to the disk
            self.disk_manager.save_table_of_metadata_from_disk(table)
            for page_id, page in table.page_directory.items():   # Iterator all table's meta-data
                self.disk_manager.save_page_from_disk(table.name, page_id, page)
        if self.buffer_pool:
            self.buffer_pool.flush_all() # Flush all pages to disks

    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        table.disk_manager = self.disk_manager
        table.buffer_pool = self.buffer_pool
        self.tables[name] = table
        return table

    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]

    def get_table(self, name):
        return self.tables.get(name, None) 
