#  * Program Name: Database System -> L_Store Concepts
#  * Author: Next Generation
#  * Date: Feb 24/2025 (Final_Version_v04)

#  * Description:
#       The milestone2 biggest update is all functions are accosiate with DiskManger File
#   Such as:
#        @ delete() function, if a record is not found into the memory? and then retrieved from disk
#        @ select() function, we will fatches the latest version using the tail records
#        @ delete() function, if a record is not found into the memory? and then retrieved from disk

from lstore.table import Table, Record
from time import time

class Query:

    """
        Creates a Query object that can perform different queries on the specified table.
        Queries that fail must return False.
        Queries that succeed should return the result or True.
        Any query that crashes (due to exceptions) should return False.
    """
    def __init__(self, table):
        self.table = table
        pass

    #  * Function Name: Delete
    #  * Purpose: Allow to remove a record from the table
    #  * Parameter: primary_key
    #  * Return: Bool
    def delete(self, primary_key):
        if primary_key not in self.table.key_directory:
            return False
        rid = self.table.key_directory[primary_key]
        record = self.table.page_directory.get(rid)
        # Milestone2 Update: Modify the delete, if the record is not found it, goto disk trying to find it
        if record is None and self.table.disk_manager:
            record = self.table.disk_manager.get_pages_from_disk(self.table.name, rid)
            if record:
                self.table.page_directory[rid] = record

        if record is None or record.get('deleted', False):
            return False
        record['deleted'] = True
        # Milestone2 Update: delete the data from the disk
        if self.table.disk_manager:
            self.table.disk_manager.save_page_from_disk(self.table.name, rid, record)
        return True
    
    #  * Function Name: insert
    #  * Purpose: Trying to inserts a new record into the table
    #  * Parameter: requirt (*columns), a tulple which is representing the values about the insert
    #  * Return: Bool, true -> insertion successful. Otherwise, false
    def insert(self, *columns):
        primary_key = columns[self.table.key]
        if primary_key in self.table.key_directory:
            return False

        rid = self.table.next_rid
        self.table.next_rid += 1
        record = {
            'rid': rid,
            'key': primary_key,
            'indirection': None,
            'data': list(columns),
            'deleted': False
        }
        self.table.page_directory[rid] = record
        self.table.key_directory[primary_key] = rid

        if self.table.index.indices[self.table.key] is not None:
            self.table.index.indices[self.table.key][primary_key] = rid
        return True


    #  * Function Name: get_last_version_value
    #  * Purpose: Trying to retrieves the recently values by the specified column into the record, based on the tail record
    #  * Parameters:
    #  *     @ record: The base record from which to retrieve the latest column value.
    #  *     @ column: The index of the column whose latest value is needed.
    #  * Return:
    #  *     @ Any: The most recent value of the specified column, retrieved from
    #  *            either the base record or the latest tail record.

    def get_last_version_value(self, record, column):
        current_rid = record['indirection']
        while current_rid is not None:
            tail_record = self.table.page_directory.get(current_rid)
            if tail_record is None:
                break
            if tail_record['data'][column] is not None:
                return tail_record['data'][column]
            current_rid = tail_record['indirection']
        return record['data'][column]
    
    # Milestone2 Update functions
    # This get merged record will
    #  * Function Name: get_merged_record(self, base_record):
    #  * Purpose: get the most recent value's record from tail pages. 
    #  * Parameters:
    #  *     @ base_record: The base record from which to retrieve the latest column valus.
    #  * Return:
    #  *     @ list: this list will containing the most of the up to date valus for each column
    #  *             and which will derived from the base record and it's assocaiated with tail records. 
    def get_merged_record(self, base_record):
        merged_data = list(base_record['data'])
        current_rid = base_record.get('indirection')
        while current_rid is not None: # Iterator the indirection chain 
            tail_record = self.table.page_directory.get(current_rid) # Retrived the last tail Record
            if tail_record is None: # if the tail record is not found it, then break 
                break
            # For each column, if this tail record provides an update, use it.
            for i in range(self.table.num_columns):
                if tail_record['data'][i] is not None:
                    merged_data[i] = tail_record['data'][i]
            current_rid = tail_record.get('indirection')    # Move to the another tail record
        return merged_data



    #  * Function Name: select
    #  * Purpose: Retrieves a record based on a given key and projects selected columns.
    #  * Parameters:
    #  *     @ search_key: The value to search for.
    #  *     @ search_key_index: The index of the column used for searching.
    #  *     @ projected_columns_index: A list indicating which columns to return.
    #  * Return:
    #  *     @ list[Record]: A list of selected records if found.
    #  *     @ bool: Will returns false if no matching record exists.

    # Finished update about Milestone2 -> Feb 21 
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            if search_key_index == self.table.key:
                if search_key not in self.table.key_directory:
                    return False
                rid = self.table.key_directory[search_key]
                base_record = self.table.page_directory.get(rid)
                if base_record is None and self.table.disk_manager:
                    base_record = self.table.disk_manager.get_pages_from_disk(self.table.name, rid)
                    if base_record:
                        self.table.page_directory[rid] = base_record
                if base_record is None or base_record.get('deleted', False):
                    return False

                # Update:
                # Instead of calling get_last_version_value per column,
                # merge the tail chain
                merged_data = self.get_merged_record(base_record)
                result_values = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i]:
                        val = self.get_last_version_value(base_record, i)
                        result_values.append(val)
                    else:
                        result_values.append(None)
                return [Record(base_record['rid'], base_record['key'], result_values)]
            else:
                # Trying to handle the non-primary key search similarly.
                for rid, record in self.table.page_directory.items():
                    if (not record.get('deleted', False) and record['data'][search_key_index] == search_key):
                        merged_data = self.get_merged_record(record)
                        result_values = []
                        for i in range(self.table.num_columns):
                            if projected_columns_index[i]:
                                result_values.append(merged_data[i])
                            else:
                                result_values.append(None)
                        return [Record(record['rid'], record['key'], result_values)]
                return False
        except Exception as selectError:
            print(f"Selection error: {selectError}")
            return False


    #  * Function Name: select_version
    #  * Purpose: Trying to retrievees a historical version, based on the relative updates
    #  * Parameters: @1th_para: search_key, the value to search for.
    #  *             @2th_para: search_key_index (int):  index of the column used for searching.
    #  *             @3th_para projected_columns_index : A list indicating which columns to return.
    #  *             @4th_para relative_version : The version offset (0 = latest, 1 = previous, etc.).
    #  * Return List:
    #  *             if successful:
    #  *             return list[Record]: The list of selected records from the specified version.
    #  *             bool: Returns False if no matching record exists.
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            if search_key_index == self.table.key:
                if search_key not in self.table.key_directory:
                    return False
                rid = self.table.key_directory[search_key]
                base_record = self.table.page_directory.get(rid)
                if base_record is None or base_record.get('deleted', False):
                    return False
                # Build tail chain: list of tail records from newest to oldest.
                tail_chain = []
                current_rid = base_record.get('indirection')
                while current_rid is not None:
                    tail_record = self.table.page_directory.get(current_rid)
                    if tail_record is None:
                        break
                    tail_chain.append(tail_record)
                    current_rid = tail_record.get('indirection')
                # If relative_version is negative, skip the last tail updates.
                if relative_version < 0:
                    k = -relative_version        #  -relative_version tail updates
                    effective_chain = tail_chain[k:] if k < len(tail_chain) else []
                else:
                    effective_chain = tail_chain[:]  # Use all tail records, since the relative_version 0.
                # Now, trying to apply the effective chain into the reverse order 
                version_data = list(base_record['data'])    # The (oldest first) onto the base data.
                for tail_record in reversed(effective_chain):
                    for i in range(self.table.num_columns):
                        if tail_record['data'][i] is not None:
                            version_data[i] = tail_record['data'][i]
                result_values = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i]:
                        result_values.append(version_data[i])
                    else:
                        result_values.append(None)
                return [Record(base_record['rid'], base_record['key'], result_values)]
            else:
                # Else, about the non-primary key search area
                for rid, record in self.table.page_directory.items():
                    if (not record.get('deleted', False) and record['data'][search_key_index] == search_key):
                        tail_chain = []
                        current_rid = record.get('indirection')
                        while current_rid is not None:
                            tail_record = self.table.page_directory.get(current_rid)
                            if tail_record is None:
                                break
                            tail_chain.append(tail_record)
                            current_rid = tail_record.get('indirection')
                        if relative_version < 0:
                            k = -relative_version
                            effective_chain = tail_chain[k:] if k < len(tail_chain) else []
                        else:
                            effective_chain = tail_chain[:]
                        version_data = list(record['data'])
                        for tail_record in reversed(effective_chain):
                            for i in range(self.table.num_columns):
                                if tail_record['data'][i] is not None:
                                    version_data[i] = tail_record['data'][i]
                        result_values = []
                        for i in range(self.table.num_columns):
                            if projected_columns_index[i]:
                                result_values.append(version_data[i])
                            else:
                                result_values.append(None)
                        return [Record(record['rid'], record['key'], result_values)]
                return False
        except Exception as e:
            print(f"select_version error: {e}")
            return False

    #  * Function Name: update
    #  * Purpose: Updates the existing record by the creating a new tail record with modifications.
    #  * Parameters:
    #  *         @ rimary_key (int/str): The unique identifier of the record to update.
    #  *         @ columns: A variable-length tuple of updated values (None means no change).
    #  * Return:
    #  *       @bool: Returns True if the update is successful,
    #  *             False if the record does not exist or is deleted.
    def update(self, primary_key, *columns):
        if primary_key not in self.table.key_directory:
            return False
        base_rid = self.table.key_directory[primary_key]
        base_record = self.table.page_directory.get(base_rid)
        if base_record is None and self.table.disk_manager:
            base_record = self.table.disk_manager.get_pages_from_disk(self.table.name, base_rid)
            if base_record:
                self.table.page_directory[base_rid] = base_record
        if base_record is None or base_record.get('deleted', False):
            return False
        new_rid = self.table.next_rid
        self.table.next_rid += 1
        # schema_encoding = ''.join(['1' if col is not None else '0' for col in columns])
        tail_record = {
            'rid': new_rid,
            'key': primary_key,
            'indirection': base_record['indirection'],
            # 'timestamp': time(),
            # 'schema_encoding': schema_encoding,
            'data': list(columns),
            'deleted': False
        }
        self.table.page_directory[new_rid] = tail_record
        base_record['indirection'] = new_rid
        if self.table.disk_manager:
            self.table.disk_manager.save_page_from_disk(self.table.name, new_rid, tail_record)
        return True


    #  * Function Name: sum
    #  * Purpose: Computes the sum of values within a given range in a specified column.
    #  * Parameters:
    #  *     @ start_range : The starting key of the range.
    #  *     @ end_range: The ending key of the range.
    #  *     @ aggregate_column_index: The index of the column to sum.
    #  * Return:
    #  *     @ int or float: The computed sum if records are found.
    #  *     @ bool: Returns false, if there is no valid records exist in the range.

    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            total = 0
            found = False
            for key, rid in self.table.key_directory.items():
                if start_range <= key <= end_range:
                    record = self.table.page_directory.get(rid)
                    if record is None or record.get('deleted', False):
                        continue
                    val = self.get_last_version_value(record, aggregate_column_index)
                    if val is not None:
                        total += val
                        found = True
            return total if found else False
        except Exception:
            return False


    #  * Function Name: sum_version
    #  * Purpose: Computes the sum of a column across a historical version of records.
    #  * Parameters:
    #  *     @ start_range : The starting key of the range.
    #  *     @ end_range : The ending key of the range.
    #  *     @ aggregate_column_index: The index of the column to sum.
    #  *     @ relative_version : The version offset, like 0 = latest, 1 = previous, and etc.
    #  * Return:
    #  *     @ int/float: The computed sum if records are found.
    #  *     @ bool: Returns False if no valid records exist in the range.
    
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            total = 0
            found = False
            for key, rid in self.table.key_directory.items():
                if start_range <= key <= end_range:
                    record = self.table.page_directory.get(rid)
                    if record is None or record.get('deleted', False):
                        continue
                    tail_chain = []
                    current_rid = record.get('indirection')
                    while current_rid is not None:
                        tail_record = self.table.page_directory.get(current_rid)
                        if tail_record is None:
                            break
                        tail_chain.append(tail_record)
                        current_rid = tail_record.get('indirection')
                    if relative_version < 0:
                        k = -relative_version
                        effective_chain = tail_chain[k:] if k < len(tail_chain) else []
                    else:
                        effective_chain = tail_chain[:]  # for relative_version 0
                    version_data = list(record['data'])
                    for tail_record in reversed(effective_chain):
                        for i in range(self.table.num_columns):
                            if tail_record['data'][i] is not None:
                                version_data[i] = tail_record['data'][i]
                    val = version_data[aggregate_column_index]
                    if val is not None:
                        total += val
                        found = True
            return total if found else False
        except Exception as e:
            print(f"sum_version error: {e}")
            return False

    #  * Function Name: increment
    #  * Purpose: Increments a specific column of a record by 1.
    #  * Parameters:
    #  *     @ key: The primary key of the record.
    #  *     @ column: The index of the column to increment.
    #  * Return:
    #  *     @ bool: Returns True if the increment is successful,
    #  *             False if the record does not exist.
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)
        if r is not False:
            base_rid = self.table.key_directory.get(key)
            if base_rid is None:
                return False
            base_record = self.table.page_directory.get(base_rid)
            if base_record is None:
                return False
            current_val = self.get_last_version_value(base_record, column)
            if current_val is None:
                return False
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = current_val + 1
            return self.update(key, *updated_columns)
        return False
