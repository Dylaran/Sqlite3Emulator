"""
Name: Don Nakashima

Comments:
Added the following functionality:
UPDATE SET DELETE
WHERE DISTINCT
LEFT OUTER JOIN ON

Sources:
Professor Dennis Phillips solution code as a base shell
"""
import string
from operator import itemgetter

_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            
            while True:
                column_name = tokens.pop(0)
                
                if "." not in column_name:
                    column_name = table_name + '.' + column_name
                
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(tokens, "INSERT") # INSERT
            pop_and_check(tokens, "INTO") # INTO
            table_name = tokens.pop(0) # table
            
            
            # No specified columns
            if tokens[0] == "VALUES":
                tokens.pop(0) # VALUES
                
                while True:
                    pop_and_check(tokens, "(")
                    row_contents = []
                    
                    while True:
                        item = tokens.pop(0)
                        row_contents.append(item)
                        comma_or_close = tokens.pop(0)
                        if comma_or_close == ")":
                            break
                        assert comma_or_close == ','
                        
                    self.database.insert_into(table_name, row_contents)
                    
                    if tokens == []:
                        break
                    else:
                        pop_and_check(tokens, ',')
            # Specified columns
            else:
                pop_and_check(tokens, '(')
                
                specified_columns = []
                while True:
                    item = tokens.pop(0)
                    item = table_name + '.' + item
                    specified_columns.append(item)
                    
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
                    
                table = self.database.get_table(table_name)
                
                pop_and_check(tokens, "VALUES")
                while True:
                    pop_and_check(tokens, '(')
                    row_contents = []
                    
                    while True:
                        item = tokens.pop(0)
                        row_contents.append(item)
                        comma_or_close = tokens.pop(0)
                        if comma_or_close == ")":
                            break
                        assert comma_or_close == ','
                    
                    row = []
                    for i in table.get_columns():
                        pos = 0
                        
                        for col in specified_columns:
                            if i == col:
                                row.append(row_contents[pos])
                            else:
                                pos += 1
                                if pos == len(specified_columns):
                                    row.append(None)
                                    
                    self.database.insert_into(table_name, row)
                    
                    if tokens == []:
                        break
                    else:
                        pop_and_check(tokens, ',')
       
            


        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            # SELECT
            pop_and_check(tokens, "SELECT")
            output_columns = []
            
            # SELECT table.*
            if tokens[0][-1] == '.' and tokens[1] == '*':
                tokens.pop(0) # table.*
                
            is_distinct = False
            # SELECT DISTINCT
            if tokens[0] == "DISTINCT":
                tokens.pop(0) # DISTINCT
                is_distinct = True
            
            while True:
                col = tokens.pop(0)
                output_columns.append(col)
                comma_or_from = tokens.pop(0)
                
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','
            table_name = tokens.pop(0)
            
            # Add table name to each output column
            output_table_columns = []
            for i in output_columns:
                if i != "*" and "." not in i:
                    table_name_col = table_name + '.' + i
                    output_table_columns.append(table_name_col)
                else:
                    output_table_columns.append(i)
                    
            output_columns = output_table_columns
            
            # OPTIONAL LEFT OUTER JOIN
            left_outer_join = False
            
            if tokens[0] == "LEFT":
                # EX: SELECT "columns" FROM "table_a" LEFT OUTER JOIN "table_b" ON "column from table_a" = "column from table_b" ORDER BY "columns";
                joined_table_tuples = []
                
                left_table = self.database.get_table(table_name) # Left table
                left_table_rows = left_table.get_rows() # Rows
                left_tuple_pairs = left_table.get_tuple_pairs() # Column definitions
                
                # print("left_table_rows: ", left_table_rows)
                # print("left_tuple_pairs: ", left_tuple_pairs)
                
                pop_and_check(tokens, "LEFT") # LEFT
                pop_and_check(tokens, "OUTER") # OUTER
                pop_and_check(tokens, "JOIN") # JOIN
                
                right_name = tokens.pop(0)
                right_table = self.database.get_table(right_name) # Right table
                right_table_rows = right_table.get_rows() # Rows
                right_tuple_pairs = right_table.get_tuple_pairs() # Column definitions
                
                # print("right_table_rows: ", right_table_rows)
                # print("right_tuple_pairs: ", right_tuple_pairs)
                
                for tuple_pair in left_tuple_pairs:
                    joined_table_tuples.append(tuple_pair)
                    
                for tuple_pair in right_tuple_pairs:
                    joined_table_tuples.append(tuple_pair)
                    
                # print("joined_table_tuples: ", joined_table_tuples)
                
                joined_table_name = "joined_table"
                self.database.create_new_table(joined_table_name, joined_table_tuples)
                joined_table = self.database.get_table(joined_table_name)
                
                pop_and_check(tokens, "ON") # ON
                on_left = tokens.pop(0) # Column from table_a
                if '.' not in on_left:
                    on_left = table_name + '.' + on_left
                    
                pop_and_check(tokens, '=') # =
                on_right = tokens.pop(0) # Column from table_b
                if '.' not in on_right:
                    on_right = right_name + '.' + on_right
                    
                right_nulls = {}
                for i in right_table.get_columns():
                    right_nulls[i] = None
                    
                for left_row in left_table_rows:
                    joined_row = [] # create a new row for each left join
                    
                    for key, value in left_row.items():
                        if key == on_left: # column from table_a matches the left condition of on
                            check = False
                            
                            for right_row in right_table_rows:
                                for k, v in right_row.items():
                                    if k == on_right: # column from table_b matches the right side of on
                                    
                                        if v is None and value is None: # If both values compared NULL
                                            check = False
                                            
                                        elif v == value:
                                            check = True
                                            temp = {**left_row, **right_row} # left row + right row
                                            for x, y in temp.items():
                                                joined_row.append(y) # append the values of the joined rows
                            
                            if check == False:
                                temp = {**left_row, **right_nulls} # left row + right row nulls
                                for x, y in temp.items():
                                    joined_row.append(y) # append the values of the joined rows
                
                    self.database.insert_into(joined_table_name, joined_row)
                    
                left_outer_join = True
                
            
            # OPTIONAL WHERE clause
            where_clause = []
            
            if tokens[0] == "WHERE":
                tokens.pop(0) # WHERE
                where_name = tokens.pop(0) # column name
                
                if '.' not in where_name:
                    where_name = table_name + '.' + where_name
                    
                where_clause.append(where_name)
                
                while tokens[0] != "ORDER" and tokens[1] != "BY":
                    token = tokens.pop(0)
                    where_clause.append(token)
                    
            
            # ORDER BY
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            
            while True:
                col = tokens.pop(0)
                order_by_columns.append(col)
                if not tokens:
                    break
                pop_and_check(tokens, ",")
                
            order_by_table_columns = []
            for i in order_by_columns:
                if "." not in i:
                    table_name_col = table_name + '.' + i
                    order_by_table_columns.append(table_name_col)
                else:
                    order_by_table_columns.append(i)
                    
            order_by_columns = order_by_table_columns
                
                
            if left_outer_join:
                joined_select = self.database.select(output_columns, joined_table_name, order_by_columns, where_clause, is_distinct)
                joined_table = self.database.get_table(joined_table_name)
                self.database.drop_table(joined_table_name)
                
                return joined_select
                
            return self.database.select(
                output_columns, table_name, order_by_columns, where_clause, is_distinct)

        def update(tokens):
            """
            Update specified table
            
            Ex: UPDATE table_name SET col1 = value1, col2 = value2;
                UPDATE student SET grades=4.0 WHERE name = 'Josh';
            """
            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0) # table name
            pop_and_check(tokens, "SET")
            
            update_clause = []
            where_clause = []
            
            # SET clause
            while True:
                # Conditions
                if tokens == []:
                    break
                
                if tokens[0] == "WHERE":
                    break
                
                elif (tokens[0] == ","):
                    tokens.pop(0) # ,
                
                update_name = tokens.pop(0) # col name
                
                if '.' not in update_name:
                    update_name = table_name + '.' + update_name
                    
                update_clause.append(update_name)
                pop_and_check(tokens, '=') # =
                
                if tokens[0] == "'":
                    tokens.pop("'") # '
                    value = tokens.pop(0) # value
                    tokens.pop ("'") # '
                else:
                    value = tokens.pop(0)
                
                update_clause.append(value)
            
            # WHERE clause
            if tokens != []:
                if tokens[0] == "WHERE":
                    tokens.pop(0) # WHERE
                    where_name = tokens.pop(0) # col name
                    
                    if '.' not in where_name:
                        where_name = table_name + '.' + where_name
                        
                    where_clause.append(where_name)
                    
                    while tokens != []:
                        token = tokens.pop(0)
                        where_clause.append(token)
                    
            self.database.update_table(table_name, update_clause, where_clause)
                
    
        def delete(tokens):
            """
            Delete from table
            
            Ex: DELETE FROM students;
                DELETE FROM students WHERE id > 4;
            """
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            
            table_name = tokens.pop(0)
            
            where_clause = []
            
            if tokens != []:
                pop_and_check(tokens, "WHERE") # make sure WHERE
                where_name = tokens.pop(0) # name
                
                if '.' not in where_name:
                    where_name = table_name + '.' + where_name
                    
                where_clause.append(where_name)
                
                while tokens != []:
                    token = tokens.pop(0)
                    where_clause.append(token)
            
            self.database.delete_from(table_name, where_clause)
        
        


        # Token statements
        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "UPDATE", "DELETE"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        else:  # tokens[0] == "SELECT"
            return select(tokens)
        assert not tokens

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def insert_into(self, table_name, row_contents):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents)
        return []

    def select(self, output_columns, table_name, order_by_columns, where_clause, is_distinct):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns, where_clause, is_distinct)
        
    def delete_from(self, table_name, where_clause):
        assert table_name in self.tables
        self.tables[table_name].delete_from_table(where_clause)
        return []
        
    def update_table(self, table_name, update_clause, where_clause):
        assert table_name in self.tables
        self.tables[table_name].update_table(update_clause, where_clause)
        return []
        
    def drop_table(self, table_name):
        assert table_name in self.tables
        self.tables.pop(table_name)
        
    def get_table(self, table_name):
        return self.tables[table_name]


class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []
        self.column_name_type_pairs = column_name_type_pairs
        
    def get_rows(self):
        return self.rows
        
    def get_columns(self):
        return self.column_names
        
    def get_tuple_pairs(self):
        return self.column_name_type_pairs

    def insert_new_row(self, row_contents):
        assert len(self.column_names) == len(row_contents)
        row = dict(zip(self.column_names, row_contents))
        self.rows.append(row)

    def select_rows(self, output_columns, order_by_columns, where_clause, is_distinct):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns):
            return sorted(self.rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)
                
        def check_where(sorted_rows, where_clause):
            if where_clause != []:
                left = where_clause.pop(0) # name
                
                compare = where_clause.pop(0) # >, <, =, !=, IS NOT, IS
                optional_compare = ""
                
                if compare == '!':
                    if where_clause[0] == '=':
                        optional_compare = where_clause.pop(0)
                        
                if compare == "IS":
                    if where_clause[0] == "NOT":
                        optional_compare = where_clause.pop(0)
                        
                right = where_clause.pop(0)  # value
                
                where_rows = []
                
                for row in sorted_rows:
                    for key, value in row.items():
                        if key == left:
                            # >, <, =, !=, IS NOT, IS
                            
                            if compare in "><=!":
                                # Can't compare NULL to a non-NULL
                                if (value is None and right is not None) or (value is not None and right is None):
                                    break
                                else:
                                    if compare == '>':
                                        if value > right:
                                            where_rows.append(row)
                                            
                                    elif compare == '<':
                                        if value < right:
                                            where_rows.append(row)
                                            
                                    elif compare == '=':
                                        if value == right:
                                            where_rows.append(row)
                                    
                                    elif compare == '!' and optional_compare == '=':
                                        if value != right:
                                            where_rows.append(row)
                                    
                            elif compare == "IS" and optional_compare == "NOT":
                                if value is not right:
                                    where_rows.append(row)
                                    
                            elif compare == "IS":
                                if value is right:
                                    where_rows.append(row)
                
                return where_rows
            else:
                return sorted_rows
                
        def check_distinct(sorted_rows, is_distinct):
            if is_distinct:
                temp = []
                rows = sorted_rows
                    
                for row in rows:
                    for col in output_columns:
                        for key, value in row.items():
                            if col == key:
                                distinct_row = True
                                for t in temp:
                                    for k, v in t.items():
                                        if col == k:
                                            if value == v:
                                                distinct_row = False
                            
                    if distinct_row:
                        temp.append(row)
                
                return temp
            else:
                return sorted_rows

        expanded_output_columns = expand_star_column(output_columns)
        check_columns_exist(expanded_output_columns)
        check_columns_exist(order_by_columns)
        sorted_rows = sort_rows(order_by_columns)
        sorted_rows = check_where(sorted_rows, where_clause)
        sorted_rows = check_distinct(sorted_rows, is_distinct)
        
        return generate_tuples(sorted_rows, expanded_output_columns)
        
    def update_table(self, update_clause, where_clause):
        """
        Update specified table
        
        Ex: UPDATE table_name SET col1 = value1, col2 = value2;
            UPDATE student SET grades=4.0 WHERE name = 'Josh';
        """
        if where_clause == []:
            while update_clause != []:
                update_name = update_clause.pop(0)
                update_value = update_clause.pop(0)
                
                update_rows = []
                
                for row in self.rows:
                    temp = []
                    for key, value in row.items():
                        if key == update_name:
                            temp.append(update_value)
                        else:
                            temp.append(value)
                    
                    update_rows.append(temp)
    
                self.rows = []
                
                for row in update_rows:
                    self.insert_new_row(row)
        else:
            left = where_clause.pop(0) # name
            
            compare = where_clause.pop(0) # >, <, =, !=, IS NOT, IS
            optional_compare = ""
            
            if compare == '!':
                if where_clause[0] == '=':
                    optional_compare = where_clause.pop(0)
                    
            if compare == "IS":
                if where_clause[0] == "NOT":
                    optional_compare = where_clause.pop(0)
                    
            right = where_clause.pop(0) # value
            
            temp_rows = []
            while update_clause != []:
                update_name = update_clause.pop(0) # name
                update_value = update_clause.pop(0) # value

 
                for row in self.rows:
                    temp = []
                    
                    for key, value in row.items():

                        if key == update_name:
                            
                            for x, y in row.items():
  
                                # >, <, =, !=, IS NOT, IS
                                
                                if x == left:
                                    if compare in "><=!":
                                        if (y is None and right is not None) or (y is not None and right is None):
                                            temp.append(value)
                                        else:
                                            if compare == '>':
                                                if y > right:
                                                    temp.append(update_value)
                                                else:
                                                    temp.append(value)
                                            elif compare == '<':
                                                if y < right:
                                                    temp.append(update_value)
                                                else:
                                                    temp.append(value)
                                            elif compare == '=':
                                                if y == right:
                                                    temp.append(update_value)
                                                else:
                                                    temp.append(value)
                                            elif compare == '!' and optional_compare == '=':
                                                if y != right:
                                                    temp.append(update_value)
                                                else:
                                                    temp.append(value)
                                            
                                    elif compare == 'IS': # Also applies to IS NOT
                                        if (y is not None and right is None) or (y is None and right is not None):
                                            temp.append(update_value)
                                        else:
                                            temp.append(value)

                        else:
                            temp.append(value)
                            
                    temp_rows.append(temp)
                    
                    
            self.rows = []
                
            for row in temp_rows:
                self.insert_new_row(row)
                                            
    
    def delete_from_table(self, where_clause):
        if where_clause == []: # DELETE FROM table;
            self.rows = []
        else: # DELETE FROM table where x compare y;
            left = where_clause.pop(0) # name
            
            compare = where_clause.pop(0) # >, <, =, !=, IS NOT, IS
            optional_compare = ""
            
            if compare == '!':
                if where_clause[0] == '=':
                    optional_compare = where_clause.pop(0)
                    
            if compare == "IS":
                if where_clause[0] == "NOT":
                    optional_compare = where_clause.pop(0)
                    
            right = where_clause.pop(0)  # value
            
            where_rows = []
            
            for row in self.rows:
                for key, value in row.items():
                    if key == left:
                        # >, <, =, !=, IS NOT, IS
                    
                        if compare in "><=!":
                            # Can't compare NULL to a non-NULL
                            if (value is None and right is not None) or (value is not None and right is None):
                                where_rows.append(row)
                            else:
                                if compare == '>':
                                    if value < right:
                                        where_rows.append(row)
                                        
                                elif compare == '<':
                                    if value > right:
                                        where_rows.append(row)
                                        
                                elif compare == '=':
                                    if value != right:
                                        where_rows.append(row)
                                
                                elif compare == '!' and optional_compare == '=':
                                    if value == right:
                                        where_rows.append(row)
                                
                        elif compare == "IS" and optional_compare == "NOT":
                            if value is right:
                                where_rows.append(row)
                                
                        elif compare == "IS":
                            if value is not right:
                                where_rows.append(row)
                                
            self.rows = where_rows


def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits + '.')
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"
    last_quote_pos = 0
    quotes = 0
    len_query = len(query)
    
    for i in range(0, len_query):
        if query[i] == "'":
            last_quote_pos = i
            quotes += 1
        elif query[i] == ',' or query[i] == ')':
            break
        
    if quotes > 2:
        temp_text = query[1:last_quote_pos]
        temp_text = temp_text.replace("''", "'")
        tokens.append(temp_text)
        query = query[last_quote_pos + 1:]
    else:
        assert query[0] == "'"
        query = query[1:]
        end_quote_index = query.find("'")
        
        text = query[:end_quote_index]
        tokens.append(text)
        query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*<>!=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens