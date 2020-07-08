"""
Name: Don Nakashima

Sources:
Dr. Nahum's Tokenize function
"""

import string
import operator
from operator import itemgetter

_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        self.filename = filename
        self.tokens = [] # Create empty tokens list
        self.tables_list = [] # Create empty tables list

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        self.tokenizes(statement)
        
        if self.tokens[0] == "CREATE":
            self.create_table() # CREATE TABLE
        elif self.tokens[0] == "INSERT":
            self.insert_into_table() # INSERT INTO
        elif self.tokens[0] == "SELECT":
            return self.select_from() # SELECT COLUMNS FROM TABLE
            
    # Decide what to do with the query
    def create_table(self):
        # Pop the front of the query list
        self.tokens.pop(0) # CREATE
        self.tokens.pop(0) # TABLE
        table_name = self.tokens.pop(0) # TABLE NAME
        
        # Create a table object and set the name
        self.table = Table()
        
        # Set the table name
        self.table.set_name(table_name)
        
        # Iterate through the list of tokens and add the parameters to the table
        for index, item in enumerate(self.tokens):
            if item in "(,":
                self.table.add_column(self.tokens[index+1], self.tokens[index+2])
                
        # Add to list of tables
        self.tables_list.append(self.table)
            
        
    def insert_into_table(self):
        # Pop the front of the query list
        self.tokens.pop(0) # INSERT
        self.tokens.pop(0) # INTO
        table_name = self.tokens.pop(0) # TABLE NAME
        
        # Check if table exists, insert the row into that table
        for index, table in enumerate(self.tables_list):
            if table_name == self.tables_list[index].get_name():
                self.tables_list[index].add_row(self.tokens)
            
    def select_from(self):
        # Pop the front of the query list
        self.tokens.pop(0) # SELECT
        
        selects = [] # COLUMNS WE WANT TO SELECT
        rows = [] # ROWS FROM THE TABLE
        columns = [] # COLUMNS FROM THE TABLE
        
        while True:
                column = self.tokens.pop(0)
                selects.append(column)
                
                if (self.tokens[0] == ','):
                    self.tokens.pop(0) # Remove comma
                elif (self.tokens[0] == "FROM"):
                    break
        
        self.tokens.pop(0) # FROM
        table_name = self.tokens.pop(0)
        
        # Get current rows + columns from table
        for index, table in enumerate(self.tables_list):
            if table_name == self.tables_list[index].get_name():
                rows = self.tables_list[index].get_rows()
                columns = self.tables_list[index].get_columns()
                
        # If we want all columns
        if selects[0] == '*':
            selects = []
            
            for index, table in enumerate(self.tables_list):
                if table_name == self.tables_list[index].get_name():
                    temp = self.tables_list[index].get_columns()
                    
            for tup in temp:
                selects.append(tup[0])

        self.tokens.pop(0) # ORDER
        self.tokens.pop(0) # BY
        
        order_by_columns = [] # COLUMNS WE WANT TO ORDER BY
        
        while (self.tokens[0] != ';'):
            column = self.tokens.pop(0)
            order_by_columns.append(column)
            
            if self.tokens[0] == ',':
                self.tokens.pop(0) # Remove comma or semicolon
                
                
        order_by_columns_number = [] # WHAT INDEX WE ORDER BY
        
        for order_by in order_by_columns:
            for index, tuples in enumerate(columns):
                if order_by == tuples[0]:
                    order_by_columns_number.append(index)
        
        rows.sort(key=operator.itemgetter(*order_by_columns_number))
        
        select_column_number = []
        
        for select_column in selects:
            for index, tuples in enumerate(columns):
                if select_column == tuples[0]:
                    select_column_number.append(index)
                    
        temp_row = []
        for row in rows:
            t = ()
            temp_row.append(t)
        
        
        for index in select_column_number:
            for r, row in enumerate(rows):
                temp_row[r] += (row[index],)
                
        return temp_row
        
        
    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass
    
    def tokenizes(self, statement):
        """
        Full credits to Joshua Nahum
        CSE 480's 'Tokenizing SQL statements' from Week 3
        
        Tokenizes depending on starting character of query and adds it to the tokens list
        """
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
          word = collect_characters(query, string.ascii_letters + "_" + string.digits)
          if word == "NULL":
            tokens.append(None)
          else:
            tokens.append(word)
          return query[len(word):]
          
        def remove_text(query, tokens):
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
            
            # Remove leading whitespace
            if query[0] in string.whitespace:
              query = remove_leading_whitespace(query, tokens)
              continue
            
            # Remove a word
            if query[0] in (string.ascii_letters + "_"):
              query = remove_word(query, tokens)
              continue
            
            # Remove non-char
            if query[0] in "(),;*":
              tokens.append(query[0])
              query = query[1:]
              continue
            
            # Remove quote
            if query[0] == "'":
              query = remove_text(query, tokens)
              continue
            
            # Remove number
            if query[0] in string.digits:
              query = remove_number(query, tokens)
              continue
            
            # Assert an error if query didn't get shorter
            if len(query) == len(old_query):
              raise AssertionError("Query did not get shorter.")
              
          return tokens
        
        self.tokens = tokenize(statement)
    

def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)

class Database:
    pass

class Table:
    def __init__(self):
        self.name = ""
        self.rows = []
        self.columns = []
        
    def get_name(self):
        return self.name
        
    def set_name(self, name):
        self.name = name
        
    def add_column(self, column_name, column_type):
        t = (column_name, column_type)
        
        self.columns.append(t) # Adds a column to the table when creating the table
        
    def get_columns(self):
        return self.columns
        
    def add_row(self, row):
        t = ()
        
        for index, value in enumerate(row):
            if row[index] == '(' or row[index] == ',':
                t = t + (row[index + 1],) # Value following ( or ,
                
        self.rows.append(t) # Adds a row to the table when inserting into table

    def get_rows(self):
        return self.rows
    
    