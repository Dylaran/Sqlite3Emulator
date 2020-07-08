import unittest
from pprint import pprint
import sqlite3_emulator

class TestMethods(unittest.TestCase):
    # Test 1
    def test_Single_Value(self):
        conn = sqlite3_emulator.connect("test1.db")
        conn.execute("CREATE TABLE students (num INTEGER);")
        conn.execute("INSERT INTO students VALUES (4);")
        result = conn.execute("SELECT num FROM students ORDER BY num;")
        result_list = list(result)
        expected = [(4,)]
        print("expected:",  expected)
        print("student:",  result_list)
        assert expected == result_list

    # Test 2
    def test_Multiple_Values(self):
        conn = sqlite3_emulator.connect("test2.db")
        conn.execute("CREATE TABLE students (num INTEGER);")
        conn.execute("INSERT INTO students VALUES (5);")
        conn.execute("INSERT INTO students VALUES (6);")
        conn.execute("INSERT INTO students VALUES (9);")
        conn.execute("INSERT INTO students VALUES (12);")
        conn.execute("INSERT INTO students VALUES (15);")
        result = conn.execute("SELECT num FROM students ORDER BY num;")
        result_list = list(result)
        expected = [(5,), (6,), (9,), (12,),(15,)]
        print("expected:",  expected)
        print("student:",  result_list)
        assert expected == result_list
    
    # Test 3
    def test_Multiple_Columns(self):
        conn = sqlite3_emulator.connect("test3.db")
        conn.execute("CREATE TABLE students (num INTEGER, num2 INTEGER);")
        conn.execute("INSERT INTO students VALUES (5, 0);")
        conn.execute("INSERT INTO students VALUES (6, 3);")
        conn.execute("INSERT INTO students VALUES (9, 4);")
        conn.execute("INSERT INTO students VALUES (12, 79);")
        result = conn.execute("SELECT num, num2 FROM students ORDER BY num;")
        result_list = list(result)
        expected = [(5, 0), (6, 3), (9, 4), (12, 79)]
        print("expected:",  expected)
        print("student:",  result_list)
        assert expected == result_list

    # Test 4
    def test_Order_By(self):
        conn = sqlite3_emulator.connect("test4.db")
        conn.execute("CREATE TABLE students (num INTEGER, num2 INTEGER);")
        conn.execute("INSERT INTO students VALUES (3, 5);")
        conn.execute("INSERT INTO students VALUES (4, 155);")
        conn.execute("INSERT INTO students VALUES (7, 133);")
        conn.execute("INSERT INTO students VALUES (10, 78);")
        result = conn.execute("SELECT num, num2 FROM students ORDER BY num2;")
        result_list = list(result)
        expected = [(3, 5), (10, 78), (7, 133),(4, 155) ]
        print("expected:",  expected)
        print("student:",  result_list)
        assert expected == result_list

    # Test 5
    def test_Select_Different_Column_Order(self):
        conn = sqlite3_emulator.connect("test5.db")
        conn.execute("CREATE TABLE students (num INTEGER, num2 INTEGER);")
        conn.execute("INSERT INTO students VALUES (3, 5);")
        conn.execute("INSERT INTO students VALUES (4, 1);")
        conn.execute("INSERT INTO students VALUES (7, 135);")
        conn.execute("INSERT INTO students VALUES (48, 135);")
        conn.execute("INSERT INTO students VALUES (10, 78);")
        result = conn.execute("SELECT num2, num FROM students ORDER BY num2;")
        result_list = list(result)
        expected = [(1, 4), (5, 3), (78, 10), (135, 7), (135, 48)]
        print("expected:",  expected)
        print("student: ",  result_list)
        assert expected == result_list

    # Test 6
    def test_Different_Types(self):
        conn = sqlite3_emulator.connect("test6.db")
        conn.execute("CREATE TABLE students (col1 INTEGER, col2 TEXT, col3 REAL);")
        conn.execute("INSERT INTO students VALUES (3, 'hi there', 4.5);")
        conn.execute("INSERT INTO students VALUES (2, 'bye', 4.7);")
        conn.execute("INSERT INTO students VALUES (7842, 'string with spaces', 3.0);")
        conn.execute("INSERT INTO students VALUES (7, 'look a null', NULL);")
        result = conn.execute("SELECT col1, col2, col3 FROM students ORDER BY col1;")
        result_list = list(result)
        expected = [(2, 'bye', 4.7), (3, 'hi there', 4.5), (7, 'look a null', None), (7842, 'string with spaces', 3.0)]
        print("expected:",  expected)
        print("student: ",  result_list)
        assert expected == result_list

    # Test 7
    def test_Column_Names(self):
        conn = sqlite3_emulator.connect("test7.db")
        conn.execute("CREATE TABLE students (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
        conn.execute("INSERT INTO students VALUES (33, 'hi', 4.5);")
        conn.execute("INSERT INTO students VALUES (7842, 'string with spaces', 3.0);")
        conn.execute("INSERT INTO students VALUES (7, 'look a null', NULL);")
        result = conn.execute("SELECT * FROM students ORDER BY col_1;")
        result_list = list(result)
        expected = [(7, 'look a null', None), (33, 'hi', 4.5), (7842, 'string with spaces', 3.0)]
        print("expected:",  expected)
        print("student: ",  result_list)
        assert expected == result_list

    # Test 8
    def test_Select_All(self):
        conn = sqlite3_emulator.connect("test8.db")
        conn.execute("CREATE TABLE students (col1 INTEGER, col2 TEXT, col3 REAL);")
        conn.execute("INSERT INTO students VALUES (3, 'hi', 4.5);")
        conn.execute("INSERT INTO students VALUES (4, 'bye', 9.5);")
        conn.execute("INSERT INTO students VALUES (7842, 'string with spaces', 3.0);")
        conn.execute("INSERT INTO students VALUES (7, 'look a null', NULL);")
        result = conn.execute("SELECT * FROM students ORDER BY col1;")
        result_list = list(result)
        expected = [(3, 'hi', 4.5), (4, 'bye', 9.5), (7, 'look a null', None), (7842, 'string with spaces', 3.0)]
        print("expected:",  expected)
        print("student: ",  result_list)
        assert expected == result_list

    # Test 9
    def test_Spaces(self):
        conn = sqlite3_emulator.connect("test9.db")
        conn.execute("CREATE TABLE students (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
        conn.execute("INSERT INTO students VALUES (33, 'hi', 4.5);")
        conn.execute("INSERT INTO students VALUES (3, 'hweri', 4.5);")
        conn.execute("INSERT INTO students VALUES (75842, 'string with spaces', 3.0);")
        conn.execute("INSERT INTO students VALUES (623, 'string with spaces', 3.0);")
        result = conn.execute("SELECT * FROM students ORDER BY col_3_, col_1;")
        result_list = list(result)
        expected = [(623, 'string with spaces', 3.0), (75842, 'string with spaces', 3.0), (3, 'hweri', 4.5), (33, 'hi', 4.5)]
        print("expected:",  expected)
        print("student: ",  result_list)
        assert expected == result_list

    # Test 10
    def test_Multiple_Tables_and_Selects(self):
        def check(sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
            
        conn = sqlite3_emulator.connect("test10.db")
        conn.execute("CREATE TABLE table1 (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
        conn.execute("INSERT INTO table1 VALUES (33, 'hi', 4.5);")
        conn.execute("CREATE TABLE table_2 (col_1 INTEGER, other INTEGER);")
        conn.execute("INSERT INTO table_2 VALUES (15, 782);")
        conn.execute("INSERT INTO table_2 VALUES (615, 7582);")
        check(
        "SELECT * FROM table1 ORDER BY _col2, col_1;", 
        [(33, 'hi', 4.5)]
        )
        check("SELECT * FROM table_2 ORDER BY other, col_1;", 
        [(15, 782), (615, 7582)]
        )
        conn.execute("INSERT INTO table1 VALUES (3, 'hi', 4.5);")
        conn.execute("INSERT INTO table_2 VALUES (165, 7282);")
        conn.execute("INSERT INTO table1 VALUES (54, 'string with spaces', 3.0);")
        conn.execute("INSERT INTO table1 VALUES (75842, 'string with spaces', 3.0);")
        conn.execute("INSERT INTO table1 VALUES (623, 'string with spaces', 3.0);")
        check("SELECT * FROM table1 ORDER BY _col2, col_1;", 
        [(3, 'hi', 4.5), (33, 'hi', 4.5), (54, 'string with spaces', 3.0), (623, 'string with spaces', 3.0), (75842, 'string with spaces', 3.0)])
        check("SELECT * FROM table_2 ORDER BY other, col_1;", 
        [(15, 782), (165, 7282), (615, 7582)])


    

if __name__ == "__main__":
    unittest.main()