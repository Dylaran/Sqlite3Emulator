import unittest
from pprint import pprint
import data_definition_language

class TestMethods(unittest.TestCase):
    # Test 1
    def test_Regression_Check(self):
        def check(sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
            
        conn = data_definition_language.connect("test1.db")
        conn.execute("CREATE TABLE pets (name TEXT, species TEXT, age INTEGER);")
        conn.execute("CREATE TABLE owners (name TEXT, age INTEGER, id INTEGER);")
        conn.execute("INSERT INTO pets VALUES ('RaceTrack', 'Ferret', 3), ('Ghost', 'Ferret', 2), ('Zoe', 'Dog', 7), ('Ebony', 'Dog', 17);")
        conn.execute("INSERT INTO pets (species, name) VALUES ('Rat', 'Ginny'), ('Dog', 'Balto'), ('Dog', 'Clifford');")
        conn.execute("UPDATE pets SET age = 15 WHERE name = 'RaceTrack';")
        check(
        "SELECT species, *, pets.name FROM pets WHERE age > 3 ORDER BY pets.name;", 
        [('Dog', 'Ebony', 'Dog', 17, 'Ebony'),
        ('Ferret', 'RaceTrack', 'Ferret', 15, 'RaceTrack'),
        ('Dog', 'Zoe', 'Dog', 7, 'Zoe')]
        )
        conn.execute("INSERT INTO owners VALUES ('Josh', 29, 10), ('Emily', 27, 2), ('Zach', 25, 4), ('Doug', 34, 5);")
        conn.execute("DELETE FROM owners WHERE name = 'Doug';")
        check(
        "SELECT owners.* FROM owners ORDER BY id;", 
        [('Emily', 27, 2), ('Zach', 25, 4), ('Josh', 29, 10)]
        )
        conn.execute("CREATE TABLE ownership (name TEXT, id INTEGER);")
        conn.execute("INSERT INTO ownership VALUES ('RaceTrack', 10), ('Ginny', 2), ('Ghost', 2), ('Zoe', 4);")
        check("SELECT pets.name, pets.age, ownership.id FROM pets LEFT OUTER JOIN ownership ON pets.name = ownership.name WHERE pets.age IS NULL ORDER BY pets.name;",
        [('Balto', None, None), ('Clifford', None, None), ('Ginny', None, 2)]
        )

    # Test 2
    def test_Connect_Interface(self):
        def check(sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = data_definition_language.connect("test2.db", timeout=0.1, isolation_level=None)
        conn.execute("CREATE TABLE pets (name TEXT, species TEXT, age INTEGER);")
        conn.execute("INSERT INTO pets VALUES ('RaceTrack', 'Ferret', 3), ('Ghost', 'Ferret', 2), ('Zoe', 'Dog', 7), ('Ebony', 'Dog', 17);")
        check(
        "SELECT * FROM pets ORDER BY pets.name;", 
        [('Ebony', 'Dog', 17),
        ('Ghost', 'Ferret', 2),
        ('RaceTrack', 'Ferret', 3),
        ('Zoe', 'Dog', 7)]
        )

    # Test 3
    def test_Multiple_Connections(self):
        def check(conn, sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn_1 = data_definition_language.connect("test3.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT, grade REAL);")
        conn_1.execute("INSERT INTO students VALUES ('Josh', 2.4);")
        conn_2 = data_definition_language.connect("test3.db", timeout=0.1, isolation_level=None)
        conn_2.execute("CREATE TABLE colors (r INTEGER, g INTEGER, b INTEGER);")
        conn_2.execute("INSERT INTO colors VALUES (1, 2, 3);")
        check(conn_2, "SELECT * FROM colors ORDER BY r;",
        [(1, 2, 3)]
        )
        check(conn_1, "SELECT * FROM students ORDER BY name;",
        [('Josh', 2.4)]
        )

    # Test 4
    def test_Multiple_Connections_No_Transactions(self):
        def check(conn, sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn_1 = data_definition_language.connect("test4.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT, grade REAL);")
        conn_1.execute("INSERT INTO students VALUES ('Josh', 2.4);")
        conn_2 = data_definition_language.connect("test4.db", timeout=0.1, isolation_level=None)
        conn_2.execute("CREATE TABLE colors (r INTEGER, g INTEGER, b INTEGER);")
        conn_2.execute("INSERT INTO colors VALUES (1, 2, 3);")
        check(conn_2, "SELECT * FROM colors ORDER BY r;",
        [(1, 2, 3)]
        )
        check(conn_1, "SELECT * FROM students ORDER BY name;",
        [('Josh', 2.4)]
        )
        check(conn_2, "SELECT * FROM students ORDER BY name;",
        [('Josh', 2.4)]
        )
        conn_3 = data_definition_language.connect("test4.db", timeout=0.1, isolation_level=None)
        check(conn_3, "SELECT * FROM students ORDER BY name;",
        [('Josh', 2.4)]
        )

    # Test 5
    def test_Multiple_Connections_Later_Changes(self):
        def check(conn, sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn_1 = data_definition_language.connect("test5.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT, grade REAL);")
        conn_1.execute("INSERT INTO students VALUES ('Josh', 2.4);")
        conn_2 = data_definition_language.connect("test5.db", timeout=0.1, isolation_level=None)
        conn_2.execute("CREATE TABLE colors (r INTEGER, g INTEGER, b INTEGER);")
        conn_2.execute("INSERT INTO colors VALUES (1, 2, 3);")
        conn_2.execute("INSERT INTO colors VALUES (4, 5, 6);")
        check(conn_2, "SELECT * FROM colors ORDER BY r;",
        [(1, 2, 3), (4, 5, 6)]
        )
        check(conn_1, "SELECT * FROM students ORDER BY name;",
        [('Josh', 2.4)]
        )
        check(conn_2, "SELECT * FROM students ORDER BY name;",
        [('Josh', 2.4)]
        )
        conn_1.execute("INSERT INTO students VALUES ('Cam', 4.0);")
        conn_3 = data_definition_language.connect("test5.db", timeout=0.1, isolation_level=None)
        check(conn_3, "SELECT * FROM students ORDER BY name;",
        [('Cam', 4.0), ('Josh', 2.4)]
        )

    # Test 6
    def test_Create_Table(self):
        conn_1 = data_definition_language.connect("test6.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")

        with self.assertRaises(Exception):
            conn_1.execute("CREATE TABLE students (name TEXT);")

        conn_1.execute("CREATE TABLE other (name TEXT);")
        conn_2 = data_definition_language.connect("test6.db", timeout=0.1, isolation_level=None)

        with self.assertRaises(Exception):
            conn_2.execute("CREATE TABLE other (name TEXT);")

        conn_2.execute("CREATE TABLE more (name TEXT);")

    # Test 7
    def test_Create_Table_If_Not_Exists(self):
        conn_1 = data_definition_language.connect("test7.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("CREATE TABLE IF NOT EXISTS students (name TEXT);")

        with self.assertRaises(Exception):
            conn_1.execute("CREATE TABLE students (name TEXT);")

        conn_1.execute("CREATE TABLE IF NOT EXISTS other (name TEXT);")
        conn_2 = data_definition_language.connect("test7.db", timeout=0.1, isolation_level=None)

        with self.assertRaises(Exception):
            conn_2.execute("CREATE TABLE other (name TEXT);")

        conn_2.execute("CREATE TABLE more (name TEXT);")
    
    # Test 8
    def test_Create_Drop_Table(self):
        conn_1 = data_definition_language.connect("test8.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("DROP TABLE students;")

        with self.assertRaises(Exception):
            conn_1.execute("DROP TABLE students;")

        conn_2 = data_definition_language.connect("test8.db", timeout=0.1, isolation_level=None)
        conn_2.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("DROP TABLE students;")

        with self.assertRaises(Exception):
            conn_2.execute("DROP TABLE students;")

    # Test 9
    def test_Create_Drop_Table_If_Exists(self):
        conn_1 = data_definition_language.connect("test9.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("DROP TABLE students;")
        conn_1.execute("DROP TABLE IF EXISTS students;")

        conn_2 = data_definition_language.connect("test9.db", timeout=0.1, isolation_level=None)
        conn_2.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("DROP TABLE students;")

        with self.assertRaises(Exception):
            conn_2.execute("DROP TABLE students;")

    # Test 10
    def test_Create_Drop_Table_Drop_Rows_In_Table(self):
        def check(conn, sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn_1 = data_definition_language.connect("test10.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_1.execute("DROP TABLE IF EXISTS students;")
        conn_1.execute("CREATE TABLE students (name TEXT);")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [])

        conn_2 = data_definition_language.connect("test10.db", timeout=0.1, isolation_level=None)
        conn_2.execute("INSERT INTO students VALUES ('Zizhen');")
        conn_2.execute("CREATE TABLE IF NOT EXISTS students (name TEXT);")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [('Zizhen',)])
        conn_1.execute("INSERT INTO students VALUES ('Cam');")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [('Cam',), ('Zizhen',)])

        with self.assertRaises(Exception):
            conn_2.execute("CREATE TABLE students (name TEXT);")

        conn_1.execute("DROP TABLE students;")

        with self.assertRaises(Exception):
            conn_2.execute("DROP TABLE students;")
 
    # Test 11
    def test_Transactions_Syntax(self):
        conn_1 = data_definition_language.connect("test11.db", timeout=0.1, isolation_level=None)
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")
        conn_2 = data_definition_language.connect("test11.db", timeout=0.1, isolation_level=None)
        conn_2.execute("BEGIN TRANSACTION;")
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")
        with self.assertRaises(Exception):
            conn_2.execute("BEGIN TRANSACTION;")

        conn_2.execute("COMMIT TRANSACTION;")

        with self.assertRaises(Exception):
            conn_2.execute("COMMIT TRANSACTION;")


    # Test 12
    def test_Transactions_With_DML(self):
        def check(conn, sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn_1 = data_definition_language.connect("test12.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")
        conn_1.execute("BEGIN TRANSACTION;")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_1.execute("COMMIT TRANSACTION;")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [("Josh",)])

    # Test 13
    def test_Transactions_With_DML2(self):
        def check(conn, sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn_1 = data_definition_language.connect("test13.db", timeout=0.1, isolation_level=None)
        conn_2 = data_definition_language.connect("test13.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_1.execute("BEGIN TRANSACTION;")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_1.execute("INSERT INTO students VALUES ('Cam');")
        check(conn_1, "SELECT * FROM students ORDER BY name;", 
        [("Cam",), ("Josh",)])
        check(conn_2, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_1.execute("COMMIT TRANSACTION;")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [("Cam",), ("Josh",)])
        check(conn_1, "SELECT * FROM students ORDER BY name;", [("Cam",), ("Josh",)])

    # Test 14
    def test_Transactions_With_DML3(self):
        def check(conn, sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn_1 = data_definition_language.connect("test14.db", timeout=0.1, isolation_level=None)
        conn_2 = data_definition_language.connect("test14.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('Josh');")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_1.execute("BEGIN TRANSACTION;")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_1.execute("INSERT INTO students VALUES ('Cam');")
        check(conn_1, "SELECT * FROM students ORDER BY name;", 
        [("Cam",), ("Josh",)])
        check(conn_2, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_2.execute("BEGIN TRANSACTION;")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_1.execute("INSERT INTO students VALUES ('Zizhen');")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_2.execute("COMMIT TRANSACTION;")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [("Josh",)])
        conn_1.execute("COMMIT TRANSACTION;")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [("Cam",), ("Josh",), ("Zizhen",)])
        check(conn_2, "SELECT * FROM students ORDER BY name;", [("Cam",), ("Josh",), ("Zizhen",)])

    # Test 15
    def test_Transaction_Modes_Syntax(self): 
        conn_1 = data_definition_language.connect("test15.db", timeout=0.1, isolation_level=None)
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")
        conn_1.execute("BEGIN DEFERRED TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")
        conn_1.execute("BEGIN IMMEDIATE TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")
        conn_1.execute("BEGIN EXCLUSIVE TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")

    # Test 16
    def test_Transaction_Modes_Exclusive_With_DML(self):
        conn_1 = data_definition_language.connect("test16.db", timeout=0.1, isolation_level=None)
        conn_2 = data_definition_language.connect("test16.db", timeout=0.1, isolation_level=None)
        conn_3 = data_definition_language.connect("test16.db", timeout=0.1, isolation_level=None)
        conn_4 = data_definition_language.connect("test16.db", timeout=0.1, isolation_level=None)
        conn_5 = data_definition_language.connect("test16.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_2.execute("BEGIN EXCLUSIVE TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("INSERT INTO students VALUES ('Josh');")

        conn_3.execute("BEGIN TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("INSERT INTO students VALUES ('Josh');")

        conn_2.execute("INSERT INTO students VALUES ('Josh');")

        with self.assertRaises(Exception):
            conn_4.execute("SELECT * FROM students ORDER BY name;")
            
        conn_2.execute("COMMIT TRANSACTION;")
        conn_5.execute("INSERT INTO students VALUES ('Josh');")

    # Test 17
    def test_Rollback_Syntax(self):
        conn_1 = data_definition_language.connect("test17.db", timeout=0.1, isolation_level=None)
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("ROLLBACK TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("COMMIT TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("ROLLBACK TRANSACTION;")

        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")

        with self.assertRaises(Exception):
            conn_1.execute("ROLLBACK TRANSACTION;")

    # Test 18
    def test_Rollback_Undo(self):
        def check(conn, sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list

        conn_1 = data_definition_language.connect("test18.db", timeout=0.1, isolation_level=None)
        conn_2 = data_definition_language.connect("test18.db", timeout=0.1, isolation_level=None)

        conn_1.execute("CREATE TABLE students (name TEXT);")
        conn_1.execute("INSERT INTO students VALUES ('a');")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [('a',)])
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("INSERT INTO students VALUES ('b');")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [('a',), ('b',)])
        conn_1.execute("ROLLBACK TRANSACTION;")
        check(conn_1, "SELECT * FROM students ORDER BY name;", [('a',)])
        conn_2.execute("BEGIN TRANSACTION;")
        conn_1.execute("BEGIN TRANSACTION;")
        conn_1.execute("INSERT INTO students VALUES ('c');")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [('a',)])
        conn_2.execute("ROLLBACK TRANSACTION;")
        conn_1.execute("COMMIT TRANSACTION;")
        check(conn_2, "SELECT * FROM students ORDER BY name;", [('a',), ('c',)])

    # Test 19
    def test_Multiple_Tables(self):
        def check(conn, sql_statement, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)

            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list


        conn_1 = data_definition_language.connect("test19.db", timeout=0.1, isolation_level=None)
        conn_2 = data_definition_language.connect("test19.db", timeout=0.1, isolation_level=None)
        conn_3 = data_definition_language.connect("test19.db", timeout=0.1, isolation_level=None)
        conn_4 = data_definition_language.connect("test19.db", timeout=0.1, isolation_level=None)
        conn_1.execute("CREATE TABLE students (name TEXT, id INTEGER);")
        conn_2.execute(
            "CREATE TABLE grades (grade INTEGER, name TEXT, student_id INTEGER);")
        conn_3.execute(
            "INSERT INTO students (id, name) VALUES (42, 'Josh'), (7, 'Cam');")
        conn_2.execute(
            "INSERT INTO grades VALUES (99, 'CSE480', 42), (80, 'CSE450', 42), (70, 'CSE480', 9);")
        conn_2.execute("BEGIN DEFERRED TRANSACTION;")
        conn_1.execute("BEGIN IMMEDIATE TRANSACTION;")
        conn_1.execute("INSERT INTO grades VALUES (10, 'CSE231', 1);")
        check(conn_2, "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
            [(80, 'CSE450', 'Josh'), (70, 'CSE480', None), (99, 'CSE480', 'Josh')]
            )
        check(conn_1, "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
            [(10, 'CSE231', None),
            (80, 'CSE450', 'Josh'),
                (70, 'CSE480', None),
                (99, 'CSE480', 'Josh')]
            )
        conn_2.execute("COMMIT TRANSACTION;")
        check(conn_2, "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
            [(80, 'CSE450', 'Josh'), (70, 'CSE480', None), (99, 'CSE480', 'Josh')]
            )
        with self.assertRaises(Exception):
            conn_3.execute("INSERT INTO students VALUES ('Zach', 732);")
        conn_1.execute("ROLLBACK TRANSACTION;")
        check(conn_1, "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
            [(80, 'CSE450', 'Josh'), (70, 'CSE480', None), (99, 'CSE480', 'Josh')]
            )
        conn_1.execute("DROP TABLE IF EXISTS other;")
        conn_3.execute("INSERT INTO students VALUES ('Zach', 732);")
        check(conn_4, "SELECT name FROM students WHERE name > 'A' ORDER BY name;",
            [('Cam',), ('Josh',), ('Zach',)]
            )

if __name__ == "__main__":
    unittest.main()
