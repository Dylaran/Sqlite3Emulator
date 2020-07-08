import unittest
from pprint import pprint
import database_constructs

class TestMethods(unittest.TestCase):
    # Test 1
    def test_Regression_Check(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = database_constructs.connect("test1.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL, course INTEGER);")
        conn.execute("CREATE TABLE profs (name TEXT, course INTEGER);")
        conn.execute("""INSERT INTO students VALUES ('Zizhen', 4.0, 450),
        ('Cam', 3.5, 480),
        ('Cam', 3.0, 450),
        ('Jie', 0.0, 231),
        ('Jie', 2.0, 331),
        ('Anne', 3.0, 231),
        ('Josh', 1.0, 231),
        ('Josh', 0.0, 480),
        ('Josh', 0.0, 331);""")
        conn.execute("""INSERT INTO profs VALUES ('Josh', 480),
        ('Josh', 450),
        ('Rich', 231),
        ('Sebnem', 331);""")
        check("""SELECT profs.name, students.grade, students.name
        FROM students LEFT OUTER JOIN profs ON students.course = profs.course
        WHERE students.grade > 0.0 ORDER BY students.name, profs.name, students.grade;""", 
        conn, 
        [('Rich', 3.0, 'Anne'),
        ('Josh', 3.0, 'Cam'),
        ('Josh', 3.5, 'Cam'),
        ('Sebnem', 2.0, 'Jie'),
        ('Rich', 1.0, 'Josh'),
        ('Josh', 4.0, 'Zizhen')])

    # Test 2
    def test_DESC_Basic(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)

            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list

        conn = database_constructs.connect("test2.db")
        conn.execute(
            "CREATE TABLE students (name TEXT, grade REAL, course INTEGER);")
        conn.execute("CREATE TABLE profs (name TEXT, course INTEGER);")
        conn.execute("""INSERT INTO students VALUES ('Zizhen', 4.0, 450),
        ('Cam', 3.5, 480),
        ('Cam', 3.0, 450),
        ('Jie', 0.0, 231),
        ('Jie', 2.0, 331),
        ('Dennis', 2.0, 331),
        ('Dennis', 2.0, 231),
        ('Anne', 3.0, 231),
        ('Josh', 1.0, 231),
        ('Josh', 0.0, 480),
        ('Josh', 0.0, 331);""")
        conn.execute("""INSERT INTO profs VALUES ('Josh', 480),
        ('Josh', 450),
        ('Rich', 231),
        ('Sebnem', 331);""")
        check("""SELECT students.name
        FROM students ORDER BY students.name DESC;""",
              conn,
              [('Zizhen',),
               ('Josh',),
                  ('Josh',),
               ('Josh',),
               ('Jie',),
               ('Jie',),
               ('Dennis',),
               ('Dennis',),
               ('Cam',),
               ('Cam',),
               ('Anne',)]
              )

    # Test 3
    def test_DESC_Advanced(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = database_constructs.connect("test3.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL, course INTEGER);")
        conn.execute("CREATE TABLE profs (name TEXT, course INTEGER);")
        conn.execute("""INSERT INTO students VALUES ('Zizhen', 4.0, 450),
        ('Cam', 3.5, 480),
        ('Cam', 3.0, 450),
        ('Jie', 0.0, 231),
        ('Jie', 2.0, 331),
        ('Dennis', 2.0, 331),
        ('Dennis', 2.0, 231),
        ('Anne', 3.0, 231),
        ('Josh', 1.0, 231),
        ('Josh', 0.0, 480),
        ('Josh', 0.0, 331);""")
        conn.execute("""INSERT INTO profs VALUES ('Josh', 480),
        ('Josh', 450),
        ('Rich', 231),
        ('Sebnem', 331);""")
        check("""SELECT profs.name, students.grade, students.name
        FROM students LEFT OUTER JOIN profs ON students.course = profs.course
        WHERE students.grade > 0.0 ORDER BY students.grade, students.name DESC, profs.name DESC;""", 
        conn, 
        [('Rich', 1.0, 'Josh'),
        ('Sebnem', 2.0, 'Jie'),
        ('Sebnem', 2.0, 'Dennis'),
        ('Rich', 2.0, 'Dennis'),
        ('Josh', 3.0, 'Cam'),
        ('Rich', 3.0, 'Anne'),
        ('Josh', 3.5, 'Cam'),
        ('Josh', 4.0, 'Zizhen')]
        )

    # Test 4
    def test_Default_Values(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
            
        conn = database_constructs.connect("test4.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL DEFAULT 0.0, id TEXT);")
        conn.execute("INSERT INTO students VALUES ('Zizhen', 4.0, 'Hi');")
        conn.execute("INSERT INTO students (name, id) VALUES ('Cam', 'Hello');")
        conn.execute("INSERT INTO students (id, name) VALUES ('Instructor', 'Josh');")
        conn.execute("INSERT INTO students (id, name, grade) VALUES ('TA', 'Dennis', 3.0);")
        conn.execute("INSERT INTO students (id, name) VALUES ('regular', 'Emily'), ('regular', 'Alex');")
        check("""SELECT name, id, grade  FROM students ORDER BY students.name;""", 
        conn, 
        [('Alex', 'regular', 0.0),
        ('Cam', 'Hello', 0.0),
        ('Dennis', 'TA', 3.0),
        ('Emily', 'regular', 0.0),
        ('Josh', 'Instructor', 0.0),
        ('Zizhen', 'Hi', 4.0)]
        )

    # Test 5
    def test_Default_Values_All(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = database_constructs.connect("test5.db")
        conn.execute("CREATE TABLE students (name TEXT DEFAULT '', health INTEGER DEFAULT 100, grade REAL DEFAULT 0.0, id TEXT DEFAULT 'NONE PROVIDED');")
        conn.execute("INSERT INTO students VALUES ('Zizhen', 45, 4.0, 'Hi');")
        conn.execute("INSERT INTO students DEFAULT VALUES;")
        conn.execute("INSERT INTO students (name, id) VALUES ('Cam', 'Hello');")
        conn.execute("INSERT INTO students (id, name) VALUES ('Instructor', 'Josh');")
        conn.execute("INSERT INTO students DEFAULT VALUES;")
        conn.execute("INSERT INTO students (id, name, grade) VALUES ('TA', 'Dennis', 3.0);")
        conn.execute("INSERT INTO students (id, name) VALUES ('regular', 'Emily'), ('regular', 'Alex');")
        check("""SELECT name, id, grade, health  FROM students ORDER BY students.name;""", 
        conn, 
        [('', 'NONE PROVIDED', 0.0, 100),
        ('', 'NONE PROVIDED', 0.0, 100),
        ('Alex', 'regular', 0.0, 100),
        ('Cam', 'Hello', 0.0, 100),
        ('Dennis', 'TA', 3.0, 100),
        ('Emily', 'regular', 0.0, 100),
        ('Josh', 'Instructor', 0.0, 100),
        ('Zizhen', 'Hi', 4.0, 45)]
        )

    # Test 6
    def test_Parameterized_Queries(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = database_constructs.connect("test6.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
        conn.executemany("INSERT INTO students VALUES (?, ?, 480);", [('Josh', 3.5), ('Tyler', 2.5), ('Grant', 3.0)])
        check("""SELECT name, class, grade FROM students ORDER BY grade;""", 
        conn, 
        [('Tyler', 480, 2.5), ('Grant', 480, 3.0), ('Josh', 480, 3.5)]
        )

    # Test 7
    def test_Parameterized_Queries2(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
            
        conn = database_constructs.connect("test7.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER DEFAULT 231);")
        conn.executemany("INSERT INTO students VALUES (?, ?, 480);", [('Josh', 3.5), ('Tyler', 2.5), ('Grant', 3.0)])
        conn.executemany("INSERT INTO students VALUES (?, 0.0, ?);", [('Jim', 231), ('Tim', 331), ('Gary', 450)])
        conn.executemany("INSERT INTO students (grade, name) VALUES (?, ?);", [(4.1, 'Tess'), (1.1, 'Jane')])
        check("""SELECT name, class, grade FROM students ORDER BY grade, name;""", 
        conn, 
        [('Gary', 450, 0.0),
        ('Jim', 231, 0.0),
        ('Tim', 331, 0.0),
        ('Jane', 231, 1.1),
        ('Tyler', 480, 2.5),
        ('Grant', 480, 3.0),
        ('Josh', 480, 3.5),
        ('Tess', 231, 4.1)] 
        )

    # Test 8
    def test_Custom_Collation(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = database_constructs.connect("test8.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
        conn.executemany("INSERT INTO students VALUES (?, ?, ?);", 
            [('Josh', 3.5, 480), 
            ('Tyler', 2.5, 480), 
            ('Tosh', 4.5, 450), 
            ('Losh', 3.2, 450), 
            ('Grant', 3.3, 480), 
            ('Emily', 2.25, 450), 
            ('James', 2.25, 450)])
        check("SELECT * FROM students ORDER BY class, name;",
        conn,
        [('Emily', 2.25, 450),
        ('James', 2.25, 450),
        ('Losh', 3.2, 450),
        ('Tosh', 4.5, 450),
        ('Grant', 3.3, 480),
        ('Josh', 3.5, 480),
        ('Tyler', 2.5, 480)]
        )
        def collate_ignore_first_letter(string1, string2):
            string1 = string1[1:]
            string2 = string2[1:]
            if string1 == string2:
                return 0
            if string1 < string2:
                return -1
            else:
                return 1
        conn.create_collation("skip", collate_ignore_first_letter)
        check("SELECT * FROM students ORDER BY name COLLATE skip, grade;", 
        conn, 
        [('James', 2.25, 450),
        ('Emily', 2.25, 450),
        ('Losh', 3.2, 450),
        ('Josh', 3.5, 480),
        ('Tosh', 4.5, 450),
        ('Grant', 3.3, 480),
        ('Tyler', 2.5, 480)]
        )

    # Test 9
    def test_Custom_Collation_DESC(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = database_constructs.connect("test9.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
        conn.executemany("INSERT INTO students VALUES (?, ?, ?);", 
            [('Josh', 3.5, 480), 
            ('Tyler', 2.5, 480), 
            ('Tosh', 4.5, 450), 
            ('Losh', 3.2, 450), 
            ('Grant', 3.3, 480), 
            ('Emily', 2.25, 450), 
            ('James', 2.25, 450)])
        check("SELECT * FROM students ORDER BY class, name;",
        conn,
        [('Emily', 2.25, 450),
        ('James', 2.25, 450),
        ('Losh', 3.2, 450),
        ('Tosh', 4.5, 450),
        ('Grant', 3.3, 480),
        ('Josh', 3.5, 480),
        ('Tyler', 2.5, 480)]
        )
        def collate_ignore_first_letter(string1, string2):
            string1 = string1[1:]
            string2 = string2[1:]
            if string1 == string2:
                return 0
            if string1 < string2:
                return -1
            else:
                return 1
        conn.create_collation("skip", collate_ignore_first_letter)
        check("SELECT * FROM students ORDER BY name COLLATE skip DESC, grade;", 
        conn, 
        [('Tyler', 2.5, 480),
        ('Grant', 3.3, 480),
        ('Losh', 3.2, 450),
        ('Josh', 3.5, 480),
        ('Tosh', 4.5, 450),
        ('Emily', 2.25, 450),
        ('James', 2.25, 450)]
        )
        
        check("SELECT * FROM students WHERE class = 480 ORDER BY grade DESC, name COLLATE skip DESC;", 
        conn, 
        [('Josh', 3.5, 480), ('Grant', 3.3, 480), ('Tyler', 2.5, 480)]
        )

    # Test 10
    def test_Custom_Collation_Another(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = database_constructs.connect("test10.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
        conn.executemany("INSERT INTO students VALUES (?, ?, ?);", 
            [('Josh', 3.5, 480), 
            ('Tyler', 2.5, 480), 
            ('Alice', 2.2, 231), 
            ('Tosh', 4.5, 450), 
            ('Losh', 3.2, 450), 
            ('Grant', 3.3, 480), 
            ('Emily', 2.25, 450), 
            ('James', 2.25, 450)])
        check("SELECT * FROM students ORDER BY class, name;",
        conn,
        [('Alice', 2.2, 231),
        ('Emily', 2.25, 450),
        ('James', 2.25, 450),
        ('Losh', 3.2, 450),
        ('Tosh', 4.5, 450),
        ('Grant', 3.3, 480),
        ('Josh', 3.5, 480),
        ('Tyler', 2.5, 480)]
        )
        def collate_ignore_first_two_letter(string1, string2):
            string1 = string1[2:]
            string2 = string2[2:]
            if string1 == string2:
                return 0
            if string1 < string2:
                return -1
            else:
                return 1
        conn.create_collation("skip2", collate_ignore_first_two_letter)
        check("SELECT * FROM students ORDER BY name COLLATE skip2 DESC, grade;", 
        conn, 
        [('Losh', 3.2, 450),
        ('Josh', 3.5, 480),
        ('Tosh', 4.5, 450),
        ('James', 2.25, 450),
        ('Tyler', 2.5, 480),
        ('Emily', 2.25, 450),
        ('Alice', 2.2, 231),
        ('Grant', 3.3, 480)]
        )
        
        check("SELECT * FROM students WHERE class = 480 ORDER BY grade DESC, name COLLATE skip2 DESC;", 
        conn, 
        [('Josh', 3.5, 480), ('Grant', 3.3, 480), ('Tyler', 2.5, 480)]
        )
    
    # Test 11
    def test_Aggregate_Functions(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = database_constructs.connect("test11.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
        conn.executemany("INSERT INTO students VALUES (?, ?, ?);", 
            [('Josh', 3.5, 480), 
            ('Tyler', 2.5, 480), 
            ('Tosh', 4.5, 450), 
            ('Losh', 3.2, 450), 
            ('Grant', 3.3, 480), 
            ('Emily', 2.25, 450), 
            ('James', 2.25, 450)])
        check("SELECT max(grade) FROM students ORDER BY grade;",
        conn,
        [(4.5,)]
        )
        check("SELECT min(class), max(name) FROM students ORDER BY grade, name;",
        conn,
        [(450, 'Tyler')]
        )

    # Test 12
    def test_Aggregate_Functions_With_WHERE(self):
        def check(sql_statement, conn, expected):
            print("SQL: " + sql_statement)
            result = conn.execute(sql_statement)
            result_list = list(result)
            
            print("expected:")
            pprint(expected)
            print("student: ")
            pprint(result_list)
            assert expected == result_list
        
        conn = database_constructs.connect("test12.db")
        conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
        conn.executemany("INSERT INTO students VALUES (?, ?, ?);", 
            [('Josh', 3.5, 480), 
            ('Tyler', 2.5, 480), 
            ('Tosh', 4.5, 450), 
            ('Losh', 3.2, 450), 
            ('Grant', 3.3, 480), 
            ('Emily', 2.25, 450), 
            ('James', 2.25, 450)])
        check("SELECT max(grade) FROM students WHERE class = 480 ORDER BY grade;",
        conn,
        [(3.5,)]
        )
        check("SELECT min(grade), min(name) FROM students WHERE name > 'T' ORDER BY grade, name;",
        conn,
        [(2.5, 'Tosh')]
        )
    
if __name__ == "__main__":
    unittest.main()
