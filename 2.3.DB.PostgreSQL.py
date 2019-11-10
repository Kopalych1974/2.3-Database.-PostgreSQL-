import psycopg2 as ps

student = {
    'name': "Homer Simpson",
    'gpa': 2.5,
    'birth': '1955-06-22'
}


students = [
    {
        'name': "Marge Simpson",
        'gpa': 4.7,
        'birth': '1966-04-04'
    },
    {
        'name': "Bart Simpson",
        'gpa': 3.2,
        'birth': '2000-08-11'
    },
    {
        'name': "Lisa Simpson",
        'gpa': 5.2,
        'birth': '2001-06-15'
    }
]

def create_db():  # создает таблицы
    with ps.connect(database='netology_db_homework') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE student (
                    id serial PRIMARY KEY,
                    name varchar(100),
                    gpa numeric(10,2),
                    birth timestamp with time zone);
                """)

            cur.execute("""
                CREATE TABLE course (
                    id serial PRIMARY KEY,
                    name varchar(100));
                """)

            cur.execute("""
                CREATE TABLE student_course (
                    id serial PRIMARY KEY,
                    student_id INTEGER REFERENCES student(id),
                    course_id INTEGER REFERENCES course(id));
                """)

def add_course(course): # оздает курсы, на которые записываем студентов
    with ps.connect(database='netology_db_homework') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO course(name) VALUES (%s)
                """, (course, ))

def add_students(course_id, students):  # создает студентов и записывает их на курс
    conn = ps.connect(database='netology_db_homework')
    cur = conn.cursor()

    for i in range(0, len(students)):

        cur.execute("""
            INSERT INTO student(name, gpa, birth) VALUES (%s, %s, %s) RETURNING id
            """, (students[i]['name'], students[i]['gpa'], students[i]['birth']))

        added_student_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO student_course(student_id, course_id) VALUES (%s, %s)
            """, (added_student_id, course_id))

        conn.commit()

def get_students(course_id):  # возвращает студентов определенного курса
    with ps.connect(database='netology_db_homework') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.name, s.gpa, s.birth FROM student s
                JOIN student_course sc
                ON s.id = sc.student_id
                WHERE sc.course_id = %s
                """, (course_id, ))
            student = cur.fetchall()
            return student

def add_student(student):  # просто создает студента
    with ps.connect(database='netology_db_homework') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO student(name, gpa, birth) VALUES (%s, %s, %s)
                """, (student['name'], student['gpa'], student['birth']))

def get_student(student_id):
    with ps.connect(database='netology_db_homework') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM student WHERE id = %s
                """, (student_id, ))
            student = cur.fetchone()
            return student

if __name__ == "__main__":
    create_db()
    add_course('Курс программирования на Python')
    add_course('Курс программирования на Java')
    add_course('Курс программирования на Go')
    add_students(2, students)
    add_student(student)
    print(get_students(2))
    print(get_student(1))
