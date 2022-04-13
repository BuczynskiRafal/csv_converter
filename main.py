"""Set of function allow converting csv file to sqlite database."""
import csv
import sqlite3


FILE = 'annual-enterprise-survey-2020-financial-year-provisional-size-bands-csv.csv'

# 1 Create DB with annual_enterprise_survey_2020 table
def connect_with_db(func):
    """Create connection with DB."""
    def wrapper():
        conn = sqlite3.connect("annual_enterprise_survey_2020.sqlite3")
        cur = conn.cursor()
        func_work = func(cur=cur)
        conn.commit()
        conn.close()
        return func_work

    return wrapper


@connect_with_db
def create_db(cur):
    """Create user table in DB."""
    query = """DROP TABLE IF EXISTS "annual_enterprise_survey_2020";
            CREATE TABLE IF NOT EXISTS "annual_enterprise_survey_2020" (
            "id" integer NOT NULL
            , "year" integer NOT NULL
            , "industry_code_ANZSIC" text NOT NULL
            , "industry_name_ANZSIC" text NOT NULL
            , "rme_size_grp" text NOT NULL
            , "variable" text NOT NULL
            , "value" integer NOT NULL
            , "unit" integer NOT NULL
            , PRIMARY KEY("id" AUTOINCREMENT)
            );
            """
    cur.executescript(query)
    return "Created"


@connect_with_db
def check_if_exist(cur):
    """Check if annual_enterprise_survey_2020 table exist in DB."""
    query = """
    SELECT count(*) FROM "annual_enterprise_survey_2020";
    """
    try:
        cur.execute(query)
        if len(cur.fetchall()[0]) > 0:
            return True
        else:
            return False
    except sqlite3.OperationalError as error:
        return False


@connect_with_db
def show_table(cur):
    """Print all rows form annual_enterprise_survey_2020 table."""
    query = """
    SELECT * FROM "annual_enterprise_survey_2020";
    """
    cur.execute(query)
    return cur.fetchall()


# 2 Load data from csv to annual_enterprise_survey_2020 table

def add_data_from_csv():
    """Create user in annual_enterprise_survey_2020 table"""
    conn = sqlite3.connect("annual_enterprise_survey_2020.sqlite3")
    cur = conn.cursor()
    if check_if_exist() is False:
        create_db()
    reader = csv.reader(open(FILE, 'r'), delimiter=',')
    for row in reader:
        to_db = [row[0], row[1], row[2], row[3], row[4], row[5], row[6]]
        cur.execute("INSERT INTO annual_enterprise_survey_2020 (year, industry_code_ANZSIC, industry_name_ANZSIC, rme_size_grp, variable, value, unit) VALUES (?, ?, ?, ?, ?, ?, ?);", to_db)
        conn.commit()
    conn.close()


if __name__ == '__main__':
    if check_if_exist() is False:
        create_db()
        print('created')
        add_data_from_csv()
        print('saved')
    else:
        print(show_table())