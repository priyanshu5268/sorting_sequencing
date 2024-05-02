import os
from config import year, season, Columns, conn_string, conn_string_spss
import pandas as pd
from flask import Flask, render_template, request,  session
from werkzeug.utils import secure_filename
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
import csv
import mysql.connector
import numpy as np

try:
    os.makedirs("temporary_files")
except FileExistsError:
    pass
UPLOAD_FOLDER = os.path.join('temporary_files')
ALLOWED_EXTENSIONS = {'csv'}
app = Flask(__name__, template_folder='templates', static_folder='temporary_files')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

#Define secret key to enable session
app.secret_key = 'This is your secret key to utlilize session in flask'

@app.route('/spss', methods=("POST", "GET"))
def chooseFile():
    if request.methods == 'POST':
        uploaded_df = request.files['uploaded-file']
        data_filename = secure_filename(uploaded_df.filename)
        uploaded_df.save(os.path.join(app.config['UPLOAD_FOLDER'], data_filename))
        # Storing uploaded file path in flask session
        session['temporary_files'] = os.path.join(app.config['UPLOAD_FOLDER'], data_filename)
        df = pd.read_csv(session['temporary_files'])
        #df = pd.read_csv(data)
        for col in df.columns:
            miss = df[col].isnull().sum()
            if miss > 0:
                return ("{} has {} missing value(s)".format(col, miss))
            else:
                continue

        list_records = df.to_dict('records')
        bad_records = []

        dup = df.duplicate().sum()
        if dup > 0:
            bad_records.append(dup)
            return(len(bad_records), "duplicate records found")

        def validate_date(val):
            try:
                date_year = int(val)
            except ValueError:
                return False
            if not date_year:
                return False
            if type(int(date_year)) != int:
                return False
            if date_year in year:
                return True

        for records in list_records[0:]:
            valid_date = validate_date(record['YEAR'])
            if not valid_date:
                bad_records.append(records)
                return(len(bad_records), "Bad records found in Year")
            else:
                continue

        def validate_season(val):
            try:
                date_season = int(val)
            except ValueError:
                return False
            if not date_season:
                return False
            if type(int(date_season)) != int:
                return False
            if date_season in season:
                return True

        for records in list_records[0:]:
            valid_season = validate_season(records['Season'])
            if not valid_season:
                bad_records.append(records)
                return(len(bad_records), "Bad Records found in season")
            else:
                continue

        def validate_QR(df):
            # logging.info("Validating QR Code")
            df_dict = df.to_dict('records')
            bad_records = []
            for each_records in df_dict:
                QRCode = each_records['QRCode']
                QRCode_split = QRCode_split()
                if not (len(QRCode.split) == 5):
                    bad_records.append(each_records)
                else:
                    for code in QRCode_split:
                        if not code.isnumeric():
                            bad_records.append(each_records)
                            break
            return bad_records
        bad_records = validate_QR(df)
        if bad_records:
            return (len(bad_records))
            for data in bad_records:
                return(data)
        else:
            pass
    return render_template('choosefile.html')

@app.route('/submit')
def submitData():
    def get_connection():
        return create_engine(
            url=conn_string
        )
    try:
        conn=get_connection()
        try:
            sql = "create database spss";
            conn.execute(sql)
        except:
            pass

        df = pd.read_csv(session['temporary_files'])
        year = (df['Year'])
        year = (year[0])
        season = (df['Season'])
        season = (season[0])
        tb_name = str(np.append(year, season))
        table_name = '    '
        table_name = tb_name.replace('    ','_')
        table_name = table_name.strip("[]{}")
        def get_connection():
            return create_engine(
                url = conn_string_spss
            )
        try:
            conn = get_connection()
            table_query = f"""
                                CREATE TABLE{table_name} (ID int AUTO_INCREMENT primary key,
                                Column_1 char(20) not null,
                                Column_2 char(20) not null,
                                Column_3 char(20) not null,
                                Column_4 char(20) not null,
                                Column_5 char(20) not null,
                                Column_6 int not null,
                                Column_7 int not null,
                                Column_8 char(20) not null,
                                Column_9 char(20) not null,
                                Column_10 char(20) UNIQUE not null,
                                Year int not null,
                                Season int not null,
                                Column_13 int not null
                                """
            conn.execute(table_query)
        except:
            pass

        for i in range(len(df)):
            try:
                df.iloc[i:i + 1].to_sql(name=table_name, if_exists='append', con=conn_string_spss, index=False)
            except:
                pass

                try:
                    query = f"ALTER TABLE {table_name} ADD IS_SCANED Char(10), ADD IS_SEQUENCED char(10), ADD IS_GROUPED char(10), " \
                            f"Scanned_ON DATETIME, ADD Sequenced_ON DATETIME, ADD Grouped_ON DATETIME, " \
                            f"ADD Scaned_BY char(10), ADD Sequenced_BY char(10), ADD Grouped_BY char(10)"
                    conn.execute(query)
                except:
                    pass

    except Exception as e:
        print(e.__str__())
        pass

    finally:
        if conn:
            try:
                conn.execute(
                    f" CREATE TABLE spss_entity_meta (UID int AUTO_INCREMENT primary key, foreign key(UID) REFERENCE{table_name}(ID), "
                    f" Name char(10) UNIQUE not null, "
                    f" Year int not null, "
                    f" Season ont not null", index=True)
            except:
                pass
            try:
                conn.execute(
                    f"""INSERT INTO spss_entity_meta (Name, Year, Season) Values ("{table_name}", {year}, {season}) """)
            except:
                pass

            conn.dispose()
            return render_template('choosefile_html')

@spp.route('/fetch', methods= ['POST', 'GET'])
def fetch():
    def get_connection():
                return create_engine(
                    url=conn_string_spss
                )
    try:
        conn = get_connection()
        table_name = request.form.to_dict()
        table_name = list(table_name.items())
        table_name = str(table_name)
        table_name = table_name.strip("[]()")
        table_name = table_name.strip("', '")
        print(table_name)
        df = pd.read_sql(f" SELECT * FROM {table_name}", con= conn_string_spss)
        records = df.to_html()
        return render_template("choosefile.html", data_var_fetch=records)
    except mysql.connector.Error as e:
        print(" Error reading data from MYSQL table", e)

@app.route('/filter')
def filter():
    def get_connection():
        return create_engine(
            url=conn_string_spss
        )
    try:
        conn = get_connection()
        year = input("Enter Year: ")
        season = input("Enter Season: ")
        tb_name = str(np.append(year, season))
        table_name = tb_name.replace(' ','_')
        table_name = table_name.strip("[]{}")
        table_name = table_name.strip("''")
        table_name = table_name.replace("'_'", "_")
        df = pd.read_sql(f"""SELECT * FROM {table_name}
        WHERE {input("Enter Condition IS_SCANED/IS_SEQUENCED/IS_GROUPED:")} {input("Enter IS NULL/IS NOT NULL: ")}""", conn_string_spss)
        records = df.to_html()
        return render_template("Choosefile.html", data_var_filter=records)
    except mysql.connector.Error as e:
        print("Error reading data from MYSQL Table", e)

if __name__ == '__main__':
    app.run(debug=True)

