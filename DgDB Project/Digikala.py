import xlrd
import json
import re

import mysql.connector as sql
import pandas as pd
from trans import transes

DATA_FILE_PATH = r'C:\Users\Crytron\Desktop\Database\5-awte8wbd.xlsx'


# NaN-Checker
def isNaN(input_str):
    return input_str == input_str


# list translator
def trans(input_list=[]):
    tmp = []
    for item in input_list:
        s = transes[item]
        tmp.append(s)
    return tmp


# dict translator
def dictrans(input_list=[]):
    tmp = []
    for item in input_list:
        tmp2 = list(item.keys())
        tmp3 = trans(tmp2)
        for i, j in zip(tmp3, range(0, len(tmp2))):
            item[i] = item.pop(tmp2[j])
        tmp.append(item)
    return tmp


# splits the given strings based on certain delimiters
def splitr(input_list):
    tot_splist = []
    for alt in input_list:
        if isNaN(alt):
            splist = list(re.split(',|#|-|/|،', alt))
            tot_splist.append(splist)
    return tot_splist


# imports proper columns from excel files and store them in a list
def col_import(col_num):
    col = product_list[product_list.columns[col_num]]
    list = col.values.tolist()
    return list


# extracts proper rows from a column and store them in a list
def row_extr(name):
    result = []
    for i in range(0, len(cat_title_list)):
        if cat_title_list[i] == name:
            result.append(i + 2)
    return result


# creates python dictionary that stores all the attributes of each product
def dict_maker(attrlist, titlelist, brandlist):
    lst = []
    for attr, pr_titlefa, pr_brandfa in zip(attrlist, titlelist, brandlist):
        dict = {}
        dict["product_title_fa"] = pr_titlefa
        dict["brand_name_fa"] = pr_brandfa
        for each_attr in attr:
            if isinstance(each_attr, str):
                continue
            if "Value" in each_attr:
                dict[each_attr["Key"]] = each_attr["Value"]
            else:
                dict[each_attr["Key"]] = "NULL"
        lst.append(dict)
    return lst


# loads data of certain columns from excel file into a list (normal data types)
def norm_store(indexlist=[], altlist=[]):
    s_altlist = []
    for index in indexlist:
        if isNaN(altlist[index - 2]) == False:
            s_altlist.append('NULL')
        else:
            s_altlist.append(altlist[index - 2])
    return s_altlist


# loads attributes from excel file into a list (JSON)
def attr_store(indexlist, attrlist):
    s_attrlist = []
    for index in indexlist:
        if isNaN(attrlist[index - 2]) == False:
            s_attrlist.append('NULL')
        else:
            s_attrlist.append(json.loads(attrlist[index - 2]))
    return s_attrlist

# create table in our database
def tab_creatr(table_names, table_columns, fk_list, code):
    if code == 'main':
        for name, column in zip(table_names, table_columns):
            sql_query = 'CREATE TABLE  IF NOT EXISTS {} ( id INT NOT NULL,' + '`{}` TEXT, ' * len(column) \
                        + ' PRIMARY KEY (id))'
            sql = sql_query.format(name, *column)
            cur.execute(sql)
    elif code == 'url' or code == 'cat':
        for name, column, fk in zip(table_names, table_columns, fk_list):
            sql_query = 'CREATE TABLE  IF NOT EXISTS {} ( id INT NOT NULL,' + '`{}` TEXT, ' * len(column) \
                        + ' PRIMARY KEY (id), FOREIGN KEY (id) REFERENCES ' + fk + '(id))'
            sql = sql_query.format(name, *column)
            cur.execute(sql)
    else:
        for name, column, fk in zip(table_names, table_columns, fk_list):
            sql_query = 'CREATE TABLE  IF NOT EXISTS {} (pid INT NOT NULL AUTO_INCREMENT, id INT DEFAULT NULL,' + '`{}` TEXT, ' * len(column) \
                        + ' PRIMARY KEY (pid), FOREIGN KEY (id) REFERENCES ' + fk + '(id))'
            sql = sql_query.format(name, *column)
            cur.execute(sql)


# inserts each attribute into its proper column
def attr_insrt(table_data, tab_nam, pr_id):
    for list_item, id in zip(table_data, pr_id):
        columns = ''
        values = []
        values.append(id)
        for k, v in list_item.items():
            columns += ", " + '`' + str(k) + '`'
            values.append(str(v))
        fmt = "%s," * len(values)
        sql = "INSERT INTO " + tab_nam + "(`id`" + columns + ") VALUES (" + fmt[:-1] + ")"
        cur.execute(sql, values)


# adds row_id and inserts each title_alt into its proper row
def alt_insrt(s_altlist, pr_id, tab_name, colname):
    for index in range(0, len(s_altlist)):
        s_altlist[index].append(pr_id[index])

    for i in range(0, len(s_altlist)):
        values = []
        for j in range(0, len(s_altlist[i]) - 1):
            values = [s_altlist[i][-1], s_altlist[i][j]]
            sql = "INSERT INTO " + tab_name + " (id, " + colname + ") VALUES (%s, %s)"
            cur.execute(sql, values)

def urlcat_insrt(pr_id, pr_col, tab_name, colname):
    col_txt = 'id, ' + colname
    for index, value in zip(pr_id, pr_col[0]):
        values = [index, value]
        sql = 'INSERT INTO ' + tab_name + '(' + col_txt + ') VALUES (%s, %s)'
        cur.execute(sql, values)

# inserts into main_table columns
def main_insrt(pr_list, pr_all, tab_name):
    col_txt = 'id, product_title_en, category_title_fa, brand_name_en'
    for index in range(0, len(pr_list)):
        values = []
        for a_list in pr_all:
            values.append(str(a_list[index]))
        fmt = "%s, " * len(pr_all)
        sql = 'INSERT INTO ' + tab_name + '(' + col_txt + ') VALUES (' + fmt[:-2] + ')'
        cur.execute(sql, values)


# creates required lists to create "product"_main tables
def main_mkr(pr_list, pr_id):
    pr_title_en = norm_store(pr_list, pr_title_en_list)
    pr_cat_list = norm_store(pr_list, cat_title_list)
    pr_brand_en = norm_store(pr_list, brand_name_en_list)
    pr_all = [pr_id, pr_title_en, pr_cat_list, pr_brand_en]
    return pr_all

def url_mkr(pr_list, pr_id):
    pr_url = norm_store(pr_list, url_code_list)
    pr_all = [pr_id, pr_url]
    return pr_all

def cat_mkr(pr_list, pr_id):
    pr_cat_list = norm_store(pr_list, url_code_list)
    pr_all = [pr_id, pr_cat_list]
    return pr_all


# connects to sql and creates a database
def start_db(host, user, passwd, db_name):
    conn = sql.connect(
        host="localhost",
        user="admin",
        passwd="obsidian9513406",
    )
    cur = conn.cursor()
    cur.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    cur.execute(f'USE {db_name}')
    return conn, cur


# shuts down the connection
def shutdown_db(conn, cursor):
    cur.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    conn, cur = start_db('localhost', 'admin', 'obsidian9513406', 'DG_Project')
    product_list = pd.read_excel(DATA_FILE_PATH)

    # importing pre-equired columns
    cat_title_list = col_import(5)
    product_id = col_import(0)
    title_alt_list = col_import(4)

    # from excel rows to proper lists
    puzzle_list = row_extr("پازل")
    book_list = row_extr("کتاب چاپی")
    mouse_list = row_extr("ماوس (موشواره)")
    phonecover_list = row_extr("محافظ صفحه نمایش گوشی")
    keyboard_list = row_extr("کیبورد (صفحه کلید)")
    phonecase_list = row_extr("کیف و کاور گوشی")
    pr_lists_holder = [book_list, puzzle_list, keyboard_list, mouse_list, phonecover_list, phonecase_list]

    # extracting each products id separated based on product type
    book_id = norm_store(book_list, product_id)
    puzzle_id = norm_store(puzzle_list, product_id)
    keyboard_id = norm_store(keyboard_list, product_id)
    mouse_id = norm_store(mouse_list, product_id)
    cover_id = norm_store(phonecover_list, product_id)
    case_id = norm_store(phonecase_list, product_id)
    pr_id_holder = [book_id, puzzle_id, keyboard_id, mouse_id, cover_id, case_id]

    # table names
    table_attr_names = ["book_attributes", "puzzle_attributes", "keyboard_attributes", "mouse_attributes", "cover_attributes", "case_attributes"]
    table_alt_names = ["book_title_alt", "puzzle_title_alt", "keyboard_title_alt", "mouse_title_alt", "cover_title_alt", "case_title_alt"]
    table_main_names = ["book_main", "puzzle_main", "keyboard_main", "mouse_main", "cover_main", "case_main"]
    table_url_names = ["book_url", "puzzle_url", "keyboard_url", "mouse_url", "cover_url", "case_url"]
    table_cat_names = ["book_catkey", "puzzle_catkey", "keyboard_catkey", "mouse_catkey", "cover_catkey", "case_catkey"]

# CREATING "PRODUCT"_MAIN TABLES:

    # importing required columns
    pr_title_fa_list = col_import(1)
    pr_title_en_list = col_import(2)
    url_code_list = col_import(3)
    category_keyword_list = col_import(6)
    brand_name_fa_list = col_import(7)
    brand_name_en_list = col_import(8)

    # creating "product"_main tables
    table_main_c = ["product_title_en", "category_title_fa", "brand_name_en"]
    table_main_columns = [table_main_c, table_main_c, table_main_c, table_main_c, table_main_c, table_main_c]  # all the tables have a seven columns with same names!
    fk_list =[]

    tab_creatr(table_main_names, table_main_columns, fk_list, "main")

    # extracting columns separated based on product type
    book_all = main_mkr(book_list, book_id)
    puzzle_all = main_mkr(puzzle_list, puzzle_id)
    keyboard_all = main_mkr(keyboard_list, keyboard_id)
    mouse_all = main_mkr(mouse_list, mouse_id)
    cover_all = main_mkr(phonecover_list, cover_id)
    case_all = main_mkr(phonecase_list, case_id)
    pr_all_holder = [book_all, puzzle_all, keyboard_all, mouse_all, cover_all, case_all]

    # inserting into "product'_main columns
    for pr_list, pr_all, tab_name in zip(pr_lists_holder, pr_all_holder, table_main_names):
        main_insrt(pr_list, pr_all, tab_name)

# CREATING "PRODUCT"_ATTRIBUTES TABLES:

    # importing required columns
    product_attr_list = col_import(9)

    # converting the attributes from JSONs stored in the excel file to Python objects in proper attr_lists
    puzzle_attrlist = attr_store(puzzle_list, product_attr_list)
    book_attrlist = attr_store(book_list, product_attr_list)
    keyboard_attrlist = attr_store(keyboard_list, product_attr_list)
    mouse_attrlist = attr_store(mouse_list, product_attr_list)
    cover_attrlist = attr_store(phonecover_list, product_attr_list)
    case_attrlist = attr_store(phonecase_list, product_attr_list)

    # extracting each products required column separated based on product type
    book_titlelist = norm_store(book_list, pr_title_fa_list)
    puzzle_titlelist = norm_store(puzzle_list, pr_title_fa_list)
    keyboard_titlelist = norm_store(keyboard_list, pr_title_fa_list)
    mouse_titlelist = norm_store(mouse_list, pr_title_fa_list)
    cover_titlelist = norm_store(phonecover_list, pr_title_fa_list)
    case_titlelist = norm_store(phonecase_list, pr_title_fa_list)

    book_brandlist = norm_store(book_list, brand_name_fa_list)
    puzzle_brandlist = norm_store(puzzle_list, brand_name_fa_list)
    keyboard_brandlist = norm_store(keyboard_list, brand_name_fa_list)
    mouse_brandlist = norm_store(mouse_list, brand_name_fa_list)
    cover_brandlist = norm_store(phonecover_list, brand_name_fa_list)
    case_brandlist = norm_store(phonecase_list, brand_name_fa_list)

    # converting lists to proper dicts and translating their keys
    book = dictrans(dict_maker(book_attrlist, book_titlelist, book_brandlist))
    puzzle = dictrans(dict_maker(puzzle_attrlist, puzzle_titlelist, puzzle_brandlist))
    keyboard = dictrans(dict_maker(keyboard_attrlist, keyboard_titlelist, keyboard_brandlist))
    mouse = dictrans(dict_maker(mouse_attrlist, mouse_titlelist, mouse_brandlist))
    cover = dictrans(dict_maker(cover_attrlist, cover_titlelist, cover_brandlist))
    case = dictrans(dict_maker(case_attrlist, case_titlelist, case_brandlist))

    # storing keys of each product in a list and translate that
    book_keys = book[0].keys()
    puzzle_keys = puzzle[0].keys()
    keyboard_keys = keyboard[0].keys()
    mouse_keys = mouse[0].keys()
    cover_keys = cover[0].keys()
    case_keys = case[1].keys()

    # creating "product"_attribute tables
    table_attr_columns = [book_keys, puzzle_keys, keyboard_keys, mouse_keys, cover_keys, case_keys]
    table_product = [book, puzzle, keyboard, mouse, cover, case]

    tab_creatr(table_attr_names, table_attr_columns, table_main_names, "")

    # inserting values into columns
    for product, name, id in zip(table_product, table_attr_names, pr_id_holder):
        attr_insrt(product, name, id)

# CREATING "PRODUCT"_TITLE_ALT TABLES:

    # creating "product"_title_alt tables
    title_alt_c = ["title_alt"]
    table_alt_columns = [title_alt_c, title_alt_c, title_alt_c, title_alt_c, title_alt_c, title_alt_c]  # all the tables have a single column with a same name!

    tab_creatr(table_alt_names, table_alt_columns, table_main_names, "")

    # extracting title_alt separated based on product type
    puzzle_altlist = splitr(norm_store(puzzle_list, title_alt_list))
    book_altlist = splitr(norm_store(book_list, title_alt_list))
    keyboard_altlist = splitr(norm_store(keyboard_list, title_alt_list))
    mouse_altlist = splitr(norm_store(mouse_list, title_alt_list))
    cover_altlist = splitr(norm_store(phonecover_list, title_alt_list))
    case_altlist = splitr(norm_store(phonecase_list, title_alt_list))

    # inserting into "product"_title_alt tables
    all_altlist = [book_altlist, puzzle_altlist, keyboard_altlist, mouse_altlist, cover_altlist, case_altlist]

    for s_altlist, pr_id, tab_name in zip(all_altlist, pr_id_holder, table_alt_names):
        alt_insrt(s_altlist, pr_id, tab_name, "title_alt")


# CREATING "PRODUCT"_URL TABLES:
    pr_url_c = ["url_code"]
    table_url_columns = [pr_url_c, pr_url_c, pr_url_c, pr_url_c, pr_url_c, pr_url_c]

    tab_creatr(table_url_names, table_url_columns, table_main_names,"url")

    # extracting url_code separated based on product type
    book_urllist = norm_store(book_list, url_code_list)
    puzzle_urllist = norm_store(puzzle_list, url_code_list)
    keyboard_urllist = norm_store(keyboard_list, url_code_list)
    mouse_urllist = norm_store(mouse_list, url_code_list)
    cover_urllist = norm_store(phonecover_list, url_code_list)
    case_urllist = norm_store(phonecase_list, url_code_list)


    # inserting into "product"_url tables
    urllist = [book_urllist, puzzle_urllist, keyboard_urllist, mouse_urllist, cover_urllist, case_urllist]
    all_urllist = []
    all_urllist.append(urllist)

    for pr_id, pr_all, tab_name in zip(pr_id_holder, all_urllist, table_url_names):
        urlcat_insrt(pr_id, pr_all, tab_name, "url_code")

# CREATING "PRODUCT"_CATKEYS TABLES:
    pr_cat_c = ["category_keywords"]
    table_cat_columns = [pr_cat_c, pr_cat_c, pr_cat_c, pr_cat_c, pr_cat_c, pr_cat_c]

    tab_creatr(table_cat_names, table_cat_columns, table_main_names,"cat")

    # extracting url_code separated based on product type
    book_catlist = norm_store(book_list, category_keyword_list)
    puzzle_catlist = norm_store(puzzle_list, category_keyword_list)
    keyboard_catlist = norm_store(keyboard_list, category_keyword_list)
    mouse_catlist = norm_store(mouse_list, category_keyword_list)
    cover_catlist = norm_store(phonecover_list, category_keyword_list)
    case_catlist = norm_store(phonecase_list, category_keyword_list)

    # inserting into "product"_url tables
    catlist = [book_catlist, puzzle_catlist, keyboard_catlist, mouse_catlist, cover_catlist, case_catlist]
    all_catlist = []
    all_catlist.append(catlist)

    for pr_id, pr_all, tab_name in zip(pr_id_holder, all_catlist, table_cat_names):
        urlcat_insrt(pr_id, pr_all, tab_name, "category_keywords")

    shutdown_db(conn, cur)