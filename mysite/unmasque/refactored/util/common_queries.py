def drop_table(tab):
    return "drop table if exists " + tab + ";"


def alter_table_rename_to(tab, retab):
    return "Alter table " + tab + " rename to " + retab + ";"


def create_table_like(tab, ctab):
    return "Create table " + tab + " (like " + ctab + ");"


def create_table_as_select_star_from(tab, fromtab):
    return "Create table " + tab + " as select * from " + fromtab + ";"


def get_row_count(tab):
    return "select count(*) from " + tab + ";"


def get_star(tab):
    return "select * from " + tab + ";"

def get_freq(tab,attrib):
    return "select count(*) as counter, " + attrib + " from " + tab + " group by " + attrib + " order by counter desc;" 

def delete_non_matching_rows(tab,attrib,val):
    return f"delete from {tab} where {attrib} != {val};"

def delete_non_matching_rows_str(tab,attrib,val):
    return f"delete from {tab} where {attrib} != '{val}';"

def get_col_idx(tabname,attrib):
    return f"select ordinal_position from information_schema.columns where table_schema = 'public' and table_name = {tabname} and column_name = '{attrib}';" 

def get_restore_name(tab):
    return tab + "_restore"


def get_min_max_ctid(tab):
    return "select min(ctid), max(ctid) from " + tab + ";"


def drop_view(tab):
    return "drop view " + tab + ";"


def get_tabname_1(tab):
    return tab + "1"


def get_tabname_4(tab):
    return tab + "4"

def get_tabname_6(tab):
    return tab + "6"

def get_tabname_un(tab):
    return tab + "_un"

def insert_row(tabname,value_list):
    return f"insert into {tabname} values {value_list};"

def delete_row(tabname,val,attrib):
    return f"delete from {tabname} where {attrib} = '{val}';"

def create_view_as_select_star_where_ctid(mid_ctid1, start_ctid, tabname, tabname1):
    return ("create view " + tabname
            + " as select * from " + tabname1
            + " where ctid >= '"
            + str(start_ctid) + "' and ctid <= '"
            + str(mid_ctid1) + "'  ; ")


def create_table_as_select_star_from_ctid(end_ctid, start_ctid, tabname, tabname1):
    return ("create table " + tabname
            + " as select * from " + tabname1
            + " where ctid >= '" + str(start_ctid)
            + "' and ctid <= '" + str(end_ctid) + "'  ; ")


def get_ctid_from(min_or_max, tabname):
    return "select " + min_or_max + "(ctid) from " + tabname + ";"
