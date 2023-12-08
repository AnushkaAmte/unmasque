#core_relation.global min dict, core sizes, 
#new : groupby flag, group by cols
import pandas as pd 
import copy
import datetime
from decimal import *
from datetime import date
from ..refactored.abstract.GroupByBase import GroupByBase
from ..refactored.util.common_queries import get_row_count, alter_table_rename_to, get_min_max_ctid, \
    drop_view, drop_table, create_table_as_select_star_from, get_ctid_from, get_tabname_1, \
    create_view_as_select_star_where_ctid, create_table_as_select_star_from_ctid, get_tabname_6, get_star, \
    get_restore_name,get_freq,delete_non_matching_rows,create_table_like,delete_non_matching_rows_str,\
    insert_row,delete_row,get_col_idx
from ..refactored.util.utils import isQ_result_empty


class ModifiedGroupBy(GroupByBase):
    def __init__(self, connectionHelper,
                 core_relations,global_min_instance_dict,global_join_graph):
        super().__init__(connectionHelper,"Group_By" ,core_relations,global_min_instance_dict,global_join_graph)

        self.has_groupby = False
        self.group_by_attrib = []
    
    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        flag1 = self.doExtractJob1(query)
        flag2 = self.doExtractJob2(query)
        self.group_by_attrib=list(set(self.group_by_attrib))
        print(f"GB {self.group_by_attrib}")
        return flag1 or flag2

    def generateDict(self,global_min_instance_dict):
        data=[]
        local_attrib_dict={}        #print(self.global_min_instance_dict)
        temp = copy.deepcopy(global_min_instance_dict)
        # print(temp  )
        for index,val in temp.items():
            print(index)
            cols =list(temp[index][0])
            #print(cols)
            del temp[index][0]
            data = temp[index]
            df = pd.DataFrame(data,columns=cols)
            #print(df)
            local_attrib_dict[index] =df
        return local_attrib_dict
      
    def checkWhetherAllSame(self,items):
        return all(x == items[0] for x in items) 
    
    def cascade_insert(self,attrib,global_join_graph,temp_val,og_val):
        tuple_with_attrib=[]
        for join_keys in global_join_graph:
            if attrib in join_keys:
                tuple_with_attrib = copy.deepcopy(join_keys)
        #insert temp_val in referenced tables except attrib
        for referenced_attribs in tuple_with_attrib:
            if(referenced_attribs != attrib):
                res = self.connectionHelper.execute_sql_fetchall("select table_name from information_schema.columns where column_name = '" + referenced_attribs + "';")
                inter_tables = list(res)
                modified_res=[]
                for sublist in inter_tables[0]:
                    for item in sublist:
                        modified_res.append(item)
                referenced_tables = []
                relations = self.core_relations
                for element in modified_res:
                    if element in relations:
                        referenced_tables.append(element)
                # to get row containing og_val in referenced_tables
                for tables in referenced_tables:
                    int_row = self.connectionHelper.execute_sql_fetchall(f"select * from  {tables} where {referenced_attribs} = '{og_val}';")
                    #insert with updated value in tables
                    new_row1= int_row[0]
                    new_row=list(new_row1[0])
                    col_idx = self.connectionHelper.execute_sql_fetchone_0(get_col_idx(tables,referenced_attribs))
                    new_row[col_idx-1] = temp_val
                    for i, item in enumerate(new_row):
                        if isinstance(item, datetime.date):
                            new_row[i] = str(item)
                        if isinstance(item,Decimal):
                            new_row[i] = float(item)
                    self.connectionHelper.execute_sql([insert_row(tables,tuple(new_row))])

    def cascade_delete(self,attrib,global_join_graph,temp_val,og_val):
        print("in del")
        for join_keys in global_join_graph:
            if attrib in join_keys:
                tuple_with_attrib = copy.deepcopy(join_keys)
        #insert temp_val in referenced tables except attrib
        for referenced_attribs in tuple_with_attrib:
            if(referenced_attribs != attrib):
                res = self.connectionHelper.execute_sql_fetchall("select table_name from information_schema.columns where column_name = '" + referenced_attribs + "';")
                inter_tables = list(res)
                modified_res=[]
                for sublist in inter_tables[0]:
                    for item in sublist:
                        modified_res.append(item)
                referenced_tables = []
                relations = self.core_relations
                for element in modified_res:
                    if element in relations:
                        referenced_tables.append(element)
                for tables in referenced_tables:
                    self.connectionHelper.execute_sql([delete_row(tables,temp_val,referenced_attribs)])
                
    def insert_and_delete_extra_row(self,query,tabname,extra_row,attrib,temp_val,og_val):
        #res = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
        #print(f"Before Insert: {res}")
        for row in extra_row:
            self.connectionHelper.execute_sql(
                            ["BEGIN;",insert_row(tabname,tuple(row))])
        
        #self.connectionHelper.execute_sql(
        #                    ["BEGIN;",insert_row(tabname,tuple(extra_row))])
        #res1 = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
        #print(f"After Insert: {res1}")
        if any(attrib in sublist for sublist in self.global_join_graph):
            self.cascade_insert(attrib,self.global_join_graph,temp_val,og_val)
        new_result = self.app.doJob(query)
        if(len(new_result)== 3):
            self.group_by_attrib.append(attrib)
            self.has_groupby = True
            print(f"GB inter: {self.group_by_attrib}")
            print(f"New Res1: {new_result}")
        
        self.connectionHelper.execute_sql(
                [delete_row(tabname,temp_val,attrib)])
        for join_keys in self.global_join_graph:
            if attrib in join_keys:
                self.cascade_delete(attrib,self.global_join_graph,temp_val,og_val)
        #res2 = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
        #print(f"After Delete: {res2}")
    
    def int_increment(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        original_val = attrib_list[0]
        temp_val = int(attrib_list[0]+1)
        extra_row = copy.deepcopy(row1.values)
        print(f"er: {extra_row}")
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
        for row in extra_row:
            row[col_idx] = temp_val
            for i, item in enumerate(row):
               if isinstance(item, datetime.date):
                   row[i] = str(item)
        #extra_row[col_idx]= temp_val
        #for i, item in enumerate(extra_row):
        #    if isinstance(item, datetime.date):
        #        extra_row[i] = str(item)
        print(f"er1: {extra_row}")
        return extra_row,temp_val,col_idx,original_val
    
    def int_decrement(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        og_val = attrib_list[0]
        temp_val = int(attrib_list[0]-1)
        extra_row = copy.deepcopy(row1.values)
        print(f"er: {extra_row}")
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
        for row in extra_row:
            row[col_idx] = temp_val
            for i, item in enumerate(row):
               if isinstance(item, datetime.date):
                   row[i] = str(item)
        #extra_row[col_idx]= temp_val
        #for i, item in enumerate(extra_row):
        #    if isinstance(item, datetime.date):
        #        extra_row[i] = str(item)
        print(f"er1: {extra_row}")
        return extra_row,temp_val,col_idx,og_val
    
    def date_increment(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        og_val = attrib_list[0]
        temp_val = attrib_list[0]+datetime.timedelta(days=1)
        extra_row = copy.deepcopy(row1.values)
        print(f"er: {extra_row}")
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
        for row in extra_row:
            row[col_idx] = temp_val
            for i, item in enumerate(row):
               if isinstance(item, datetime.date):
                   row[i] = str(item)
        #extra_row[col_idx]= temp_val
        #for i, item in enumerate(extra_row):
        #    if isinstance(item, datetime.date):
        #        extra_row[i] = str(item)
        print(f"er1: {extra_row}")
        return extra_row,temp_val,col_idx,og_val
    

    def date_decrement(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        og_val = attrib_list[0]
        temp_val = attrib_list[0]-datetime.timedelta(days=1)
        extra_row = copy.deepcopy(row1.values)
        print(f"er: {extra_row}")
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
        for row in extra_row:
            row[col_idx] = temp_val
            for i, item in enumerate(row):
               if isinstance(item, datetime.date):
                   row[i] = str(item)
        #extra_row[col_idx]= temp_val
        #for i, item in enumerate(extra_row):
        #    if isinstance(item, datetime.date):
        #        extra_row[i] = str(item)
        print(f"er1: {extra_row}")
        return extra_row,temp_val,col_idx,og_val

    def doExtractJob1(self,query):
        local_attrib_dict = self.generateDict(self.global_min_instance_dict)
        for tabname in local_attrib_dict:
            row1 = copy.deepcopy(local_attrib_dict[tabname])
            print(f"ecp {local_attrib_dict[tabname].values}")
            for attrib,vals in local_attrib_dict[tabname].items():
                attrib_list = (vals.values).tolist()
                print(f"attr ist :{attrib_list}")
                if(type(attrib_list[0])==int and self.checkWhetherAllSame(attrib_list)):
                    extra_row,temp_val,col_idx,og_val= self.int_increment(row1,attrib_list,local_attrib_dict,attrib,tabname)
                    try:
                        self.insert_and_delete_extra_row(query,tabname,extra_row,attrib,temp_val,og_val)
                    except Exception as error:
                        print("Error Occurred in  Group By Integer. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
                elif(type(attrib_list[0])==date and self.checkWhetherAllSame(attrib_list)):
                    extra_row,temp_val,col_idx,og_val = self.date_increment(row1,attrib_list,local_attrib_dict,attrib,tabname)
                    temp_val.strftime("%Y-%d-%m")
                    
                    try:
                        self.insert_and_delete_extra_row(query,tabname,extra_row,attrib,str(temp_val),og_val)
                    except Exception as error:
                        print("Error Occurred in  Group By Date. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
        return self.has_groupby
    
    def doExtractJob2(self,query):
        local_attrib_dict = self.generateDict(self.global_min_instance_dict)
        for tabname in local_attrib_dict:
            row1 = copy.deepcopy(local_attrib_dict[tabname])
            for attrib,vals in local_attrib_dict[tabname].items():
                attrib_list = (vals.values).tolist()
                if(type(attrib_list[0])==int and self.checkWhetherAllSame(attrib_list)):
                    extra_row,temp_val,col_idx,og_val = self.int_decrement(row1,attrib_list,local_attrib_dict,attrib,tabname)
                    try:
                        self.insert_and_delete_extra_row(query,tabname,extra_row,attrib,temp_val,og_val)
                    except Exception as error:
                        print("Error Occurred in  Group By Integer. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
                elif(type(attrib_list[0])==date):
                    extra_row,temp_val,col_idx,og_val = self.date_decrement(row1,attrib_list,local_attrib_dict,attrib,tabname)
                    print(f"date: {temp_val}type date1: {type(temp_val)}")
                    temp_val.strftime("%Y-%d-%m")
                    print(f"date: {temp_val}type date1: {type(temp_val)}")
                    
                    try:
                        self.insert_and_delete_extra_row(query,tabname,extra_row,attrib,str(temp_val),og_val)
                    except Exception as error:
                        print("Error Occurred in  Group By Date. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
        return self.has_groupby


                    
                    
                         

                        
                
                    
                        