#core_relation.global min dict, core sizes, 
#new : groupby flag, group by cols
import pandas as pd 
import copy
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
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
        for join_keys in global_join_graph:
            if attrib in join_keys:
                tuple_with_attrib = copy.deepcopy(attrib)
        #insert temp_val in referenced tables except attrib
        for referenced_attribs in tuple_with_attrib:
            if(referenced_attribs != attrib):
                res = self.connectionHelper.execute_sql_fetchall("select table_name from information_schema.columns where column_name = '" + referenced_attribs + "';")
                referenced_tables = list(res)
                # to get row containing og_val in referenced_tables
                for tables in referenced_tables:
                    new_row = self.connectionHelper.execute_sql_fetchone_0("select * from " + tables + " where " + attrib + " = " + og_val + ";")
                    #insert with updated value in tables
                    col_idx = self.connectionHelper.execute_sql_fetchone_0(get_col_idx(tables,referenced_attribs))
                    new_row[col_idx-1] = temp_val
                    self.connectionHelper.execute_sql([insert_row(tables,tuple(new_row))])

    def cascade_delete(self,attrib,global_join_graph,temp_val,og_val):
        for join_keys in global_join_graph:
            if attrib in join_keys:
                tuple_with_attrib = copy.deepcopy(attrib)
        #insert temp_val in referenced tables except attrib
        for referenced_attribs in tuple_with_attrib:
            if(referenced_attribs != attrib):
                res = self.connectionHelper.execute_sql_fetchall("select table_name from information_schema.columns where column_name = '" + referenced_attribs + "';")
                referenced_tables = list(res)
                # to get row containing og_val in referenced_tables
                for tables in referenced_tables:
                    self.connectionHelper.execute_sql([delete_row(tables,temp_val)])
                
    def insert_and_delete_extra_row(self,query,tabname,extra_row,attrib,temp_val,og_val):
        res = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
        print(f"Before Insert: {res}")
        #1.temp_val <--- inserted value of attribute attrib 
        #2.check whether attrib in JG(E)
        #3.return the tuple which contains atrrib
        #4.search the table that contains the column names in tuple 
        #5.query for getting 4 -->  select table_name from information_schema.columns where column_name = 'your_column_name'

        self.connectionHelper.execute_sql(
                            ["BEGIN;",insert_row(tabname,tuple(extra_row))])
        res1 = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
        print(f"After Insert: {res1}")
        new_result = self.app.doJob(query)
        print(f"New Res: {new_result}")
        if attrib in self.global_join_graph:
            self.cascade_insert(attrib,self.global_join_graph,temp_val,og_val)
        #print(f"gb: {new_result}")
        #size = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
        #print(f" res:{res} des:{des}")
        if(len(new_result)== 3):
            self.group_by_attrib.append(attrib)
            self.has_groupby = True
            print(f"GB inter: {self.group_by_attrib}")
            print(f"New Res1: {new_result}")
        self.connectionHelper.execute_sql(
                [delete_row(tabname,temp_val,attrib)])
        if attrib in self.global_join_graph:
            self.cascade_delete(attrib,self.global_join_graph,temp_val,og_val)
        res2 = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
        print(f"After Delete: {res2}")
    
    def int_increment(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        original_val = attrib_list[0]
        temp_val = int(attrib_list[0]+1)
        extra_row = copy.deepcopy(row1.values)
        for i, item in enumerate(extra_row):
            if isinstance(item, datetime.date):
                extra_row[i] = str(item)
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
                    #print(col_idx)
        extra_row[col_idx]= temp_val
        return extra_row,temp_val,col_idx,original_val
    
    def int_decrement(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        og_val = attrib_list[0]
        temp_val = int(attrib_list[0]-1)
        extra_row = copy.deepcopy(row1.values)
        for i, item in enumerate(extra_row):
            if isinstance(item, datetime.date):
                extra_row[i] = str(item)
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
                    #print(col_idx)
        extra_row[col_idx]= temp_val
        return extra_row,temp_val,col_idx,og_val
    
    def date_increment(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        print(f"og Date: {attrib_list[0]}")
        og_val = attrib_list[0]
        temp_val = attrib_list[0]+datetime.timedelta(days=1)
        print(f"tv in fn: {temp_val}")
        print(f"type date: {type(temp_val)}")
        extra_row = copy.deepcopy(row1.values)
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
        extra_row[col_idx]= temp_val
        for i, item in enumerate(extra_row):
            if isinstance(item, datetime.date):
                extra_row[i] = str(item)
                    #print(col_idx)
        return extra_row,temp_val,col_idx,og_val
    

    def date_decrement(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        print(f"og Date: {attrib_list[0]}")
        og_val = attrib_list[0]
        temp_val = attrib_list[0]-datetime.timedelta(days=1)
        print(f"tv in fn: {temp_val}")
        print(f"type date: {type(temp_val)}")
        extra_row = copy.deepcopy(row1.values)
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
        extra_row[col_idx]= temp_val
        for i, item in enumerate(extra_row):
            if isinstance(item, datetime.date):
                extra_row[i] = str(item)
                    #print(col_idx)
        return extra_row,temp_val,col_idx,og_val

    def doExtractJob1(self,query):
        local_attrib_dict = self.generateDict(self.global_min_instance_dict)
        #print(f"Local: {local_attrib_dict.keys()}")
        for tabname in local_attrib_dict:
            #col_idx=0;
            row1 = copy.deepcopy(local_attrib_dict[tabname].iloc[0])
            for attrib,vals in local_attrib_dict[tabname].items():
                attrib_list = (vals.values).tolist()
                #print(f'{attrib_list[0]} : {type(attrib_list[0])}')
                if(type(attrib_list[0])==int and self.checkWhetherAllSame(attrib_list)):
                    extra_row,temp_val,col_idx,og_val= self.int_increment(row1,attrib_list,local_attrib_dict,attrib,tabname)
                    try:
                        self.insert_and_delete_extra_row(query,tabname,extra_row,attrib,temp_val,og_val)
                        #extra_row[col_idx-1]=temp_val-1
                    except Exception as error:
                        print("Error Occurred in  Group By Integer. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
                elif(type(attrib_list[0])==date and self.checkWhetherAllSame(attrib_list)):
                    extra_row,temp_val,col_idx,og_val = self.date_increment(row1,attrib_list,local_attrib_dict,attrib,tabname)
                    print(f"date: {temp_val}type date1: {type(temp_val)}")
                    temp_val.strftime("%Y-%d-%m")
                    print(f"date: {temp_val}type date1: {type(temp_val)}")
                    
                    try:
                        self.insert_and_delete_extra_row(query,tabname,extra_row,attrib,str(temp_val),og_val)
                        extra_row[col_idx-1]
                    except Exception as error:
                        print("Error Occurred in  Group By Date. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
        return self.has_groupby
    
    def doExtractJob2(self,query):
        local_attrib_dict = self.generateDict(self.global_min_instance_dict)
        #print(f"Local: {local_attrib_dict.keys()}")
        for tabname in local_attrib_dict:
            #col_idx=0;
            row1 = copy.deepcopy(local_attrib_dict[tabname].iloc[0])
            for attrib,vals in local_attrib_dict[tabname].items():
                attrib_list = (vals.values).tolist()
                #print(f'{attrib_list[0]} : {type(attrib_list[0])}')
                if(type(attrib_list[0])==int and self.checkWhetherAllSame(attrib_list)):
                    extra_row,temp_val,col_idx,og_val = self.int_decrement(row1,attrib_list,local_attrib_dict,attrib,tabname)
                    try:
                        self.insert_and_delete_extra_row(query,tabname,extra_row,attrib,temp_val,og_val)
                        #extra_row[col_idx-1]=temp_val-1
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
                        extra_row[col_idx-1]
                    except Exception as error:
                        print("Error Occurred in  Group By Date. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
        return self.has_groupby


                    
                    
                         

                        
                
                    
                        