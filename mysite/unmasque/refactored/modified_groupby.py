#core_relation.global min dict, core sizes, 
#new : groupby flag, group by cols
import pandas as pd 
import copy
import datetime
from datetime import date
from ..refactored.abstract.GroupByBase import GroupByBase
from ..refactored.util.common_queries import get_row_count, alter_table_rename_to, get_min_max_ctid, \
    drop_view, drop_table, create_table_as_select_star_from, get_ctid_from, get_tabname_1, \
    create_view_as_select_star_where_ctid, create_table_as_select_star_from_ctid, get_tabname_6, get_star, \
    get_restore_name,get_freq,delete_non_matching_rows,create_table_like,delete_non_matching_rows_str,\
    insert_row,delete_row
from ..refactored.util.utils import isQ_result_empty


class ModifiedGroupBy(GroupByBase):
    def __init__(self, connectionHelper,
                 core_relations,global_min_instance_dict):
        super().__init__(connectionHelper,"Group_By" ,core_relations,global_min_instance_dict)

        self.has_groupby = False
        self.group_by_attrib = []
    
    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        flag1 = self.doExtractJob1(query)
        #if not flag1:
        #    flag2 = self.doExtractJob2(query)
        self.group_by_attrib=list(set(self.group_by_attrib))
        print(f"GB {self.group_by_attrib}")
        return flag1 

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
      
    def checkWhetherAllSame(items):
        return all(x == items[0] for x in items) 
    
    def insert_and_delete_extra_row(self,query,tabname,extra_row,attrib,temp_val):
        #res = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
        #print(f"Before Insert: {res}")
        self.connectionHelper.execute_sql(
                            ["BEGIN;",insert_row(tabname,tuple(extra_row))])
        #res1 = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
        #print(f"After Insert: {res1}")
        new_result = self.app.doJob(query)
        #print(f"gb: {new_result}")
        size = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
        #print(f" res:{res} des:{des}")
        if(size== 2):
            self.group_by_attrib.append(attrib)
            self.has_groupby = True
        self.connectionHelper.execute_sql(
                [delete_row(tabname,temp_val,attrib)])
        #res2 = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
        #print(f"After Delete: {res2}")
    
    def int_increment(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        temp_val = int(attrib_list[0]+1)
        extra_row = copy.deepcopy(row1.values)
        for i, item in enumerate(extra_row):
            if isinstance(item, datetime.date):
                extra_row[i] = str(item)
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
                    #print(col_idx)
        extra_row[col_idx]= temp_val
        return extra_row,temp_val,col_idx
    
    def date_increment(self,row1,attrib_list,local_attrib_dict,attrib,tabname):
        temp_val = attrib_list[0]+datetime.timedelta(days=1)
        print(f"type date: {type(temp_val)}")
        extra_row = copy.deepcopy(row1.values)
        col_idx = local_attrib_dict[tabname].columns.get_loc(attrib)
        extra_row[col_idx]= temp_val
        for i, item in enumerate(extra_row):
            if isinstance(item, datetime.date):
                extra_row[i] = str(item)
                    #print(col_idx)
        return extra_row,temp_val,col_idx

    def doExtractJob1(self,query):
        local_attrib_dict = self.generateDict(self.global_min_instance_dict)
        #print(f"Local: {local_attrib_dict.keys()}")
        for tabname in local_attrib_dict:
            #col_idx=0;
            row1 = copy.deepcopy(local_attrib_dict[tabname].iloc[0])
            for attrib,vals in local_attrib_dict[tabname].items():
                attrib_list = (vals.values).tolist()
                #print(f'{attrib_list[0]} : {type(attrib_list[0])}')
                if(type(attrib_list[0])==int):
                    extra_row,temp_val,col_idx = self.int_increment(row1,attrib_list,local_attrib_dict,attrib,tabname)
                    try:
                        self.insert_and_delete_extra_row(query,tabname,extra_row,attrib,temp_val)
                        #extra_row[col_idx-1]=temp_val-1
                    except Exception as error:
                        print("Error Occurred in  Group By Integer. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
                #elif(type(attrib_list[0])==date):
                    #extra_row,temp_val,col_idx = self.date_increment(row1,attrib_list,local_attrib_dict,attrib,tabname)
                    #temp_val.strftime('%Y-%d-%m')
                    #print(f"date: {temp_val}type date1: {type(temp_val)}")
                    
                    #try:
                        #self.insert_and_delete_extra_row(query,tabname,extra_row,attrib,str(temp_val))
                        #extra_row[col_idx-1]
                    #except Exception as error:
                        #print("Error Occurred in  Group By Date. Error: " + str(error))
                        #self.connectionHelper.execute_sql(["ROLLBACK;"])
                        #exit(1)
        return self.has_groupby


                    
                    
                         

                        
                
                    
                        