#core_relation.global min dict, core sizes, 
#new : groupby flag, group by cols
import pandas as pd 
import copy
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
        flag2 = self.doExtractJob2(query)
        self.group_by_attrib=list(set(self.group_by_attrib))
        print(f"GB {self.group_by_attrib}")
        return flag1 or flag2

    def generateDict(self,global_min_instance_dict):
        data=[]
        #print(self.global_min_instance_dict)
        temp = copy.deepcopy(global_min_instance_dict)
        # print(temp  )
        for index in temp:
            #print(index)
            cols =list(temp[index][0])
            #print(cols)
            del temp[index][0]
            data = temp[index]
            df = pd.DataFrame(data,columns=cols)
            #print(df)
            local_attrib_dict = {index:df}
        return local_attrib_dict
      
    def checkWhetherAllSame(items):
        return all(x == items[0] for x in items) 
    
    def doExtractJob2(self,query):
        local_attrib_dict = self.generateDict(self.global_min_instance_dict)
        #row1 values
        for tabname in local_attrib_dict:#iterate over dataframe
            row1= local_attrib_dict[tabname].iloc[0]

            col_idx=0
            for attrib,values in local_attrib_dict[tabname].items():  #iterate over columns
                attrib_list = values.tolist()
                if(type(attrib_list[0])==int):
                    temp_val = attrib_list[0]-1
                    extra_row = row1
                   
                    extra_row[col_idx] = temp_val
                   
                    col_idx+=1
                    try:
                        self.connectionHelper.execute_sql(
                            ["BEGIN;",insert_row(tabname,tuple(extra_row))]
                        )
                        result = self.app.doJob(query)
                        size = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
                        print(size)
                        if(size == 2):
                            self.group_by_attrib.append(attrib)
                            self.has_groupby = True
                        self.connectionHelper.execute_sql(
                            [delete_row(tabname,temp_val,attrib)]
                        )
                        extra_row[col_idx-1]=temp_val+1
                    except Exception as error:
                        print("Error Occurred in  Group By. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
        #print("Checking..")
        #print(self.group_by_attrib)
        return self.has_groupby

    def doExtractJob1(self,query):
        local_attrib_dict = self.generateDict(self.global_min_instance_dict)
        #row1 values
        for tabname in local_attrib_dict:#iterate over dataframe
            row1= local_attrib_dict[tabname].iloc[0]

            col_idx=0
            for attrib,values in local_attrib_dict[tabname].items():  #iterate over columns
                attrib_list = values.tolist()
                if(type(attrib_list[0])==int):
                    temp_val = attrib_list[0]+1
                    extra_row = row1
                   
                    extra_row[col_idx] = temp_val
                   
                    col_idx+=1
                    try:
                        self.connectionHelper.execute_sql(
                            ["BEGIN;",insert_row(tabname,tuple(extra_row))]
                        )
                        result = self.app.doJob(query)
                        size = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
                        if(size == 2):
                            self.group_by_attrib.append(attrib)
                            self.has_groupby = True
                        self.connectionHelper.execute_sql(
                            [delete_row(tabname,temp_val,attrib)]
                        )
                        extra_row[col_idx-1]=temp_val-1
                    except Exception as error:
                        print("Error Occurred in  Group By. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)
        #print("Checking..")
        #print(self.group_by_attrib)
        return self.has_groupby

                    
                    
                         

                        
                
                    
                        