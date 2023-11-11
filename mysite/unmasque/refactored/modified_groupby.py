#core_relation.global min dict, core sizes, 
#new : groupby flag, group by cols
import pandas as pd 
import copy
from ..refactored.abstract.GroupByBase import GroupByBase
from ..refactored.util.common_queries import get_row_count, alter_table_rename_to, get_min_max_ctid, \
    drop_view, drop_table, create_table_as_select_star_from, get_ctid_from, get_tabname_1, \
    create_view_as_select_star_where_ctid, create_table_as_select_star_from_ctid, get_tabname_6, get_star, \
    get_restore_name,get_freq,delete_non_matching_rows,create_table_like,delete_non_matching_rows_str
from ..refactored.util.utils import isQ_result_empty


class ModifiedGroupBy(GroupByBase):
    def __init__(self, connectionHelper,
                 core_relations,global_min_instance_dict):
        super().__init__(connectionHelper, core_relations,global_min_instance_dict,"New_Minimizer")

        self.has_groupby = False
        self.group_by_attrib = []
    
    def generateDict(global_min_instance_dict):
        data=[]
        temp = global_min_instance_dict
        for index in temp:
            cols =list(temp[index][0])
            #print(cols)
            del temp[index][0]
            data = temp[index]
            df = pd.DataFrame(data,columns=cols)
            #print(df)
            local_attrib_dict = {index:df}
        return local_attrib_dict
      
    
    def doExtractJob(self,query,local_attrib_dict):
        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            #get attributes   local_attrib_dict[i] --> attributes of table i
            