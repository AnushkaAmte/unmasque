import copy

import pandas as pd
from .abstract.MinimizerBase import Minimizer
from ..refactored.util.common_queries import get_row_count, alter_table_rename_to, get_min_max_ctid, \
    drop_view, drop_table, create_table_as_select_star_from, get_ctid_from, get_tabname_4, \
    create_view_as_select_star_where_ctid, create_table_as_select_star_from_ctid, get_tabname_6, get_star, \
    get_restore_name,get_freq,delete_non_matching_rows,create_table_like,delete_non_matching_rows_str
from ..refactored.util.utils import isQ_result_empty

class NewMinimizer(Minimizer):


    def __init__(self, connectionHelper,
                 core_relations,core_sizes,sampling_status):
        super().__init__(connectionHelper, core_relations,core_sizes,"New_Minimizer")
        self.cs2_passed = sampling_status
        self.global_other_info_dict = {}
        self.global_result_dict = {}
        self.local_other_info_dict = {}
        self.global_min_instance_dict = {}
        #self.global_all_attribs =[]

    def extract_params_from_args(self, args):
        return args[0]
    
    def get_attribs(self,tabname):
        res = self.connectionHelper.execute_sql_fetchall("select column_name "
                                                                   "from "
                                                                   "information_schema.columns "
                                                                   "where table_schema = 'public' and "
                                                                   "table_name = '" + tabname + "';")
        tab_attribs = list(res)
        attribs= []
        for t in tab_attribs[0]:
            for item in t:
             attribs.append(item)
        return attribs
        

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        if self.cs2_passed:
            return self.frequency_counter(query,True)
        else:
            return self.frequency_counter(query,False)

    
    def populate_dict_info(self, query):
        # POPULATE MIN INSTANCE DICT
        for tabname in self.core_relations:
            self.global_min_instance_dict[tabname] = []
            sql_query = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
            df = pd.DataFrame(sql_query)
            self.global_min_instance_dict[tabname].append(tuple(df.columns))
            for index, row in df.iterrows():
                self.global_min_instance_dict[tabname].append(tuple(row))

        # populate other data
        new_result = self.app.doJob(query)
        print(f"nm pop:{new_result}")
        self.global_result_dict['min'] = copy.deepcopy(new_result)
        self.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
        self.global_other_info_dict['min'] = copy.deepcopy(self.local_other_info_dict)
        print(f"global_min_instance_dict: {self.global_min_instance_dict}")
    
    def sanity_check(self, query):
        # SANITY CHECK
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            print("Error: Query out of extractable domain\n")
            return False
        return True
    
    def frequency_counter(self, query,cs_pass):
        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            attrib_list = self.get_attribs(tabname)
            #print(attrib_list)
            try:
                self.connectionHelper.execute_sql([
                    alter_table_rename_to(tabname, tabname + "_backup"),
                    create_table_as_select_star_from(tabname, tabname + "_backup")
                ])
            except Exception as error:
                print("Error Occurred in  minimizer. Error: " + str(error))
                self.connectionHelper.execute_sql(["ROLLBACK;"])
                exit(1)

            for attrib in attrib_list:
                #row_count = get_row_count(tabname)
                #freq_dict = {};
                freq_count = pd.read_sql_query(get_freq(tabname,attrib),self.connectionHelper.conn)  
                df = pd.DataFrame(freq_count)
                #print(df)



                for i in range(len(df.index)):
                    # max_freq_count = df.iloc[i]['counter']
                    max_freq_val = df.iloc[i][attrib]
                    #check = isinstance(max_freq_val,str)
                    #print(f'max val: {max_freq_val}')
                    try:
                        self.connectionHelper.execute_sql(
                                    ["SAVEPOINT new_min;","BEGIN;", alter_table_rename_to(tabname,get_tabname_6(tabname)) , 
                                    create_table_as_select_star_from(tabname,get_tabname_6(tabname)),
                                        delete_non_matching_rows_str(tabname,attrib,max_freq_val),
                                        drop_table(get_tabname_6(tabname))])
                        #print(1)
                        size = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
                        #print(f'size : {size}')
                        result = self.app.doJob(query)
                        #print(f" nm: {result}")
                        if len(result)>1:
                            break
                        if len(result)<=1:
                            self.connectionHelper.execute_sql(["ROLLBACK TO SAVEPOINT new_min;"])
                            #print(result)


                    
                            

                    except Exception as error:
                        print("Error Occurred in  minimizer. Error: " + str(error))
                        self.connectionHelper.execute_sql(["ROLLBACK;"])
                        exit(1)

        # self.connectionHelper.execute_sql(["ROLLBACK;"])
        for tabname in self.core_relations:
            res, desc = self.connectionHelper.execute_sql_fetchall(get_star(tabname))
            self.connectionHelper.execute_sql([drop_table(get_tabname_4(tabname)),
                                               create_table_as_select_star_from(get_tabname_4(tabname), tabname)])
            #print(tabname, "==", res)
        self.populate_dict_info(query)   
        #if not self.sanity_check(query):
        #    print("FAIL")
        #    return False
        return True            

                    



                
                    

            
    

    