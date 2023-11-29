import copy
import math
import datetime
from decimal import *
from ..refactored.util.common_queries import get_row_count, alter_table_rename_to, get_min_max_ctid, \
    drop_view, drop_table, create_table_as_select_star_from, get_ctid_from, get_tabname_4, \
    create_view_as_select_star_where_ctid, create_table_as_select_star_from_ctid, get_tabname_6, get_star, \
    get_restore_name,get_freq,delete_non_matching_rows,create_table_like,delete_non_matching_rows_str,get_max_val,\
    drop_column,compute_join,truncate_table,get_col_idx,insert_row,get_type,insert_col,flood_fill,sort_insert,update_n_rows
from .util.utils import isQ_result_empty, get_val_plus_delta, get_cast_value, \
    get_min_and_max_val, get_format, get_mid_val, is_left_less_than_right_by_cutoff
from .abstract.where_clause import WhereClause


class ModifiedFilter(WhereClause):

    def __init__(self, connectionHelper,
                 global_key_lists,
                 core_relations,
                 global_min_instance_dict,
                 global_key_attributes,
                 global_join_graph,
                 group_by_attrib):
        super().__init__(connectionHelper,
                         global_key_lists,
                         core_relations,
                         global_min_instance_dict,
                         group_by_attrib)
        self.filter_predicates = None
        self.global_key_attributes = global_key_attributes
        self.global_join_graph = global_join_graph

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        self.do_init()
        self.preprocess(query)
        self.filter_predicates = self.get_filter_predicates(query)
        return self.filter_predicates

    def get_index(self,value_list,attrib):
        for i,v in enumerate(value_list):
            if v == attrib:
                return i
            
    def compute_width(self,tabname):
        col_list = self.connectionHelper.execute_sql_fetchall(f"select count(*) from information_schema.columns WHERE table_name = '{tabname}6';")
        return col_list
    
    def preprocess(self,query):
        #do this for all core relations kindof
        #rn runs only once
        for original_table in self.core_relations:
            self.connectionHelper.execute_sql(
                    ["SAVEPOINT preprocess;","BEGIN;", alter_table_rename_to(original_table,get_tabname_6(original_table)) , 
                    create_table_like(original_table,get_tabname_6(original_table))])
        #attrib_list = self.global_key_attributes
        #print(f"glob key attr: {self.global_key_attributes}")
     
        tuple_with_attrib=[]
        for attrib in self.global_join_graph:
                tuple_with_attrib.append(attrib)
        #print(f"tup attr: {tuple_with_attrib}")
        referenced_tables = []
        for attrib_tuple in tuple_with_attrib:    
            ref_tab=[]
            for referenced_attribs in attrib_tuple:
                res = self.connectionHelper.execute_sql_fetchall("select table_name from information_schema.columns where column_name = '" + referenced_attribs + "';")
                inter_tables = list(res)
                modified_res=[]
                for sublist in inter_tables[0]:
                    for item in sublist:
                        modified_res.append(item)
                #print(f"inter rtab: {modified_res}")
                relations = self.core_relations
                for element in modified_res:
                    if element in relations:
                        ref_tab.append(element)
                        #print(f"ref tab: {ref_tab}")
            referenced_tables.append(ref_tab)
        #print(f"refer tab: {referenced_tables}")
        #print(f"core rel: {self.core_relations}"
        #join all the tables in referenced_tables and join on tuple_with_attrib
        join_result=[]
        size_list=[]
        index=[]
        for attr_tup,ref_tup in zip(tuple_with_attrib,referenced_tables):
            #ref_tup6 = [element + '6' for element in ref_tup]
            #print(ref_tup6)
            temp =self.connectionHelper.execute_sql_fetchall(compute_join(ref_tup,attr_tup))
            join_result.append(temp[0])
            widths=[]
            for tabname in ref_tup:
                temp1 = self.compute_width(tabname)
                widths.append(temp1[0][0][0])
            size_list.append(widths)
            col_idx=[]
            for tabname,attrib in zip(ref_tup,attr_tup):
                col_idx.append(self.connectionHelper.execute_sql_fetchone_0(get_col_idx(get_tabname_6(tabname),attrib)))
            index.append(col_idx)
        print(f"Join Result is {join_result}")
        print(f"size list:{size_list}")
        print(f"indexes :{index}")

        for i in range(len(join_result)):
           
            for res in join_result[i]:
                size = size_list[i]
                idx = index[i]
                
                temp_tab1 = res[:(size[0])]
                temp_tab2= res[size[0]:] 
                t_t1 = list(temp_tab1)
                t_t2 = list(temp_tab2)
                print(f"{t_t1} size={len(t_t1)}")
                print(f"{t_t2} size={len(t_t2)}")
                for j, item in enumerate(t_t1):
                    if isinstance(item, datetime.date):
                        t_t1[j] = str(item)
                    if isinstance(item, Decimal):
                        t_t1[j] = float(item)
                for k, item in enumerate(t_t2):
                    if isinstance(item, datetime.date):
                        t_t2[k] = str(item)
                    if isinstance(item, Decimal):
                        t_t2[k] = float(item)
                print(f"{t_t1} size={len(t_t1)}")
                print(f"{t_t2} size={len(t_t2)}")
               
                print(f"ref tab: {referenced_tables[i]}")
                self.connectionHelper.execute_sql(
                        ["BEGIN;",insert_row(referenced_tables[i][0],tuple(t_t1)),insert_row(referenced_tables[i][1],tuple(t_t2))])
        
        for attr_tup,ref_tup in zip(tuple_with_attrib,referenced_tables):
            for key_atrrib,original_table in zip(attr_tup,ref_tup):
               
                datatype = self.connectionHelper.execute_sql_fetchall(get_type(get_tabname_6(original_table),key_atrrib))
                print(f"att: {key_atrrib} type: {datatype[0][0][0]}")
                self.connectionHelper.execute_sql([flood_fill(original_table,key_atrrib)])
                ans = self.connectionHelper.execute_sql_fetchall(f"select * from {original_table}")
                print(f"attr : {key_atrrib} tab: {original_table}")
                print(ans[0])



    def get_filter_predicates(self, query):
        filter_attribs = []
        total_attribs = 0
        d_plus_value = copy.deepcopy(self.global_d_plus_value)
        attrib_max_length = copy.deepcopy(self.global_attrib_max_length)

        for entry in self.global_attrib_types:
            # aoa change
            self.global_attrib_types_dict[(entry[0], entry[1])] = entry[2]
        
        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            size = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
            print(f"size bef loop: {size}")
            attrib_list = self.global_all_attribs[i]
            total_attribs = total_attribs + len(attrib_list)
            for attrib in attrib_list:
                if attrib not in self.global_key_attributes: 
                    self.extract_filter_on_attrib1(attrib, attrib_max_length, d_plus_value, filter_attribs,
                                                  query, tabname) # filter is allowed only on non-key attribs
                    self.extract_filter_on_attrib(attrib, attrib_max_length, d_plus_value, filter_attribs,
                                                  query, tabname)
                        
                

                # print("filter_attribs", filter_attribs)
        return filter_attribs

    def extract_filter_on_attrib1(self, attrib, attrib_max_length, d_plus_value, filter_attribs, query, tabname):

        if 'int' in self.global_attrib_types_dict[(tabname, attrib)]:
            if attrib not in self.group_by_attrib:
                self.having_int(query,attrib,tabname)    

        


    def extract_filter_on_attrib(self, attrib, attrib_max_length, d_plus_value, filter_attribs, query, tabname):
        if 'int' in self.global_attrib_types_dict[(tabname, attrib)]:
            if attrib in self.group_by_attrib:
                self.handle_date_or_int_filter('int', attrib, d_plus_value, filter_attribs, tabname, query)

        elif 'date' in self.global_attrib_types_dict[(tabname, attrib)]:
            self.handle_date_or_int_filter('date', attrib, d_plus_value, filter_attribs, tabname, query)

        elif any(x in self.global_attrib_types_dict[(tabname, attrib)] for x in ['text', 'char', 'varbit']):
            self.handle_string_filter(attrib, attrib_max_length, d_plus_value, filter_attribs, tabname, query)

        elif 'numeric' in self.global_attrib_types_dict[(tabname, attrib)]:
            self.handle_numeric_filter(attrib, d_plus_value, filter_attribs, tabname, query)

    def checkAttribValueEffect(self, query, tabname, attrib, val):
        self.connectionHelper.execute_sql(["update " + tabname + " set " + attrib + " = " + str(val) + ";"])
        print("hello there")
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            self.revert_filter_changes(tabname)
            return False
        return True
    
    def check_lower_bound(self,query,attrib,tabname,min_val):
        try:
            size1 = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
            print(f"size bef: {size1}")
            self.connectionHelper.execute_sql(["SAVEPOINT fl;","BEGIN;",insert_col(tabname,'checking','INT'),flood_fill(tabname,'checking')])
            size = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
            print(f"size: {size}")
            flag=0
            for i in range(1,size+1):
                print(f"iter: {i}")
                self.connectionHelper.execute_sql([update_n_rows(tabname,attrib,i,min_val)])
                res = self.app.doJob(query)
                if isQ_result_empty(res):
                    flag = 1
                    print(f"Lower bound exists on {attrib}")
                    break
            if flag ==0:
                print(f"No lower bound on {attrib}")
            self.connectionHelper.execute_sql(["ROLLBACK TO SAVEPOINT fl;"])
        except Exception as error:
            print("Error occured while getting lower bound" + error)
            self.connectionHelper.execute_sql(["ROLLBACK TO SAVEPOINT fl;"])
            

    def having_int(self,query,attrib,tabname):
        size = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
        print(f"size: {size}")
        self.connectionHelper.execute_sql(["Begin;",create_table_as_select_star_from('new_table',tabname),
                                           truncate_table(tabname),sort_insert(tabname,attrib),
                                           drop_table('new_table')])
       
        min_val_domain, max_val_domain = get_min_and_max_val('int')
        self.check_lower_bound(query,attrib,tabname,min_val_domain)

    def handle_numeric_filter(self, attrib, d_plus_value, filterAttribs, tabname, query):
        min_val_domain, max_val_domain = get_min_and_max_val('numeric')
        # NUMERIC HANDLING
        # PRECISION TO BE GET FROM SCHEMA GRAPH
        min_present = self.checkAttribValueEffect(query, tabname, attrib,
                                                  min_val_domain)  # True implies row was still present
        max_present = self.checkAttribValueEffect(query, tabname, attrib,
                                                  max_val_domain)  # True implies row was still present
        # inference based on flag_min and flag_max
        if not min_present and not max_present:
            equalto_flag = self.get_filter_value(query, 'int', tabname, attrib, float(d_plus_value[attrib]) - .01,
                                                 float(d_plus_value[attrib]) + .01, '=')
            if equalto_flag:
                filterAttribs.append(
                    (tabname, attrib, '=', float(d_plus_value[attrib]), float(d_plus_value[attrib])))
            else:
                val1 = self.get_filter_value(query, 'float', tabname, attrib, math.ceil(float(d_plus_value[attrib])),
                                             max_val_domain, '<=')
                val2 = self.get_filter_value(query, 'float', tabname, attrib, min_val_domain,
                                             math.floor(float(d_plus_value[attrib])), '>=')
                filterAttribs.append((tabname, attrib, 'range', float(val2), float(val1)))
        elif min_present and not max_present:
            val = self.get_filter_value(query, 'float', tabname, attrib, math.ceil(float(d_plus_value[attrib])) - 5,
                                        max_val_domain, '<=')
            val = float(val)
            val1 = self.get_filter_value(query, 'float', tabname, attrib, val, val + 0.99, '<=')
            filterAttribs.append((tabname, attrib, '<=', float(min_val_domain), float(round(val1, 2))))
        elif not min_present and max_present:
            val = self.get_filter_value(query, 'float', tabname, attrib, min_val_domain,
                                        math.floor(float(d_plus_value[attrib]) + 5), '>=')
            val = float(val)
            val1 = self.get_filter_value(query, 'float', tabname, attrib, val - 1, val, '>=')
            filterAttribs.append((tabname, attrib, '>=', float(round(val1, 2)), float(max_val_domain)))

    def get_filter_value(self, query, datatype,
                         tabname, filter_attrib,
                         min_val, max_val, operator):
        query_front = "update " + str(tabname) + " set " + str(filter_attrib) + " = "
        query_back = ";"
        delta, while_cut_off = get_constants_for(datatype)

        self.revert_filter_changes(tabname)

        low = min_val
        high = max_val

        if operator == '<=':
            while is_left_less_than_right_by_cutoff(datatype, low, high, while_cut_off):
                mid_val, new_result = self.run_app_with_mid_val(datatype, high, low, query, query_front, query_back)
                if mid_val == low or mid_val == high:
                    self.revert_filter_changes(tabname)
                    break
                if isQ_result_empty(new_result):
                    new_val = get_val_plus_delta(datatype, mid_val, -1 * delta)
                    high = new_val
                else:
                    low = mid_val
                self.revert_filter_changes(tabname)
            return low

        if operator == '>=':
            while is_left_less_than_right_by_cutoff(datatype, low, high, while_cut_off):
                mid_val, new_result = self.run_app_with_mid_val(datatype, high, low, query, query_front, query_back)
                if mid_val == low or mid_val == high:
                    self.revert_filter_changes(tabname)
                    break
                if isQ_result_empty(new_result):
                    new_val = get_val_plus_delta(datatype, mid_val, delta)
                    low = new_val
                else:
                    high = mid_val
                self.revert_filter_changes(tabname)
            return high

        else:  # =, i.e. datatype == 'int', date
            is_low = True
            is_high = True
            # updatequery
            is_low = self.run_app_for_a_val(datatype, filter_attrib, is_low,
                                            low, query, query_back, query_front,
                                            tabname)
            is_high = self.run_app_for_a_val(datatype, filter_attrib, is_high,
                                             high, query, query_back, query_front,
                                             tabname)
            self.revert_filter_changes(tabname)
            return not is_low and not is_high

    def run_app_for_a_val(self, datatype, filter_attrib, is_low, low, query, query_back, query_front, tabname):
        low_query = query_front + " " + get_format(datatype, low) + " " + query_back
        self.connectionHelper.execute_sql([low_query])
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            is_low = False
        # put filter_
        return is_low

    def run_app_with_mid_val(self, datatype, high, low, query, q_front, q_back):
        mid_val = get_mid_val(datatype, high, low)
        # print("[low,high,mid]", low, high, mid_val)
        # updatequery
        update_query = q_front + " " + get_format(datatype, mid_val) + q_back
        self.connectionHelper.execute_sql([update_query])
        new_result = self.app.doJob(query)
        # print(new_result, mid_val)
        return mid_val, new_result

        # mukul

    def handle_date_or_int_filter(self, datatype, attrib, d_plus_value, filterAttribs, tabname, query):
        # min and max domain values (initialize based on data type)
        # PLEASE CONFIRM THAT DATE FORMAT IN DATABASE IS YYYY-MM-DD
        min_val_domain, max_val_domain = get_min_and_max_val(datatype)
        min_present = self.checkAttribValueEffect(query, tabname, attrib,
                                                  get_format(datatype, min_val_domain))  # True implies row
        # was still present
        max_present = self.checkAttribValueEffect(query, tabname, attrib,
                                                  get_format(datatype, max_val_domain))  # True implies row
        # was still present
        if not min_present and not max_present:
            equalto_flag = self.get_filter_value(query, datatype, tabname, attrib,
                                                 get_val_plus_delta(datatype,
                                                                    get_cast_value(datatype, d_plus_value[attrib]), -1),
                                                 get_val_plus_delta(datatype,
                                                                    get_cast_value(datatype, d_plus_value[attrib]), 1),
                                                 '=')
            if equalto_flag:
                filterAttribs.append((tabname, attrib, '=', d_plus_value[attrib], d_plus_value[attrib]))
            else:
                val1 = self.get_filter_value(query, datatype, tabname, attrib,
                                             get_cast_value(datatype, d_plus_value[attrib]),
                                             get_val_plus_delta(datatype,
                                                                get_cast_value(datatype, max_val_domain), -1), '<=')
                val2 = self.get_filter_value(query, datatype, tabname, attrib,
                                             get_val_plus_delta(datatype, get_cast_value(datatype, min_val_domain), 1),
                                             get_cast_value(datatype, d_plus_value[attrib]), '>=')
                filterAttribs.append((tabname, attrib, 'range', val2, val1))
        elif min_present and not max_present:
            val = self.get_filter_value(query, datatype, tabname, attrib,
                                        get_cast_value(datatype, d_plus_value[attrib]),
                                        get_val_plus_delta(datatype,
                                                           get_cast_value(datatype, max_val_domain), -1), '<=')
            filterAttribs.append((tabname, attrib, '<=', min_val_domain, val))
        elif not min_present and max_present:
            val = self.get_filter_value(query, datatype, tabname, attrib,
                                        get_val_plus_delta(datatype, get_cast_value(datatype, min_val_domain), 1),
                                        get_cast_value(datatype, d_plus_value[attrib]), '>=')
            filterAttribs.append((tabname, attrib, '>=', val, max_val_domain))

    def handle_string_filter(self, attrib, attrib_max_length, d_plus_value, filterAttribs, tabname, query):
        # STRING HANDLING
        # ESCAPE CHARACTERS IN STRING REMAINING
        if self.checkStringPredicate(query, tabname, attrib):
            # returns true if there is predicate on this string attribute
            representative = str(d_plus_value[attrib])
            max_length = 100000
            if (tabname, attrib) in attrib_max_length.keys():
                max_length = attrib_max_length[(tabname, attrib)]
            val = self.getStrFilterValue(query, tabname, attrib, representative, max_length)
            val = val.strip()
            if '%' in val or '_' in val:
                filterAttribs.append((tabname, attrib, 'LIKE', val, val))
            else:
                filterAttribs.append((tabname, attrib, 'equal', val, val))
        # update table so that result is not empty
        self.revert_filter_changes(tabname)

    def revert_filter_changes(self, tabname):
        self.connectionHelper.execute_sql(["Truncate table " + tabname + ';',
                                           "Insert into " + tabname + " Select * from " + tabname + "4;"])

    def checkStringPredicate(self, query, tabname, attrib):
        # updatequery
        if self.global_d_plus_value[attrib] is not None and self.global_d_plus_value[attrib][0] == 'a':
            val = 'b'
        else:
            val = 'a'
        new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, val)
        if isQ_result_empty(new_result):
            self.revert_filter_changes(tabname)
            return True
        new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, "" "")
        if isQ_result_empty(new_result):
            self.revert_filter_changes(tabname)
            return True
        return False

    def getStrFilterValue(self, query, tabname, attrib, representative, max_length):
        index = 0
        output = ""
        # currently inverted exclaimaination is being used assuming it will not be in the string
        # GET minimal string with _
        while index < len(representative):
            temp = list(representative)
            if temp[index] == 'a':
                temp[index] = 'b'
            else:
                temp[index] = 'a'
            temp = ''.join(temp)
            new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
            if not isQ_result_empty(new_result):
                temp = copy.deepcopy(representative)
                temp = temp[:index] + temp[index + 1:]
                new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
                if not isQ_result_empty(new_result):
                    representative = representative[:index] + representative[index + 1:]
                else:
                    output = output + "_"
                    representative = list(representative)
                    representative[index] = u"\u00A1"
                    representative = ''.join(representative)
                    index = index + 1
            else:
                output = output + representative[index]
                index = index + 1
        if output == '':
            return output
        # GET % positions
        index = 0
        representative = copy.deepcopy(output)
        if len(representative) < max_length:
            output = ""
            while index < len(representative):
                temp = list(representative)
                if temp[index] == 'a':
                    temp.insert(index, 'b')
                else:
                    temp.insert(index, 'a')
                temp = ''.join(temp)
                new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
                if not isQ_result_empty(new_result):
                    output = output + '%'
                output = output + representative[index]
                index = index + 1
            temp = list(representative)
            if temp[index - 1] == 'a':
                temp.append('b')
            else:
                temp.append('a')
            temp = ''.join(temp)
            new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
            if not isQ_result_empty(new_result):
                output = output + '%'
        return output

    def run_updateQ_with_temp_str(self, attrib, query, tabname, temp):
        # updatequery
        up_query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
        self.connectionHelper.execute_sql([up_query])
        new_result = self.app.doJob(query)
        return new_result


def get_constants_for(datatype):
    if datatype == 'int' or datatype == 'date':
        while_cut_off = 0
        delta = 1
    elif datatype == 'float' or datatype == 'numeric':
        while_cut_off = 0.00001
        delta = 0.01
    return delta, while_cut_off
