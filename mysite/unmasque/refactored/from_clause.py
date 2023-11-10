from ..refactored.abstract.ExtractorBase import Base
from ..refactored.executable import Executable
from ..refactored.initialization import Initiator
from ..refactored.util.common_queries import alter_table_rename_to, create_table_like
from ..refactored.util.utils import isQ_result_empty

try:
    import psycopg2
except ImportError:
    pass


class FromClause(Base):
    DEBUG_QUERY = "select pid, state, query from pg_stat_activity where datname = 'tpch';"
    TERMINATE_STUCK_QUERIES = "SELECT pg_terminate_backend(pid);"

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "FromClause")
        self.app = Executable(connectionHelper)
        self.init = Initiator(connectionHelper)

        self.all_relations = set()
        self.core_relations = []
  

    def get_core_relations_by_rename(self, query):
        #we rename the table t to temp
        #then we run the blackbox
        for tabname in self.all_relations: #do this for all the tables in db
            
            try: #rename the table to temp, create a new table like temp and run the query on this mutated db
                self.connectionHelper.execute_sql(
                    ["BEGIN;", alter_table_rename_to(tabname, "temp"), create_table_like(tabname, "temp")])
#is name was a table in the query, rename name to temp, create a new empty table with the same schema as temp, rereun the query
                new_result = self.app.doJob(query)
                if isQ_result_empty(new_result): #if result has <=1 tuple,i.e if result is empty
                    self.core_relations.append(tabname) #table is in query
            except Exception as error:
                print("Error Occurred in table extraction. Error: " + str(error)) #error is thrown, table was not built properly
                self.connectionHelper.execute_sql(["ROLLBACK;"])# rollback the transaction
                exit(1)
#if table was not in query then we get the same result i,e non empty so we rollback anyways
            self.connectionHelper.execute_sql(["ROLLBACK;"]) #if we identified correctly, then undo the renaming

    def get_core_relations_by_error(self, query):
        for tabname in self.all_relations:
            try:
                self.connectionHelper.execute_sql(["BEGIN;", alter_table_rename_to(tabname, "temp")])
                #rename t to temp
                try:
                    new_result = self.app.doJob(query)  # slow, this approach is not always feasible due to how some dbs handle error queries
                    if isQ_result_empty(new_result): 
                        self.core_relations.append(tabname) #table in our query
                except psycopg2.Error as e:
                    if e.pgcode == '42P01': #undefined table
                        self.core_relations.append(tabname) #means table in our query, add to relations Te
                    elif e.pgcode != '57014': #query canceled, not in our query timeout occurs
                        raise

            except Exception as error:
                print("Error Occurred in table extraction. Error: " + str(error))

            finally:
                self.connectionHelper.execute_sql(["ROLLBACK;"]) #rollback the renaming

    def extract_params_from_args(self, args):
        return args[0], args[1]

    def doJob(self, *args):
        check = self.init.result
        if not self.init.done:
            check = self.init.doJob()
        if not check:
            return False
        self.all_relations = self.init.all_relations
        return super().doJob(*args)

    def doActualJob(self, args):
        query, method = self.extract_params_from_args(args)
        self.core_relations = []
        if method == "rename":
            self.get_core_relations_by_rename(query)
        else:
            self.get_core_relations_by_error(query)
        return self.core_relations

    def get_key_lists(self):
        return self.init.global_key_lists


