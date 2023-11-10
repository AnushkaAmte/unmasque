from .QueryStringGenerator import QueryStringGenerator
from .elapsed_time import create_zero_time_profile
from ...src.util.ConnectionHelper import ConnectionHelper
from ...refactored.aggregation import Aggregation
from ...refactored.cs2 import Cs2
from ...refactored.equi_join import EquiJoin
from ...refactored.filter import Filter
from ...refactored.from_clause import FromClause
from ...refactored.groupby_clause import GroupBy
from ...refactored.limit import Limit
from ...refactored.orderby_clause import OrderBy
from ...refactored.projection import Projection
from ...refactored.view_minimizer import ViewMinimizer
from ...refactored.new_minimizer import NewMinimizer

def extract(query):

    connectionHelper = ConnectionHelper()
    connectionHelper.connectUsingParams()
    
    time_profile = create_zero_time_profile()

    '''
    From Clause Extraction
    '''
    fc = FromClause(connectionHelper)
    check = fc.doJob(query, "rename")
    time_profile.update_for_from_clause(fc.local_elapsed_time)
    if not check or not fc.done:
        print("Some problem while extracting from clause. Aborting!")
        return None, time_profile

    '''
    Correlated Sampling
    '''
    cs2 = Cs2(connectionHelper, fc.all_relations, fc.core_relations, fc.get_key_lists())
    
    check = cs2.doJob(query)
    time_profile.update_for_cs2(cs2.local_elapsed_time)
    if not check or not cs2.done:
        print("Sampling failed!")

    '''
    Database Minimization: View Minimization
    '''
    # vm = ViewMinimizer(connectionHelper, fc.core_relations, cs2.sizes, cs2.passed)
    # check = vm.doJob(query)
    # time_profile.update_for_view_minimization(vm.local_elapsed_time)
    # if not check:
    #    print("Cannot do database minimization. ")
    #    return None, time_profile
    # if not vm.done:
    #    print("Some problem while view minimization. Aborting extraction!")
    #    return None, time_profile 

    nm = NewMinimizer(connectionHelper, fc.core_relations,cs2.sizes)
    #print(nm)
    check = nm.doJob(query)
    #print(check)
    time_profile.update_for_new_minimization(nm.local_elapsed_time)
    if not check:
        print("Cannot do database minimization. ")
        return None,time_profile
    if not nm.done:
        print("Some problem while view minimization. Aborting extraction!")
        return None, time_profile


    '''
    Join Graph Extraction
    '''
    ej = EquiJoin(connectionHelper,
                  fc.get_key_lists(),
                  fc.core_relations,
                  nm.global_min_instance_dict)
    check = ej.doJob(query)
    time_profile.update_for_where_clause(ej.local_elapsed_time)
    if not check:
        print("Cannot find Join Predicates.")
    if not ej.done:
        print("Some error while Join Predicate extraction. Aborting extraction!")
        return None, time_profile

    '''
    Filters Extraction
    '''
    fl = Filter(connectionHelper,
                fc.get_key_lists(),
                fc.core_relations,
                nm.global_min_instance_dict,
                ej.global_key_attributes)
    check = fl.doJob(query)
    time_profile.update_for_where_clause(fl.local_elapsed_time)
    if not check:
        print("Cannot find Filter Predicates.")
    if not fl.done:
        print("Some error while Filter Predicate extraction. Aborting extraction!")
        return None, time_profile

    '''
    Projection Extraction
    '''
    pj = Projection(connectionHelper,
                    ej.global_attrib_types,
                    fc.core_relations,
                    fl.filter_predicates,
                    ej.global_join_graph,
                    ej.global_all_attribs)
    check = pj.doJob(query)
    time_profile.update_for_projection(pj.local_elapsed_time)
    if not check:
        print("Cannot find projected attributes. ")
        return None, time_profile
    if not pj.done:
        print("Some error while projection extraction. Aborting extraction!")
        return None, time_profile

    '''
    Group By Clause Extraction
    '''
    gb = GroupBy(connectionHelper,
                 ej.global_attrib_types,
                 fc.core_relations,
                 fl.filter_predicates,
                 ej.global_all_attribs,
                 ej.global_join_graph,
                 pj.projected_attribs)
    check = gb.doJob(query)
    time_profile.update_for_group_by(gb.local_elapsed_time)
    if not check:
        print("Cannot find group by attributes. ")

    if not gb.done:
        print("Some error while group by extraction. Aborting extraction!")
        return None, time_profile

    '''
    Aggregation Extraction
    '''
    agg = Aggregation(connectionHelper,
                      ej.global_key_attributes,
                      ej.global_attrib_types,
                      fc.core_relations,
                      fl.filter_predicates,
                      ej.global_all_attribs,
                      ej.global_join_graph,
                      pj.projected_attribs,
                      gb.has_groupby,
                      gb.group_by_attrib,
                      pj.dependencies,
                      pj.solution,
                      pj.param_list)
    check = agg.doJob(query)
    time_profile.update_for_aggregate(agg.local_elapsed_time)
    if not check:
        print("Cannot find aggregations.")
    if not agg.done:
        print("Some error while extrating aggregations. Aborting extraction!")
        return None, time_profile

    '''
    Order By Clause Extraction
    '''
    ob = OrderBy(connectionHelper,
                 ej.global_key_attributes,
                 ej.global_attrib_types,
                 fc.core_relations,
                 fl.filter_predicates,
                 ej.global_all_attribs,
                 ej.global_join_graph,
                 pj.projected_attribs,
                 pj.projection_names,
                 agg.global_aggregated_attributes)
    ob.doJob(query)
    time_profile.update_for_order_by(ob.local_elapsed_time)
    if not ob.has_orderBy:
        print("Cannot find aggregations.")
    if not ob.done:
        print("Some error while extrating aggregations. Aborting extraction!")
        return None, time_profile

    '''
    Limit Clause Extraction
    '''
    lm = Limit(connectionHelper,
               ej.global_attrib_types,
               ej.global_key_attributes,
               fc.core_relations,
               fl.filter_predicates,
               ej.global_all_attribs,
               gb.group_by_attrib)
    lm.doJob(query)
    time_profile.update_for_limit(lm.local_elapsed_time)
    if lm.limit is None:
        print("Cannot find limit.")
    if not lm.done:
        print("Some error while extrating aggregations. Aborting extraction!")
        return None, time_profile

    # last component in the pipeline should do this
    time_profile.update_for_app(lm.app.method_call_count)

    q_generator = QueryStringGenerator(connectionHelper)
    eq = q_generator.generate_query_string(fc, ej, fl, pj, gb, agg, ob, lm)
    # print("extracted query :\n", eq)

    connectionHelper.closeConnection()

    return eq, time_profile
