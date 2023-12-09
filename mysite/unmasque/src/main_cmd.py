from .core import MutatedPipeline

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # hq = "select l_orderkey as orderkey, max(c_acctbal) as revenue, " \
    #      "o_orderdate " \
    #      "as orderdate, " \
    #      "o_shippriority as " \
    #      "shippriority from customer, orders, " \
    #      "lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
    #      "o_orderdate " \
    #      "< '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue " \
    #      "desc, o_orderdate, l_orderkey limit 10;"

    # hq = "Select o_orderdate "\
    #      "From customer, orders, lineitem "\
    #      "Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and "\
    #      "o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' "\
    #      "Group By o_orderdate having min(o_shippriority) =0 "\
    #      "Order by  o_orderdate "\
    #      "Limit 10;"
    
    # hq = "select name,avg(marks) from performance group by name having sum(marks) < 241;"

    # hq ="Select  o_orderdate "\
    #    "From customer, orders "\
    #    "where c_custkey = o_custkey and "\
    #    "o_orderdate < date '1995-03-15'  "\
    #    "Group By o_orderdate having min(o_shippriority)=0 "\
    #    "Order by o_orderdate "\
    #    "Limit 10;"
    
    hq = "select ps_partkey from partsupp group by ps_partkey having max(ps_availqty) > 7000;"

    # hq ="select s_name,sum(credits) from course_info,student_info where sr_no = student_sr_no group by s_name having sum(credits)>17 and avg(attendance) > 80;"

    # filter of having and where clause are disjoint
    # each attribute has atmost one aggregation
    # joins of type pk-fk and fk-fk
    # where clause has conjunction of filter and join predicate
    # filter predicate feature non-key columns

    eq, time = MutatedPipeline.extract(hq)

    print("=========== Extracted Query =============")
    print(eq)
    time.print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
