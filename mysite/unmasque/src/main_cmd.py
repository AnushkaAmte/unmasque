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

    hq = "Select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority "\
         "From customer, orders, lineitem "\
         "Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and "\
         "o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' "\
         "Group By l_orderkey, o_orderdate, o_shippriority "\
         "Order by revenue desc, o_orderdate "\
         "Limit 10;"\
        #  "HAVING min (l_extendedprice) > 1000 "\
         
    eq, time = MutatedPipeline.extract(hq)

    print("=========== Extracted Query =============")
    print(eq)
    time.print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
