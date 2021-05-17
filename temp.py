fpath = '//192.168.2.100/trddata/investment_manager_products/hx_slx_jkb/fund.log'
with open(fpath) as f:
    list_lines = f.readlines()
    print(list_lines)