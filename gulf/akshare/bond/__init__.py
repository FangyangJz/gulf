from gulf.akshare.bond.all_basic import get_bond_basic_df
from gulf.akshare.bond.history import get_bond_index_daily, get_bond_daily_df, update_bond_daily_res_dict_thread

if __name__ == '__main__':
    get_bond_basic_df()
    get_bond_index_daily()
    get_bond_daily_df()
    update_bond_daily_res_dict_thread()
