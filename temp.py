import pickle

str_today = '20210428'
fpath_ = r'\\192.168.2.177\PosSave_Check\20210428\18_ht\1500.pkl'

with open(fpath_, 'rb') as f:
    ret = pickle.load(f)
    print(ret['P18_ht_Hedge'])


