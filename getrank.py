import sys

from pymongo import MongoClient,DESCENDING,ASCENDING

def get_rank(user_id):
    client =MongoClient()
    db = client.shiyanlou
    contests = db.contests

    #计算用户 user_id 的排名，总分数以及花费的总时间
#    t=contests.find()
#    sort_t = t.sort('score',DESCENDING)
#    for i in sort_t:
#        print(i['score'])
#        print('*'*10)
#        print(i)
    #利用mongodb的aggreate方法进行分类，求和
    all_infos = contests.aggregate
    group = {'$group':{'_id':'$user_id',
        'total_score':{'$sum':'$score'},'total_time':{'$sum':'$submit_time' }}}
    #all_infos.sort([('total_score',DESCENDING),('total_time',ASCENDING)]) 
    sort = {'$sort':{'total_score':-1}}
    all_infos = contests.aggregate([group,sort])
    for a_info in all_infos:
        print('id:%s,score:%s,time:%s'%(a_info['_id'],a_info['total_score'],
                a_info['total_time']))


    #依次返回排名，分数和时间，不能修改顺序
    #return rank,score,submit_time

if __name__ == '__main__':

    """
    1.判断参数格式是否符合要求
    2.获取user_id 参数
    """
    get_rank(1)
    #pass
    #根据用户ID获取用户排名，分数和时间
    #userdata = get_rank(user_id)
    #print(userdata)
