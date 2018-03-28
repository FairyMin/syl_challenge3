import sys

from pymongo import MongoClient,DESCENDING,ASCENDING

def get_rank(user_id):
    client =MongoClient()
    db = client.shiyanlou
    contests = db.contests

    #计算用户 user_id 的排名，总分数以及花费的总时间
    #排名的计算方法：
    #   1.找到user_id所对应的信息总和，即得分总数，用时总数
    #   2.根据得到的得分与用时，对应排名规则，筛选出排名在该用户前的所有用户
    #   3.对筛选结果进行计数，即可得到当前用户的排名
        
        
    #匹配user_id对应的所有数据   
    pl_match = {'$match':{'user_id':user_id}}
    #对这些数据进行求和
    pl_group ={'$group':{
            '_id':'$user_id',
            'total_score':{'$sum':'$score'},
            'total_time':{'$sum':'$submit_time'}
            }}
    user_info = contests.aggregate([pl_match,pl_group])
    l = list(user_info)
    #记录指定用户的信息
    user_score=l[0]['total_score']
    user_time=l[0]['total_time']
#    print("id:%s,score:%s,time:%s"%(user_id,
#        user_score,user_time))
    if len(l) == 0:
        return 0,0,0

    #根据id对所有数据进行分组求和
    p_group1 = {'$group':{'_id':'$user_id','total_score':{'$sum':'$score'},
            'total_time':{'$sum':'$submit_time'}}}
    
    #根据排名规则筛选符合排名在指定用户前面的所有数据
    p_match = {'$match':{'$or':[
        {'total_score':{'$gt':user_score}},
        {'total_time':{'$lt':user_time},'total_score':user_score}
            ]}}

    #对筛选结果进行计数
    p_group2={'$group':{'_id':None,'count':{'$sum':1}}}

    pipline=[p_group1,p_match,p_group2]
    result =list(contests.aggregate(pipline))

    #获得排名
    if len(result)>0:
        rank = result[0]['count'] + 1
    else:
        rank = 1
      

    #依次返回排名，分数和时间，不能修改顺序
    return rank,user_score,user_time

if __name__ == '__main__':

    """
    1.判断参数格式是否符合要求
    2.获取user_id 参数
    """
    if len(sys.argv) != 2:
        print("Parameter error.")
        sys.exit(1)
    user_id=sys.argv[1]
  #根据用户ID获取用户排名，分数和时间
    userdata = get_rank(int(user_id))
    print(userdata)

