import sys

from pymongo import MongoClient,DESCENDING,ASCENDING

def get_rank(user_id):
    client =MongoClient()
    db = client.shiyanlou
    contests = db.contests

    #计算用户 user_id 的排名，总分数以及花费的总时间
    pl_match = {'$match':{'user_id':user_id}}
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
#    print(type(user_time))
#    print(type(user_score))
#    print("id:%s,score:%s,time:%s"%(user_id,
#        user_score,user_time))
    if len(l) == 0:
        return 0,0,0

    p_group1 = {'$group':{'_id':'$user_id','total_score':{'$sum':'$score'},
            'total_time':{'$sum':'$submit_time'}}}

    p_match = {'$match':{'$or':[
        {'total_score':{'$gt':user_score}},
        {'total_time':{'$lt':user_time},'total_score':user_score}
            ]}}

    p_group2={'$group':{'_id':None,'count':{'$sum':1}}}

    pipline=[p_group1,p_match,p_group2]
    result =list(contests.aggregate(pipline))
    if len(result)>0:
        rank = result[0]['count'] + 1
    else:
        rank = 1

    return rank,user_score,user_time

    
    

    
        

    #依次返回排名，分数和时间，不能修改顺序
    #return rank,score,submit_time

if __name__ == '__main__':

    """
    1.判断参数格式是否符合要求
    2.获取user_id 参数
    """
    if len(sys.argv) != 2:
        print("Parameter error.")
        sys.exit(1)
    user_id=sys.argv[1]

    userdata = get_rank(int(user_id))
    print(userdata)
    #pass
    #根据用户ID获取用户排名，分数和时间
    #userdata = get_rank(user_id)
    #print(userdata)
