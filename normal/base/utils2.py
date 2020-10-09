from base.expert import expert as expert_data
from base.task import task_list as task_data
import logging
from schedule_2 import show_task_type_graph

# 重构专家数据结构
class Expert:
    def __init__(self,
                 expert_id,
                 skill_dict):
        self.id = expert_id
        self.skill = skill_dict  # Python dict {task_id: processing_time}
        self.processing_task_num = 0
        self.work_load = 0
        self.processing_task = []
        self.current_time = None        # 更新专家状态时再实时刷新

    def __str__(self):
        return "<id:{},skills:{}processing_task_num:{},word_load:{}>".format(self.id,
                                                                            self.skill,
                                                                             self.processing_task_num,
                                                                             self.work_load)

    def __repr__(self):
        return self.__str__()

    def assign(self,task):
        # check 专家负荷是否超载
        if self.processing_task_num >= 3 :
            raise FutureWarning

        # 专家负荷任务数+1,更新专家工作列表
        self.processing_task_num += 1
        self.processing_task.append(task)

        # 更新当前所分配任务Task的信息
        task.assigned_expert = self
        task.start_time = self.current_time
        task.assigned_times += 1
        if task.task_type in self.skill:        # 该专家可解决该类型的任务
            task.end_time = task.start_time + self.skill[task.task_type]
            task.wait_flag = False
        else:
            task.end_time = 99999999
            task.wait_flag = True
        with open("submit.csv", "a+") as f:
            f.write("{},{},{}\n".format(task.id, self.id, self.current_time))
            f.flush()

    # 更新
    def update(self,now_time):
        # 更新专家工作负荷量、工作列表、负荷任务数目
        self.work_load += self.processing_task_num * 1
        for task in self.processing_task:
            if task.end_time == now_time and task.wait_flag == False:
                self.processing_task.remove(task)
                self.processing_task_num -= 1
        self.current_time = now_time




class Task:
    def __init__(self,
                 task_id,
                 task_type,
                 begin_time,
                 time_limit):
        self.id = task_id
        self.task_type = task_type
        self.begin_time = begin_time
        self.time_limit = time_limit
        self.assigned_expert = None
        self.assigned_expert: Expert
        self.start_time = None
        self.end_time = None
        self.current_time = 0
        self.assigned_times = 0
        self.wait_flag = None

    def __str__(self):
        return "<id:{},type:{},begin:{},limit:{}>".format(self.id,
                                                          self.task_type,
                                                          self.begin_time,
                                                          self.time_limit)

    def __repr__(self):
        return self.__str__()

    # 实现自我检查任务是否被处理掉，更新基本属性参数
    def update(self,now_time):
        self.current_time = now_time
        if self.end_time == now_time:
            return True


# 通过专家的ID找到对应的专家类，以查询专家的状态
def load_expert_list(processing_matrix_file):
    data = expert_data(processing_matrix_file)
    raw_matrix = data.expert_list

    expert_list = []
    for i in range(1, data.expert_num + 1):
        skill_dict = {}
        for j in range(1, data.question_num + 1):
            if raw_matrix[i][j] < 999999:
                skill_dict[j] = raw_matrix[i][j]
        expert_list.append(Expert(expert_id=i,
                                  skill_dict=skill_dict))

    return expert_list


def load_task_list(work_order_file):
    data = task_data(work_order_file)
    raw_task_list = data.task_list

    task_list = []
    for task in raw_task_list:
        task_list.append(Task(task_id=task[0],
                              task_type=task[2],
                              begin_time=task[1],
                              time_limit=task[3]))
    return task_list


# 对于给定的任务，哪些专家可以完成这项任务
def load_task_expert_dict(processing_matrix_file):
    data = expert_data(processing_matrix_file).sorted_expert_task_dict

# 专家优先级分数 = 有该技能并空闲*(-10000) + 负荷时间*a + 负荷任务数目*b + 处理时间*c （注：暂定a=1,b=10,c=1，分数越低优先级高)
def score_expert(expert:Expert,task_type):
    if task_type in expert.skill and expert.processing_task_num < 3:
        return  expert.work_load + expert.processing_task_num * 10 + expert.skill[task_type] -10000
    else:
        return expert.work_load + expert.processing_task_num * 10 +10000

# 任务优先级分数 = 剩余响应时间*a + 流转次数*b （注：暂定a=1，b=-5，分数低优先级高）
def score_task(task:Task):
    return (task.time_limit-(task.current_time - task.begin_time)) ** 2 + (task.time_limit-(task.current_time - task.begin_time))+ task.task_type * 5

# 实现专家实列列表expert_list 或 任务等待列表wait_list 按优先级的排序，返回[expert/task1,expert/task2,...]
def expert_priority_sort(expert_list, task_type):
    # priority={e/t_id:e/t_sore,...}, priority_list=[expert1,expert2,...]
    priority,priority_list = {},[]
    for i in range(len(expert_list)):       # 注意expert_list中的元素是Expert类实例，专家ID从1开始编号
        priority[expert_list[i].id] = score_expert(expert_list[i], task_type)        # 给每位专家评估分数
    p_list = sorted(priority.items(), key=lambda p: (p[1], p[0]))  # 按值(优先级分数）升序排列，值相同是按健（ID）排
    # logger.debug(">>> len:{}, p_list：{}".format(len(p_list),p_list))
    for i in range(len(p_list)):
        priority_list.append(expert_list[p_list[i][0]- 1] )      #p_list[i][0]为优先级最高专家的ID，ID-1即为所需索引号
    return priority_list

# 只返回score值较小的一部分
def task_priority_sort(wait_list, score):
    priority, priority_list = {}, []
    for i in range(len(wait_list)):
        priority[wait_list[i]] = score_task(wait_list[i])
    p_list = sorted(priority.items(), key=lambda p:(p[1],p[0].id))
    for p in p_list:
        priority_list.append(p[0])    # 优先级序列中加入该任务
    return priority_list

# 如果专家技能排在所有专家范围内的前10 或者处理时间小于50则保留，另外保证每个专家最终保留至少有一项技能
def skill_change(expert_list,flag):
    for i in range(1,108):
        # task_type = i 时，最所有专家进行评分和排序
        priority_list = expert_priority_sort(expert_list, i)
        # 废掉有1技能排名前三的专家其他的技能
        if i == 1 and flag == 0:
            for j in range(0, 7):
                skills = {}
                skills[i] = priority_list[j].skill[i]
                priority_list[j].skill = skills
            continue
        if i == 4 and flag == 0:
            for j in range(0, 1):
                skills = {}
                skills[i] = priority_list[j].skill[i]
                priority_list[j].skill = skills
            continue
        for j in range(3,len(priority_list)):
            if i in priority_list[j].skill and priority_list[j].skill[i] > 50 and len(priority_list[j].skill) > 1 :
                a =  priority_list[j].skill.pop(i)
# 交换1，2的技能skill
def swap(expert_list1,expert_list2):
    for i in range(len(expert_list1)):
        expert_list1[i].skill = expert_list2[i].copy()



if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)
    expert_list = load_expert_list("data/process_time_matrix.csv")
    task_list = load_task_list("data/work_order.csv")
    expert_list2 = []
    for i in range(len(expert_list)):
        expert_list2.append(expert_list[i].skill.copy())
    skill_change(expert_list,0)
    # 经完成的任务列表、等待处理任务列表（包含：已分配但不能处理的任务、未分配的任务）
    completed_list, wait_list = [], []
    # 声明计时器,这里节省时间从480开始计时
    time = 0
    while True:
        time += 1
        # if time == 1200:
        #     swap(expert_list,expert_list2)
        #     skill_change(expert_list,1)
        if len(completed_list) == 8840 :
            break
        # 声明当前时刻任务列表,注意同一时刻可能接收到多个任务
        current_task_list = []
        for task in task_list:
            if task.begin_time == time:     # 该时间点有任务产生，加入当前时刻任务列表，等待处理
                current_task_list.append(task)
                task.update(time)       # 更新当前时刻产生的任务实例
            elif task.current_time < time:
                Bool = task.update(time)       # 更新之前就已经产生的任务实例
                if Bool == True:        # 检测到已处理完成的任务
                    completed_list.append(task)        # 加入已经完成任务列表

        # 更新专家实例
        for expert in expert_list:
            expert.update(time)
        if time%50 == 0:
            print("time:{} completed_num:{} wait_num".format(time,len(completed_list)),len(wait_list))
            logger.debug(">>>expert_list:%s",expert_list)
            logger.debug(">>> wait_list:%s", wait_list)
            logger.debug(">>> current_task_list:%s", current_task_list)
            show_task_type_graph(wait_list)
        # TODO:考虑等待列表的分配，何时调出再分配？
        # 流转次数小于4的，可以继续分配给摸鱼的专家积累负荷量，等于4的则必须被处理掉或继续等待
        # 等待列表中任务的优先级应考虑剩余响应时间、流转次数等因素,另外注意回收的任务再分配要释放专家
        if wait_list != []:
            wait_task_priority_list= task_priority_sort(wait_list, 30)
            #TODO：仔细考虑等待序列再分配问题，这里暂且采用每次强行分配一半的策略
            for i in range(min(len(wait_task_priority_list),500) ):
                task = wait_task_priority_list[i]
                # 产生该任务对应专家的优先级序列expert_priority_list
                expert_priority_list = expert_priority_sort(expert_list, task.task_type)
                for expert in expert_priority_list:
                    # 流转次数小于4或者专家技能与任务匹配的都直接分配，之后从wait_list中移除该任务
                    if expert.processing_task_num < 3 and task.task_type in expert.skill:   # 技能匹配才分配
                        # 如果是流转的任务，应释放之前的专家,专家任务数目-1，任务列表移除该任务
                        if task.assigned_expert != None:
                            task.assigned_expert.processing_task_num -= 1
                            print("task:{} expert:{}  task_num:{}:".format(task,expert.id,task.assigned_expert.processing_task_num))
                            task.assigned_expert.processing_task.remove(task)
                            task.assigned_expert = None
                        expert.assign(task)
                        wait_list.remove(task)
                        break



        # 开始分配当前时刻新产生的任务实例
        if current_task_list == []:
            continue        # 当前时刻未产生新任务，直接进入下一轮循环
        else:
            for task in current_task_list:
                # TODO:产生该任务对应专家的优先级序列expert_priority_list
                expert_priority_list = expert_priority_sort(expert_list,task.task_type)
                # logger.debug(">>> {}号任务对应专家优先级序列：{}".format(task.task_type,expert_priority_list))
                for expert in expert_priority_list:
                    if expert.processing_task_num < 3 and task.task_type in expert.skill:       # 技能匹配才分配
                        expert.assign(task)
                        # logger.debug(">>> task.assigned_expert:%s",task.assigned_expert)
                        break

                # 检查该专家是否在摸鱼，任务处于实际并不能真正被处理,或者是否所有的专家都忙碌，任务未被分配
                if task.wait_flag == True or task.assigned_expert == None:
                    wait_list.append(task)      # 将该任务加入等待队列，等待以后重新分配


    sum_M, sum_R, sum_L = 0, 0, 0
    for task in task_list:
        sum_M += max(task.start_time - task.begin_time - task.time_limit, 0) / task.time_limit
        sum_R += (task.end_time - task.start_time) / (task.end_time - task.begin_time)
    M, R = sum_M / len(task_list), sum_R / len(task_list)
    for expert in expert_list:
        sum_L += expert.work_load / (60 * 8 * 3)
    L = sum_L / len(expert_list)
    L_2 = 0
    for expert in expert_list:
        L_2 += (expert.work_load / (60 * 8 * 3) - L) ** 2
    sigma_L = (L_2 / len(expert_list)) ** 0.5
    score = 3000 * R / (3 * M + 2 * sigma_L)
    print("R:{}  M:{}  sigma:{}  score:{}".format(R, M, sigma_L, score))





